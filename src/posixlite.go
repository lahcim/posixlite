// posixlite.go
//
// Minimal POSIX-metadata overlay filesystem using FUSE (bazil.org/fuse).
// - Lower: existing directory (e.g., gcsfuse mount)
// - Upper: FUSE mountpoint
// - Persists chmod/chown by saving metadata JSON files under Lower/.posixmeta/
//
// Build:  go build -o posixlite posixlite.go
// Run:    ./posixlite -lower ~/dev_gcs_lower -mount ~/dev -logfile ~/posixlite.log
// Unmount: fusermount -u ~/dev
//
// Notes:
// - Intentionally minimal. Good enough for chmod +x persistence.
// - Metadata persists in the lower tree as objects/files.

package main

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

type meta struct {
	Mode uint32 `json:"mode"`
	UID  uint32 `json:"uid"`
	GID  uint32 `json:"gid"`
}

type overlayFS struct {
	lower       string
	metaDir     string
	defFileMode os.FileMode
	defDirMode  os.FileMode
	mu          sync.Mutex
}

func main() {
	lower := flag.String("lower", "", "Lower directory (backing store), e.g. gcsfuse mount")
	mount := flag.String("mount", "", "Mount point for FUSE")
	defFile := flag.String("def-file-mode", "0666", "Default file mode (octal) when no metadata exists")
	defDir := flag.String("def-dir-mode", "0777", "Default dir mode (octal) when no metadata exists")
	debug := flag.Bool("debug", false, "Enable verbose FUSE debug logging")
	logfile := flag.String("logfile", "", "Write logs to this file (in addition to stderr)")
	daemon := flag.Bool("daemon", true, "Run in background (daemonize) and return immediately")

	flag.Parse()

	if *lower == "" || *mount == "" {
		log.Fatalf("Usage: %s -lower <dir> -mount <dir> [-debug] [-logfile <path>]", os.Args[0])
	}

	maybeDaemonize(*daemon, *logfile)

	// Optional file logging
	if *logfile != "" {
		f, err := os.OpenFile(*logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o666)
		if err != nil {
			log.Fatalf("failed to open logfile %s: %v", *logfile, err)
		}
		log.SetOutput(io.MultiWriter(os.Stderr, f))
	}
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	log.SetPrefix(fmt.Sprintf("[pid=%d] ", os.Getpid()))

	dfm, err := parseOctalMode(*defFile)
	if err != nil {
		log.Fatalf("bad -def-file-mode: %v", err)
	}
	ddm, err := parseOctalMode(*defDir)
	if err != nil {
		log.Fatalf("bad -def-dir-mode: %v", err)
	}

	lowerAbs, _ := filepath.Abs(*lower)
	mountAbs, _ := filepath.Abs(*mount)

	if err := os.MkdirAll(mountAbs, 0o777); err != nil {
		log.Fatalf("failed to create mount dir %s: %v", mountAbs, err)
	}

	metaDir := filepath.Join(lowerAbs, ".posixmeta")
	if err := os.MkdirAll(metaDir, 0o777); err != nil {
		log.Fatalf("failed to create metadata dir %s: %v", metaDir, err)
	}

	// Best-effort stale mount cleanup (helps after kill -9)
	_ = fuse.Unmount(mountAbs)
	_ = exec.Command("fusermount", "-uz", mountAbs).Run()
	_ = exec.Command("fusermount3", "-uz", mountAbs).Run()

	// Signals
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	// Enable bazil FUSE debug output (package-level hook in this version)
	if *debug {
		fuse.Debug = func(msg interface{}) {
			log.Printf("FUSE %v", msg)
		}
	}

	log.Printf("Starting posixlite lower=%s mount=%s meta=%s debug=%v", lowerAbs, mountAbs, metaDir, *debug)

	c, err := fuse.Mount(
		mountAbs,
		fuse.FSName("posixlite"),
		fuse.Subtype("posixlite"),
	)
	if err != nil {
		log.Fatalf("mount error: %v", err)
	}

	fsys := &overlayFS{
		lower:       lowerAbs,
		metaDir:     metaDir,
		defFileMode: dfm,
		defDirMode:  ddm,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- fs.Serve(c, fsys)
	}()

	select {
	case <-ctx.Done():
		log.Printf("Signal received, unmounting...")
		_ = fuse.Unmount(mountAbs)
		_ = c.Close()
		time.Sleep(200 * time.Millisecond)
		_ = exec.Command("fusermount", "-uz", mountAbs).Run()
		_ = exec.Command("fusermount3", "-uz", mountAbs).Run()
		log.Printf("Exiting.")
	case err := <-serveErr:
		log.Printf("fs.Serve returned: %v", err)
		_ = c.Close()
	}
}

func maybeDaemonize(daemon bool, logfile string) {
	// If not requested, or we're already the daemon child, do nothing.
	if !daemon || os.Getenv("POSIXLITE_DAEMON") == "1" {
		return
	}

	// Re-exec self as a new session leader (detached from the terminal).
	cmd := exec.Command(os.Args[0], os.Args[1:]...)
	cmd.Env = append(os.Environ(), "POSIXLITE_DAEMON=1")

	// Detach from controlling terminal.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}

	// Redirect stdio:
	// - If logfile is provided, keep stderr/stdout there.
	// - Otherwise discard to /dev/null.
	var out *os.File
	var err error
	if logfile != "" {
		out, err = os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o666)
		if err != nil {
			// If we can't open logfile, fall back to /dev/null.
			out, _ = os.OpenFile("/dev/null", os.O_WRONLY, 0)
		}
	} else {
		out, _ = os.OpenFile("/dev/null", os.O_WRONLY, 0)
	}
	in, _ := os.OpenFile("/dev/null", os.O_RDONLY, 0)
	cmd.Stdin = in
	cmd.Stdout = out
	cmd.Stderr = out

	// Start child and exit parent immediately.
	if err := cmd.Start(); err != nil {
		log.Fatalf("failed to daemonize: %v", err)
	}
	fmt.Printf("posixlite started in background (pid %d)\n", cmd.Process.Pid)
	os.Exit(0)
}

func (d *dirNode) Mknod(ctx context.Context, req *fuse.MknodRequest) (fs.Node, error) {
	childRel := joinRel(d.rel, req.Name)
	lp := filepath.Join(d.o.lower, childRel)

	// Only support regular files in this minimal overlay
	if (req.Mode & os.ModeType) != 0 {
		log.Printf("Mknod unsupported type rel=%q mode=%#o", childRel, req.Mode)
		return nil, fuse.ENOSYS
	}

	// Create the file (O_EXCL avoids clobber if it exists)
	f, err := os.OpenFile(lp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, req.Mode)
	if err != nil {
		// If already exists, just return the node
		if os.IsExist(err) {
			return &fileNode{o: d.o, rel: childRel}, nil
		}
		log.Printf("Mknod FAILED rel=%q lower=%q mode=%#o err=%v", childRel, lp, req.Mode, err)
		return nil, err
	}
	_ = f.Close()

	// Persist perms (best-effort)
	d.o.setMeta(childRel, meta{
		Mode: uint32(req.Mode & os.ModePerm),
		UID:  uint32(os.Getuid()),
		GID:  uint32(os.Getgid()),
	})

	log.Printf("Mknod OK rel=%q lower=%q mode=%#o", childRel, lp, req.Mode)
	return &fileNode{o: d.o, rel: childRel}, nil
}

// --- FS root ---

func (o *overlayFS) Root() (fs.Node, error) {
	return &dirNode{o: o, rel: ""}, nil
}

// --- directory node ---

type dirNode struct {
	o   *overlayFS
	rel string
}

func (d *dirNode) lowerPath() string {
	return filepath.Join(d.o.lower, d.rel)
}

func (d *dirNode) Attr(ctx context.Context, a *fuse.Attr) error {
	st, err := os.Lstat(d.lowerPath())
	if err != nil {
		if os.IsNotExist(err) {
			return fuse.ENOENT
		}
		log.Printf("Dir Attr FAILED rel=%q lower=%q err=%v", d.rel, d.lowerPath(), err)
		return err
	}

	fillAttrFromInfo(st, a)

	if md, ok := d.o.getMeta(d.rel); ok {
		a.Mode = (a.Mode &^ os.ModePerm) | os.FileMode(md.Mode)&os.ModePerm
		a.Uid = md.UID
		a.Gid = md.GID
	} else {
		a.Mode = (a.Mode &^ os.ModePerm) | d.o.defDirMode
	}
	return nil
}

func (h *fileHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.Printf("Fsync rel=%q", h.rel)

	if err := h.f.Sync(); err != nil {
		log.Printf("File Fsync ignored rel=%q err=%v", h.rel, err)
	}
	return nil
}

func (f *fileNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// Best-effort: ignore fsync failures in object-store/FUSE stacks.
	log.Printf("NodeFsync(file) rel=%q", f.rel)
	return nil
}

func (d *dirNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.Printf("NodeFsync(dir) rel=%q", d.rel)
	return nil
}

func (d *dirNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if name == ".posixmeta" {
		return nil, fuse.ENOENT
	}

	childRel := joinRel(d.rel, name)
	lp := filepath.Join(d.o.lower, childRel)

	st, err := os.Lstat(lp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}
		log.Printf("Lookup FAILED rel=%q lower=%q err=%v", childRel, lp, err)
		return nil, err
	}

	if st.IsDir() {
		return &dirNode{o: d.o, rel: childRel}, nil
	}
	return &fileNode{o: d.o, rel: childRel}, nil
}

func (d *dirNode) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := os.ReadDir(d.lowerPath())
	if err != nil {
		return nil, err
	}
	out := make([]fuse.Dirent, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if name == ".posixmeta" {
			continue
		}
		var t fuse.DirentType
		if e.IsDir() {
			t = fuse.DT_Dir
		} else {
			t = fuse.DT_File
		}
		out = append(out, fuse.Dirent{Name: name, Type: t})
	}
	return out, nil
}

func (d *dirNode) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	childRel := joinRel(d.rel, req.Name)
	lp := filepath.Join(d.o.lower, childRel)

	if err := os.Mkdir(lp, req.Mode); err != nil {
		return nil, err
	}
	d.o.setMeta(childRel, meta{
		Mode: uint32(req.Mode & os.ModePerm),
		UID:  uint32(os.Getuid()),
		GID:  uint32(os.Getgid()),
	})
	return &dirNode{o: d.o, rel: childRel}, nil
}

func (d *dirNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	childRel := joinRel(d.rel, req.Name)
	lp := filepath.Join(d.o.lower, childRel)

	// Keep create simple: gcsfuse/object-store FUSE layers can reject exotic combinations.
	flags := os.O_WRONLY | os.O_CREATE
	if req.Flags&fuse.OpenTruncate != 0 {
		flags |= os.O_TRUNC
	}
	if req.Flags&fuse.OpenAppend != 0 {
		flags |= os.O_APPEND
	}

	f, err := os.OpenFile(lp, flags, req.Mode)
	if err != nil {
		log.Printf("Create FAILED rel=%q lower=%q reqFlags=%v oflags=%#x mode=%#o err=%v",
			childRel, lp, req.Flags, flags, req.Mode, err)
		return nil, nil, err
	}

	// Persist perms (best-effort; if metadata write fails we still keep the file)
	d.o.setMeta(childRel, meta{
		Mode: uint32(req.Mode & os.ModePerm),
		UID:  uint32(os.Getuid()),
		GID:  uint32(os.Getgid()),
	})

	n := &fileNode{o: d.o, rel: childRel}
	h := &fileHandle{
		f:      f,
		rel:    childRel,
		o:      d.o,
		append: (flags&os.O_APPEND != 0),
	}
	return n, h, nil
}

func (d *dirNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	childRel := joinRel(d.rel, req.Name)
	lp := filepath.Join(d.o.lower, childRel)

	if err := os.Remove(lp); err != nil {
		return err
	}
	d.o.delMeta(childRel)
	return nil
}

func (d *dirNode) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error {
	nd, ok := newDir.(*dirNode)
	if !ok {
		return fuse.EIO
	}

	oldRel := joinRel(d.rel, req.OldName)
	newRel := joinRel(nd.rel, req.NewName)

	oldPath := filepath.Join(d.o.lower, oldRel)
	newPath := filepath.Join(d.o.lower, newRel)

	if err := os.Rename(oldPath, newPath); err != nil {
		return err
	}

	if md, ok := d.o.getMeta(oldRel); ok {
		d.o.setMeta(newRel, md)
		d.o.delMeta(oldRel)
	}
	return nil
}

// --- file node ---

type fileNode struct {
	o   *overlayFS
	rel string
}

func (f *fileNode) lowerPath() string {
	return filepath.Join(f.o.lower, f.rel)
}

func (f *fileNode) Attr(ctx context.Context, a *fuse.Attr) error {
	st, err := os.Lstat(f.lowerPath())
	if err != nil {
		if os.IsNotExist(err) {
			return fuse.ENOENT
		}
		log.Printf("File Attr FAILED rel=%q lower=%q err=%v", f.rel, f.lowerPath(), err)
		return err
	}

	fillAttrFromInfo(st, a)

	if md, ok := f.o.getMeta(f.rel); ok {
		a.Mode = (a.Mode &^ os.ModePerm) | os.FileMode(md.Mode)&os.ModePerm
		a.Uid = md.UID
		a.Gid = md.GID
	} else {
		a.Mode = (a.Mode &^ os.ModePerm) | f.o.defFileMode
	}
	return nil
}

func (d *dirNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	lp := d.lowerPath()

	f, err := os.Open(lp)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}
		log.Printf("Dir Open FAILED rel=%q lower=%q err=%v", d.rel, lp, err)
		return nil, err
	}

	return &dirHandle{f: f, rel: d.rel, lowerDir: lp}, nil
}

func (h *dirHandle) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	entries, err := os.ReadDir(h.lowerDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		}
		log.Printf("Dir ReadDirAll FAILED rel=%q lower=%q err=%v", h.rel, h.lowerDir, err)
		return nil, err
	}

	out := make([]fuse.Dirent, 0, len(entries))
	for _, e := range entries {
		name := e.Name()
		if name == ".posixmeta" {
			continue
		}
		var t fuse.DirentType
		if e.IsDir() {
			t = fuse.DT_Dir
		} else {
			t = fuse.DT_File
		}
		out = append(out, fuse.Dirent{Name: name, Type: t})
	}
	return out, nil
}

func (h *dirHandle) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	log.Printf("Fsync rel=%q", h.rel)

	if err := h.f.Sync(); err != nil {
		log.Printf("Dir Fsync ignored rel=%q err=%v", h.rel, err)
	}
	return nil
}

func (h *dirHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	if err := h.f.Sync(); err != nil {
		log.Printf("Dir Flush ignored rel=%q err=%v", h.rel, err)
	}
	return nil
}

func (h *dirHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if err := h.f.Close(); err != nil {
		log.Printf("Dir Release(close) ignored rel=%q err=%v", h.rel, err)
		return nil
	}
	return nil
}

func (f *fileNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	oflags := openFlags(req.Flags)
	of, err := os.OpenFile(f.lowerPath(), oflags, 0)
	if err != nil {
		log.Printf("Open FAILED rel=%q lower=%q reqFlags=%v oflags=%#x err=%v",
			f.rel, f.lowerPath(), req.Flags, oflags, err)
		return nil, err
	}
	return &fileHandle{
		f:      of,
		rel:    f.rel,
		o:      f.o,
		append: (oflags&os.O_APPEND != 0),
	}, nil
}

func (f *fileNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	md, ok := f.o.getMeta(f.rel)
	if !ok {
		md = meta{Mode: uint32(f.o.defFileMode & os.ModePerm), UID: uint32(os.Getuid()), GID: uint32(os.Getgid())}
	}

	if req.Valid.Mode() {
		md.Mode = uint32(req.Mode & os.ModePerm)
	}
	if req.Valid.Uid() {
		md.UID = req.Uid
	}
	if req.Valid.Gid() {
		md.GID = req.Gid
	}
	f.o.setMeta(f.rel, md)

	if req.Valid.Mtime() || req.Valid.Atime() {
		at := time.Now()
		mt := time.Now()
		if req.Valid.Atime() {
			at = req.Atime
		}
		if req.Valid.Mtime() {
			mt = req.Mtime
		}

		if err := os.Chtimes(f.lowerPath(), at, mt); err != nil {
			log.Printf("Setattr Chtimes ignored rel=%q err=%v", f.rel, err)
			// do NOT return err
		}
		//_ = os.Chtimes(f.lowerPath(), at, mt)
	}

	return f.Attr(ctx, &resp.Attr)
}

// --- file handle ---

type fileHandle struct {
	f      *os.File
	rel    string
	o      *overlayFS
	append bool
}

type dirHandle struct {
	f        *os.File
	rel      string
	lowerDir string
}

func (h *fileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	if err := h.f.Sync(); err != nil {
		log.Printf("File Flush ignored rel=%q err=%v", h.rel, err)
	}
	return nil
}

func (h *fileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// Sequential read: seek then read
	if _, err := h.f.Seek(req.Offset, io.SeekStart); err != nil {
		log.Printf("Read Seek FAILED rel=%q off=%d err=%v", h.rel, req.Offset, err)
		return err
	}
	buf := make([]byte, req.Size)
	n, err := h.f.Read(buf)
	if err != nil && err != io.EOF {
		log.Printf("Read FAILED rel=%q off=%d size=%d err=%v", h.rel, req.Offset, req.Size, err)
		return err
	}
	resp.Data = buf[:n]
	return nil
}

func (h *fileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// If file opened with O_APPEND, ignore req.Offset and just Write.
	// This is REQUIRED for ">>" redirection semantics to work reliably.
	if !h.append {
		if _, err := h.f.Seek(req.Offset, io.SeekStart); err != nil {
			log.Printf("Write Seek FAILED rel=%q off=%d err=%v", h.rel, req.Offset, err)
			return err
		}
	}

	n, err := h.f.Write(req.Data)
	if err != nil {
		log.Printf("Write FAILED rel=%q append=%v off=%d len=%d err=%v",
			h.rel, h.append, req.Offset, len(req.Data), err)
		return err
	}
	resp.Size = n
	return nil
}

func (h *fileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	if err := h.f.Close(); err != nil {
		// Editors may treat close() errors as fsync/write failure.
		// On object-store/FUSE stacks this can happen; ignore to allow saves.
		log.Printf("File Release(close) ignored rel=%q err=%v", h.rel, err)
		return nil
	}
	return nil
}

// --- metadata storage ---

func (o *overlayFS) metaPath(rel string) string {
	sum := sha1.Sum([]byte(rel))
	fn := hex.EncodeToString(sum[:]) + ".json"
	return filepath.Join(o.metaDir, fn)
}

func (o *overlayFS) getMeta(rel string) (meta, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()

	var md meta
	b, err := os.ReadFile(o.metaPath(rel))
	if err != nil {
		return md, false
	}
	if err := json.Unmarshal(b, &md); err != nil {
		return meta{}, false
	}
	return md, true
}

func (o *overlayFS) setMeta(rel string, md meta) {
	o.mu.Lock()
	defer o.mu.Unlock()

	_ = os.MkdirAll(o.metaDir, 0o777)

	b, _ := json.Marshal(md)

	// Write directly (no rename) for compatibility with gcsfuse/object-store semantics.
	if err := os.WriteFile(o.metaPath(rel), b, 0o666); err != nil {
		log.Printf("setMeta FAILED rel=%q meta=%q err=%v", rel, o.metaPath(rel), err)
		// best-effort: do not fail the filesystem operation because metadata failed
	}
}

func (o *overlayFS) delMeta(rel string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	_ = os.Remove(o.metaPath(rel))
}

// --- helpers ---

func joinRel(parent, name string) string {
	if parent == "" {
		return name
	}
	return filepath.Join(parent, name)
}

func parseOctalMode(s string) (os.FileMode, error) {
	s = strings.TrimSpace(s)
	var v uint32
	_, err := fmt.Sscanf(s, "%o", &v)
	if err != nil {
		return 0, err
	}
	return os.FileMode(v), nil
}

func openFlags(ff fuse.OpenFlags) int {
	flags := 0

	// access mode
	switch ff & fuse.OpenAccessModeMask {
	case fuse.OpenReadOnly:
		flags |= os.O_RDONLY
	case fuse.OpenWriteOnly:
		flags |= os.O_WRONLY
	case fuse.OpenReadWrite:
		flags |= os.O_RDWR
	default:
		flags |= os.O_RDONLY
	}

	// common modifiers
	if ff&fuse.OpenAppend != 0 {
		flags |= os.O_APPEND
	}
	if ff&fuse.OpenTruncate != 0 {
		flags |= os.O_TRUNC
	}

	// NOTE: we intentionally do not try to map every Linux open(2) flag here.
	// gcsfuse can be sensitive; keeping it minimal improves compatibility.
	return flags
}

func fillAttrFromInfo(fi os.FileInfo, a *fuse.Attr) {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if ok {
		a.Inode = st.Ino
		a.Size = uint64(fi.Size())
		a.Blocks = uint64(st.Blocks)
		a.Atime = time.Unix(int64(st.Atim.Sec), int64(st.Atim.Nsec))
		a.Mtime = time.Unix(int64(st.Mtim.Sec), int64(st.Mtim.Nsec))
		a.Ctime = time.Unix(int64(st.Ctim.Sec), int64(st.Ctim.Nsec))
		a.Uid = st.Uid
		a.Gid = st.Gid
	} else {
		a.Size = uint64(fi.Size())
		a.Mtime = fi.ModTime()
		a.Uid = uint32(os.Getuid())
		a.Gid = uint32(os.Getgid())
	}
	a.Mode = fi.Mode()
}
