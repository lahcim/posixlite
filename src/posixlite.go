// posixlite.go
//
// Minimal POSIX-metadata overlay filesystem using FUSE (bazil.org/fuse).
// - Lower: existing directory (e.g., gcsfuse mount)
// - Upper: FUSE mountpoint
// - Persists chmod/chown by saving metadata JSON files under Lower/.posixmeta/
//
// Build:  go build -o posixlite posixlite.go
// Run:    ./posixlite -lower ~/dev_gcs_lower -mount ~/dev2
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
	flag.Parse()

	if *lower == "" || *mount == "" {
		log.Fatalf("Usage: %s -lower <dir> -mount <dir>", os.Args[0])
	}

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

	// Create mountpoint if needed
	if err := os.MkdirAll(mountAbs, 0o777); err != nil {
		log.Fatalf("failed to create mount dir %s: %v", mountAbs, err)
	}

	// Metadata directory lives in the lower tree (persisted in GCS via gcsfuse)
	metaDir := filepath.Join(lowerAbs, ".posixmeta")
	if err := os.MkdirAll(metaDir, 0o777); err != nil {
		log.Fatalf("failed to create metadata dir %s: %v", metaDir, err)
	}

	// --- Self-heal: clean up stale mounts from previous crash/kill -9 ---
	// Best-effort unmount (ignore errors if not mounted)
	_ = fuse.Unmount(mountAbs)

	// In Cloud Shell it's common for stale mounts to require a "lazy" unmount.
	// We do this best-effort; if fusermount isn't present, it just fails silently.
	// NOTE: this is optional but helps a lot after SIGKILL.
	_ = exec.Command("fusermount", "-uz", mountAbs).Run()
	_ = exec.Command("fusermount3", "-uz", mountAbs).Run()

	// --- Install signal handling (Ctrl+C, SIGTERM, SIGHUP) ---
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer stop()

	// Mount with conservative options compatible across bazil.org/fuse versions
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

	// Wait for either a signal or Serve() finishing with error
	select {
	case <-ctx.Done():
		// Shutdown path: unmount + close conn so Serve() unblocks
		_ = fuse.Unmount(mountAbs)
		_ = c.Close()

		// If unmount is still stuck (open files), attempt lazy unmount as a last resort
		time.Sleep(200 * time.Millisecond)
		_ = exec.Command("fusermount", "-uz", mountAbs).Run()
		_ = exec.Command("fusermount3", "-uz", mountAbs).Run()

		// Drain serve error if it returns
		select {
		case err := <-serveErr:
			if err != nil && err != io.EOF {
				log.Printf("fs.Serve returned: %v", err)
			}
		default:
		}

	case err := <-serveErr:
		// Serve finished (usually means mount went away or an error occurred)
		if err != nil && err != io.EOF {
			log.Printf("fs.Serve error: %v", err)
		}
		_ = c.Close()
	}

	// Final best-effort unmount on exit
	_ = fuse.Unmount(mountAbs)
	_ = exec.Command("fusermount", "-uz", mountAbs).Run()
	_ = exec.Command("fusermount3", "-uz", mountAbs).Run()
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

func (d *dirNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	if name == ".posixmeta" {
		return nil, fuse.ENOENT
	}
	childRel := joinRel(d.rel, name)
	lp := filepath.Join(d.o.lower, childRel)
	st, err := os.Lstat(lp)
	if err != nil {
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

	f, err := os.OpenFile(lp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, req.Mode)
	if err != nil {
		return nil, nil, err
	}
	d.o.setMeta(childRel, meta{
		Mode: uint32(req.Mode & os.ModePerm),
		UID:  uint32(os.Getuid()),
		GID:  uint32(os.Getgid()),
	})

	n := &fileNode{o: d.o, rel: childRel}
	h := &fileHandle{f: f, rel: childRel, o: d.o}
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

func (f *fileNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	flags := openFlags(req.Flags)
	of, err := os.OpenFile(f.lowerPath(), flags, 0)
	if err != nil {
		return nil, err
	}
	return &fileHandle{f: of, rel: f.rel, o: f.o}, nil
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
		_ = os.Chtimes(f.lowerPath(), at, mt)
	}

	return f.Attr(ctx, &resp.Attr)
}

// --- file handle ---

type fileHandle struct {
	f   *os.File
	rel string
	o   *overlayFS
}

func (h *fileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.f.ReadAt(buf, req.Offset)
	if err != nil && err != io.EOF {
		return err
	}
	resp.Data = buf[:n]
	return nil
}

func (h *fileHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := h.f.WriteAt(req.Data, req.Offset)
	if err != nil {
		return err
	}
	resp.Size = n
	return nil
}

func (h *fileHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return h.f.Sync()
}

func (h *fileHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.f.Close()
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
	tmp := o.metaPath(rel) + ".tmp"
	_ = os.WriteFile(tmp, b, 0o666)
	_ = os.Rename(tmp, o.metaPath(rel))
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
	switch ff & fuse.OpenAccessModeMask {
	case fuse.OpenReadOnly:
		return os.O_RDONLY
	case fuse.OpenWriteOnly:
		return os.O_WRONLY
	case fuse.OpenReadWrite:
		return os.O_RDWR
	default:
		return os.O_RDONLY
	}
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
