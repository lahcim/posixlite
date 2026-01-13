# posixlite

 Minimal POSIX-metadata overlay filesystem using FUSE (bazil.org/fuse).
 - Lower: existing directory (e.g., gcsfuse mount)
 - Upper: FUSE mountpoint
 - Persists chmod/chown by saving metadata JSON files under Lower/.posixmeta/

 Build:  go build -o posixlite posixlite.go
 Run:    ./posixlite -lower ~/dev_gcs_lower -mount ~/dev

 Notes:
 - Intentionally minimal. Good enough for chmod +x persistence.
 - Metadata persists in the lower tree as objects/files.


## Usage
```
sudo -u <user> gcsfuse -o nonempty -file-mode=777 -dir-mode=777 --debug_gcs <gcs.url.com> ~/dev_gcs_lower
./posixlite -lower ~/dev_gcs_lower -mount ~/dev
```

Unmount:
- Ctrl+C
- ```fusermount -uz ~/dev```

## Build
```
go build -o posixlite posixlite.go
```
