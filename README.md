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

### GCP Environment
GCP uses legacy fuse, but basel/fuse uses fusermount3.

For this reason this might require adding some one-time "hack" to create fusermount3 that calls fusermount:

```
mkdir -p "$HOME/bin"

cat > "$HOME/bin/fusermount3" <<'EOF'
#!/bin/sh
exec /usr/bin/fusermount "$@"
EOF

chmod +x "$HOME/bin/fusermount3"

export PATH="$HOME/bin:$PATH"

# sanity
which fusermount3
fusermount3 -V || true
```

## Build
```
go build -o posixlite posixlite.go
```

## Notes
Vibecoded with brain :)

## Author
Michal Zygmunt
