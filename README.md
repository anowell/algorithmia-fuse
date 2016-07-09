# algorithmia-fuse
Experimental: FUSE-based Algorithmia FileSystem

A user-mode virtual filesystem backed by the Algorithmia API. Basically, it handles filesystem requests by turning them into API calls and lazily building a local cache of remote resources. The end result is that you can mount Algorithmia data to a local directory, and use standard file operations to work with Algorithmia data (all the standard caveats of networked filesystems apply).

Screenshots demonstrate basic traversal and read operations from CLI and file explorer:

![Screenshot](https://dl.dropboxusercontent.com/u/39033486/Algorithmia/algofs-walk-and-grep.png)

![Screenshot](https://dl.dropboxusercontent.com/u/39033486/Algorithmia/algofs-reading-files.png)

## Progress
- [x] Read-Only Filesystem
  - [x] Basic filesystem traversal
  - [x] Connector support (very limited, blocked by API - see [Issue #1](../../issues/1))
  - [x] Basic file reading (read cache caveats: see [Issue #2](../../isues/2))
- [ ] Writeable Filesystem
  - [x] Writing files (saves to API on file close, or explicit `fsync`, [upstream discussion](https://github.com/zargony/rust-fuse/issues/67) to explore deferred commits)
  - [ ] Deleting files and dirs (i.e. impl `rmdir`, `unlink`, and probably `forget`)
  - [ ] Making directories (i.e. impl `mkdir`)
- [ ] Production Filesystem
  - [ ] All the hard problems like sane caching, multi-process data races, large files, large directories, inode limits, optimizing, better timestamp consistency

Idea: Experiment with representing algorithms as `FileType::NamedPipe` under `<mountpoint>/algorithms/<username>/<algoname>/<version>` so you can have one handle that pipes data to an algorithm and another handle that receives responses. (e.g. `echo 'Are you pondering?' > ~/algofs/algorithms/anowell/Pinky/latest`)

## Build, Test, Run, Debug

To build and test (tests are pretty barebones):
```
$ cargo build
$ cargo test
```

To mount the filesystem:
```
$ mkdir ~/algofs
$ target/debug/algofs ~/algofs
```

The `algofs` executable will print all the current debug output,
so currently it works best to browse the `~/algofs` from another terminal.

Note: some shell enhancements can cause a lot of extra listing operations.
And file explorers may trigger a lot of extra reads to preload or preview files.

To stop algofs, unmount it as root. (Note: killing `algofs` will stop request handling, but leaves `~/algofs` as a volume with no transport connected).
```
fusermount -u ~/algofs
# or `sudo umount ~/algofs`
```

