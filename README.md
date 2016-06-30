# algorithmia-fuse
Experimental: FUSE-based Algorithmia FileSystem

A user-mode virtual filesystem backed by the Algorithmia API. Basically, it handles filesystem requests by turning them into API calls and lazily building a local cache of remote resources. The end result is that you can mount Algorithmia data to a local directory, and use standard file operations to work with Algorithmia data (all the standard caveats of networked filesystems apply).

Screenshots demonstrate walking the your Algorithmia data with basic `ls` operations or a file explorer:

![Screenshot](https://dl.dropboxusercontent.com/u/39033486/Algorithmia/algofs-walking.png)

![Screenshot](https://dl.dropboxusercontent.com/u/39033486/Algorithmia/algofs-explore.png)

## Progress
- [ ] Read-Only Filesystem
  - [x] Basic downward dir traversal (Initial POC)
  - [x] Basic upward dir traversal (Refactored to use a sequential trie to easily lookup parent)
  - [x] Getting attributes for a file that hasn't been traversed (i.e. handle cache misses in `lookup`)
  - [ ] Connector support (very limited support until some upstream issues are address - tracking in #1)
- [ ] Writeable Filesystem
  - [ ] Reading files (i.e. impl `read` - gonna have to experiment a bit to understand how to leverage offset/size)
  - [ ] Writing files (i.e. impl `write`, `mknod` - probably `fsync` but need to experiment to better understand `flush`)
  - [ ] Deleting files and dirs (i.e. impl `rmdir`, `unlink`, and probably `forget` which will require refactoring inode storage)
  - [ ] Making directories (i.e. impl `mkdir`)
- [ ] Production Filesystem
  - [ ] All the hard problems like sane caching, large files, large directories, inode limits, optimizing

Crazy idea: Experiment with representing algorithms as `FileType::NamedPipe` under `<mountpoint>/algorithms/<username>/<algoname>/<version>` so you can have one handle that pipes data to an algorithm and another handle that receives responses. (e.g. `echo 'Are you pondering?' > ~/algofs/algorithms/anowell/Pinky/latest`)
