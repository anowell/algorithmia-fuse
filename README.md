# algorithmia-fuse
Experimental: FUSE-based Algorithmia FileSystem

---

Maybe this will get a real explanation at some point, but for now, figure it out from this screenshot:

![Screenshot](https://dl.dropboxusercontent.com/u/39033486/Algorithmia/algofs-walking.png)

## Progress
- [x] Basic downward dir traversal (Initial POC)
- [x] Basic upward dir traversal (Refactored to use a sequential trie to easily lookup parent)
- [ ] Getting attributes for a file that hasn't been traversed (i.e. handle cache misses in `lookup`)
- [ ] Connector support (i.e. fix upstream rust client to sanely handle connector paths)
- [ ] Reading files (i.e. impl `read` - gonna have to experiment a bit to understand how to leverage offset/size)
- [ ] Writing files (i.e. impl `write`, `mknod` - probably `fsync` but need to experiment to better understand `flush`)  
- [ ] Deleting files and dirs (i.e. impl `rmdir`, `unlink`, and probably `forget` - may need to refactor inode structure)
- [ ] Making directories (i.e. impl `mkdir`)
- [ ] All the hard problems like caching, large files, large directories, inode limits, optimizing
