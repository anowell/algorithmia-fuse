extern crate algorithmia;
extern crate fuse;
extern crate libc;
extern crate time;
extern crate sequence_trie;

mod inode;

use algorithmia::*;
use algorithmia::data::*;
use fuse::*;
use libc::{ENOENT, EIO, EROFS, ENOSYS};
use std::collections::HashMap;
use std::io::Read;
use std::path::Path;
use time::Timespec;
use inode::{Inode, InodeStore};

// 2015-03-12 00:00 PST Algorithmia Launch
pub const DEFAULT_TIME: Timespec = Timespec { sec: 1426147200, nsec: 0 };
pub const DEFAULT_TTL: Timespec = Timespec { sec: 1, nsec: 0 };

pub struct MountOptions<'a> {
    path: &'a Path,
    uid: u32,
    gid: u32,
    // read_only: bool,
}

impl <'a> MountOptions<'a> {
    pub fn new<P: AsRef<Path>>(path: &P) -> MountOptions {
        MountOptions {
            path: path.as_ref(),
            uid: unsafe { libc::getuid() } as u32,
            gid: unsafe { libc::getgid() } as u32,
            // read_only: false,
        }
    }
}

pub struct AlgoFs {
    inodes: InodeStore,
    /// map of inodes to to data buffers - indexed by inode (NOT inode-1)
    cache: HashMap<u64, Vec<u8>>,
    client: Algorithmia,
    uid: u32,
    gid: u32,
}

impl AlgoFs {
    pub fn mount(options: MountOptions, client: Algorithmia) {
        let data_root = FileAttr {
            ino: 2,
            size: 0,
            blocks: 0,
            atime: DEFAULT_TIME,
            mtime: DEFAULT_TIME,
            ctime: DEFAULT_TIME,
            crtime: DEFAULT_TIME,
            kind: FileType::Directory,
            perm: 0o550,
            nlink: 2,
            uid: options.uid,
            gid: options.gid,
            rdev: 0,
            flags: 0,
        };

        let mut inodes = InodeStore::new(0o550, options.uid, options.gid);
        inodes.insert(Inode::new("data", data_root));

        let adfs = AlgoFs {
            client: client,
            inodes: inodes,
            cache: HashMap::new(),
            uid: options.uid,
            gid: options.gid,
        };
        fuse::mount(adfs, &options.path, &[]);
    }

    fn cache_listdir<'a>(&'a self, ino: u64, offset: u64, mut reply: ReplyDirectory) {
        for (i, child) in self.inodes.children(ino).iter().enumerate().skip(offset as usize) {
            reply.add(child.attr.ino,
                i as u64 + offset + 2,
                child.attr.kind,
                get_basename(&child.path));
        }
        reply.ok();
    }

    // TODO: support a page/offset type of arg?
    fn algo_listdir<'a>(&'a mut self, ino: u64, offset: u64, mut reply: ReplyDirectory) {
        // TODO: support offset
        if offset > 0 {
            return reply.ok();
        }

        let uri = {
            let inode = self.inodes.get(ino).expect(&format!("path not found for inode {}", ino));
            path_to_uri(&inode.path)
        };

        println!("Fetching algo dir listing for inode: {} (+{}) => {}",
                 ino,
                 offset,
                 uri);

        {
            // Mark this node visited
            let ref mut inodes = self.inodes;
            let mut dir_inode = inodes.get_mut(ino).expect("inode missing for dir just listed");
            dir_inode.visited = true;
        }

        for (i, entry_result) in self.client.dir(&uri).list().enumerate() {
            let child = match entry_result {
                Ok(DataItem::Dir(d)) => {
                    let path = uri_to_path(&d.to_data_uri());
                    self.insert_dir(&path, DEFAULT_TIME, 0o750)
                }
                Ok(DataItem::File(f)) => {
                    let path = uri_to_path(&f.to_data_uri());
                    let mtime = Timespec::new(f.last_modified.timestamp(), 0);
                    self.insert_file(&path, mtime, f.size)
                }
                Err(err) => {
                    println!("Error listing directory: {}", err);
                    return reply.error(ENOENT);
                }
            };
            reply.add(child.attr.ino,
                i as u64 + offset + 2,
                child.attr.kind,
                get_basename(&child.path));
        }
        reply.ok()
    }

    // TODO: support a page/offset type of arg?
    fn algo_lookup(&mut self, path: &str) -> Result<&Inode, String> {
        let uri = path_to_uri(&path);
        println!("algo_lookup: {}", uri);
        match self.client.data(&uri).into_type() {
            Ok(DataItem::Dir(_)) => {
                // TODO: API should indicate not listable
                Ok(self.insert_dir(&path, DEFAULT_TIME, 0o750))
            }
            Ok(DataItem::File(f)) => {
                Ok(self.insert_file(&path, Timespec::new(f.last_modified.timestamp(), 0), f.size))
            }
            Err(err) => Err(err.to_string()),
        }
    }

    // TODO: support a page/offset type of arg?
    fn algo_read(&mut self, path: &str) -> Result<Vec<u8>, String> {
        let uri = path_to_uri(&path);
        println!("algo_read: {}", uri);
        match self.client.file(&uri).get() {
            Ok(mut response) => {
                let mut buffer = Vec::new();
                let _ = response.read_to_end(&mut buffer);
                Ok(buffer)
            }
            Err(err) => Err(err.to_string()),
        }
    }

    fn insert_dir(&mut self, path: &str, mtime: Timespec, perm: u16) -> &Inode {
        let ref mut inodes = self.inodes;
        let ino = inodes.len() as u64 + 1;
        println!("insert_dir: {} {}", ino, path);

        let attr = FileAttr {
            ino: ino,
            size: 0,
            blocks: 0,
            atime: mtime,
            mtime: mtime,
            ctime: mtime,
            crtime: mtime,
            kind: FileType::Directory,
            perm: perm,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        };
        inodes.insert(Inode::new(path, attr));
        inodes.get(ino).unwrap()
    }

    fn insert_file(&mut self, path: &str, mtime: Timespec, size: u64) -> &Inode {
        let ref mut inodes = self.inodes;
        let ino = inodes.len() as u64 + 1;
        println!("insert_file: {} {}", ino, path);

        let attr = FileAttr {
            ino: ino,
            size: size,
            blocks: 0,
            atime: mtime,
            mtime: mtime,
            ctime: mtime,
            crtime: mtime,
            kind: FileType::RegularFile,
            perm: 0o640,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        };
        inodes.insert(Inode::new(path, attr));
        inodes.get(ino).unwrap()
    }
}

impl Filesystem for AlgoFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        let name = name.to_string_lossy();
        println!("lookup(parent={}, name=\"{}\")", parent, name);
        match (parent, name.as_ref()) {
            (1, "") => reply.entry(&DEFAULT_TTL, &self.inodes[1].attr, 0),
            (1, "data") => reply.entry(&DEFAULT_TTL, &self.inodes[2].attr, 0),
            (1, connector) if !connector.starts_with("dropbox") && !connector.starts_with("s3") => {
                // Filesystems look for a bunch of junk in the rootdir by default, so lets whitelist supported connector prefixes
                reply.error(ENOENT);
            }
            _ => {
                // Clone until MIR NLL lands
                match self.inodes.child(parent, &name).cloned() {
                    Some(child_inode) => reply.entry(&DEFAULT_TTL, &child_inode.attr, 0),
                    None => {
                        // Clone until MIR NLL lands
                        let parent_inode = self.inodes[parent].clone();
                        if parent_inode.visited {
                            println!("lookup - short-circuiting cache miss");
                            reply.error(ENOENT);
                        } else {
                            let child_path = format!("{}/{}", parent_inode.path, name);
                            match self.algo_lookup(&child_path) {
                                Ok(child_inode) => reply.entry(&DEFAULT_TTL, &child_inode.attr, 0),
                                Err(err) => {
                                    println!("lookup error - {}", err);
                                    reply.error(ENOENT);
                                }
                            }
                        }
                    }
                }
            }
        };
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.inodes.get(ino) {
            Some(inode) => reply.attr(&DEFAULT_TTL, &inode.attr),
            None => {
                println!("getattr ENOENT: {}", ino);
                reply.error(ENOENT);
            }
        };
    }

    // TODO: don't buffer the whole thing. Just store the DataResponse and read size at a time as long as offsets line up
    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, size: u32, reply: ReplyData) {
        println!("read(ino={}, fh={}, offset={}, size={})", ino, _fh, offset, size);

        if offset == 0 || !self.cache.contains_key(&ino) {
            // Clone until MIR NLL
            let path = self.inodes[ino].path.clone();
            let response = self.algo_read(&path);
            match response {
                Ok(buffer) => self.cache.insert(ino, buffer),
                Err(err) => {
                    println!("read error: {}", err);
                    reply.error(EIO);
                    return
                }
            };
        }

        let reset_cache = {
            let buffer = self.cache.get(&ino).unwrap();

            let end_offset = offset + size as u64;
            match buffer.len() {
                len if len as u64 > offset + size as u64 => {
                    reply.data(&buffer[(offset as usize)..(end_offset as usize)]);
                    false
                }
                len if len as u64 > offset => {
                    reply.data(&buffer[(offset as usize)..]);
                    true
                }
                len => {
                    println!("attempted read beyond buffer for ino {} len={} offset={} size={}", ino, len, offset, size);
                    reply.error(ENOENT);
                    true
                }
            }
        };

        if reset_cache {
            // FIXME: data race if 2 processes were reading the same file.
            let _ = self.cache.remove(&ino);
        }
    }

    fn readdir(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, mut reply: ReplyDirectory) {
        if offset > 0 {
            reply.ok();
            return;
        }

        match (ino, offset) {
            (1, 0) => {
                reply.add(1, 0, FileType::Directory, ".");
                reply.add(1, 1, FileType::Directory, "..");
                reply.add(2, 2, FileType::Directory, "data");
                reply.ok();
            }
            (1, _) => reply.ok(),
            _ => {
                if offset == 0 {
                    let parent = self.inodes.parent(ino).expect("inode has no parent");
                    reply.add(ino, 0, FileType::Directory, ".");
                    reply.add(parent.attr.ino, 1, FileType::Directory, "..");
                }

                let dir_visited  = self.inodes.get(ino).map(|n| n.visited).unwrap_or(false);
                match dir_visited {
                    true => self.cache_listdir(ino, offset, reply),
                    false => self.algo_listdir(ino, offset, reply),
                };
            },
        }
    }

    fn open (&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        println!("open(ino={}, flags=0x{:x})", ino, flags);
        // match flags & O_ACCMODE => O_RDONLY, O_WRONLY, O_RDWR
        // flags & O_CREAT => create if not exist
        //

        reply.opened(0, 0);
    }

    fn write (&mut self, _req: &Request, ino: u64, fh: u64, offset: u64, data: &[u8], flags: u32, reply: ReplyWrite) {
        // TODO: check if in read-only mode: EROFS
        println!("write(ino={}, fh={}, offset={}, flags=0x{:x})", ino, fh, offset, flags);

        // if offset==0 && data.len() >= inode.attr.size {
        //    TODO: skip data cache lookup
        // }
        if !self.cache.contains_key(&ino) {
            // Clone until MIR NLL
            let path = self.inodes[ino].path.clone();

            // TODO: check if file exists
            let response = self.algo_read(&path);
            match response {
                Ok(buffer) => self.cache.insert(ino, buffer),
                Err(err) => {
                    println!("read error: {}", err);
                    reply.error(EIO);
                    return
                }
            };
        }

        let new_size = match self.cache.get_mut(&ino) {
            Some(ref mut cache_line) => {
                let end = data.len() + offset as usize;
                if end > self.inodes[ino].attr.size as usize {
                    cache_line.resize(end, 0);
                }
                cache_line[(offset as usize)..end].copy_from_slice(data);
                println!("update cache for ino={} for range {}..{}", ino, offset, end);
                reply.written(data.len() as u32);
                cache_line.len() as u64
            }
            None => {
                println!("write failed to read file");
                reply.error(ENOENT);
                return;
            }
        };

        let ref mut inode = self.inodes[ino];
        inode.attr.size = new_size;
    }

    fn fsync (&mut self, _req: &Request, ino: u64, fh: u64, datasync: bool, reply: ReplyEmpty) {
        println!("fsync(ino={}, fh={}, datasync={})", ino, fh, datasync);
        reply.error(ENOSYS);
    }

    /// Remove a file
    fn unlink (&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEmpty) {
        println!("unlink(parent={}, name=\"{}\")", parent, name.to_string_lossy());
        reply.error(ENOSYS);
    }

    fn setattr (&mut self, _req: &Request, ino: u64, _mode: Option<u32>, uid: Option<u32>, gid: Option<u32>, size: Option<u64>, _atime: Option<Timespec>, _mtime: Option<Timespec>, _fh: Option<u64>, _crtime: Option<Timespec>, _chgtime: Option<Timespec>, _bkuptime: Option<Timespec>, flags:               Option<u32>, reply: ReplyAttr) {
        println!("setattr(ino={}, mode={:?}, size={:?}, fh={:?}, flags={:?})", ino, _mode, size, _fh, flags);
        match self.inodes.get_mut(ino) {
            Some(mut inode) => {
                if let Some(new_size) = size {
                    inode.attr.size = new_size;
                }
                if let Some(new_uid) = uid {
                    inode.attr.uid = new_uid;
                }
                if let Some(new_gid) = gid {
                    inode.attr.gid = new_gid;
                }
                // TODO: is mode (u32) equivalent to attr.perm (u16)?
                reply.attr(&DEFAULT_TTL, &inode.attr);
            }
            None => reply.error(ENOENT)
        }
    }
}

pub fn path_to_uri(path: &str) -> String {
    let parts: Vec<_> = match path.starts_with("/") {
        true => &path[1..],
        false => &path,
    }.splitn(2, "/").collect();

    match parts.len() {
        1 => format!("{}://", parts[0]),
        2 => parts.join("://"),
        _ => unreachable!(),
    }
}

pub fn uri_to_path(uri: &str) -> String {
    let parts: Vec<_> = uri.splitn(2, "://").collect();
    match parts.len() {
        1 if parts[0].is_empty() => "data".to_string(),
        1 => format!("data/{}", parts[0]),
        2 if parts[1].is_empty() => parts[0].to_string(),
        2 => parts.join("/"),
        _ => unreachable!(),
    }
}


fn get_basename(path: &str) -> String {
    path.rsplitn(2, "/").next().unwrap().to_string()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_path_to_uri() {
        assert_eq!(&*path_to_uri("data"), "data://");
        assert_eq!(&*path_to_uri("data/foo"), "data://foo");
        assert_eq!(&*path_to_uri("data/foo/bar.txt"), "data://foo/bar.txt");
    }

    #[test]
    fn test_uri_to_path() {
        assert_eq!(&*uri_to_path("data://"), "data");
        assert_eq!(&*uri_to_path("data://foo"), "data/foo");
        assert_eq!(&*uri_to_path("data://foo/bar.txt"), "data/foo/bar.txt");
    }

}
