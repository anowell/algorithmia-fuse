extern crate fuse;
extern crate libc;
extern crate time;
extern crate algorithmia;

use algorithmia::*;
use algorithmia::data::*;
use std::env;
use std::path::Path;
use libc::{ENOENT, EINTR};
use time::Timespec;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use std::collections::HashMap;
const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

const CREATE_TIME: Timespec = Timespec { sec: 1381237736, nsec: 0 };    // 2013-10-08 08:56


pub struct AlgoFs {
    // indexed by inode-1
    inodes: Vec<FileAttr>,
    // indexed by inode-1
    paths: Vec<String>,
    /// map of inodes to to data buffers - indexed by inode (NOT inode-1)
    _cache: HashMap<u64, Vec<u8>>, //
    /// map of inodes to child inodes -  indexed by inode (NOT inode-1) - TODO: use a trie
    children: HashMap<u64, Vec<u64>>,
    client: Algorithmia,
    uid: u32,
    gid: u32,
}

impl AlgoFs {
    pub fn mount<P: AsRef<Path>>(path: &P) {
        // TODO: allow setting uid/gid for FS
        let api_key = env::var("ALGORITHMIA_API_KEY").expect("Must set ALGORITHMIA_API_KEY");
        let client = Algorithmia::client(&*api_key);
        let uid = unsafe { libc::getuid() } as u32;
        let gid = unsafe { libc::getgid() } as u32;
        let adfs_root = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: CREATE_TIME,
            mtime: CREATE_TIME,
            ctime: CREATE_TIME,
            crtime: CREATE_TIME,
            kind: FileType::Directory,
            perm: 0o550,
            nlink: 2,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
        };
        let data_root = FileAttr {
            ino: 2,
            size: 0,
            blocks: 0,
            atime: CREATE_TIME,
            mtime: CREATE_TIME,
            ctime: CREATE_TIME,
            crtime: CREATE_TIME,
            kind: FileType::Directory,
            perm: 0o550,
            nlink: 2,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
        };

        let mut inodes = Vec::with_capacity(1024);
        let mut paths = Vec::with_capacity(1024);
        let mut children = HashMap::new();
        inodes.push(adfs_root);
        inodes.push(data_root);
        paths.push("".into());
        paths.push("data".into());
        children.insert(1, vec![2]);

        let adfs = AlgoFs {
            client: client,
            inodes: inodes,
            paths: paths,
            _cache: HashMap::new(),
            children: children,
            uid: uid,
            gid: gid,
        };
        fuse::mount(adfs, path, &[]);
    }

    fn cache_listdir<'a>(&self, ino: u64, offset: u64) -> Result<Vec<FileAttr>, String> {
        // TODO: support offset
        if offset > 0 {
            return Ok(vec![]);
        }

        match self.children.get(&ino) {
            Some(children) => Ok(children.iter().map(|c_ino| self.inodes[(c_ino-1) as usize].clone() ).collect()),
            None => Err(format!("Cache miss - should not have called `cache_listdir`")),
        }
    }

    // TODO: support a page/offset type of arg?
    fn algo_listdir(&mut self, ino: u64, offset: u64) -> Result<Vec<FileAttr>, String> {
        // TODO: support offset
        if offset > 0 {
            return Ok(vec![]);
        }

        let local_path = try!(self.paths.get((ino-1) as usize)
                .ok_or(format!("path not found for inode {}", ino))
            ).clone();
        let path = path_to_uri(&local_path);
        println!("Fetching algo dir listing for inode: {} (+{}): {} => {}", ino, offset, local_path, path);

        let my_dir = self.client.dir(&path);
        let inos = my_dir.list().map(|entry_result|
            match entry_result {
                Ok(DirEntry::Dir(d)) => self.insert_dir(&uri_to_path(&d.to_data_uri()), CREATE_TIME),
                Ok(DirEntry::File(f)) => self.insert_file(&uri_to_path(&f.to_data_uri()), Timespec::new(f.last_modified.timestamp(),0), f.size),
                Err(err) => {
                    // TODO: should return Err(...)?
                    println!("Error listing directory: {}", err);
                    0
                },
            }
        ).filter(|ino| *ino != 0).collect::<Vec<_>>();

        // Update children cache so we don't hammer the API
        self.children.insert(ino, inos);
        println!("added to children: {:?}", self.children);
        Ok(self.children[&(ino)].iter().map(|ino| self.inodes[(ino-1) as usize].clone() ).collect())
    }

    fn insert_dir(&mut self, path: &str, mtime: Timespec) -> u64 {
        let ino = self.inodes.len() as u64 + 1;
        self.inodes.push(FileAttr {
            ino: ino,
            size: 0,
            blocks: 0,
            atime: mtime,
            mtime: mtime,
            ctime: mtime,
            crtime: mtime,
            kind: FileType::Directory,
            perm: 0o750,
            nlink: 2,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            flags: 0,
        });
        self.paths.push(path.to_string());
        ino
    }

    fn insert_file(&mut self, path: &str, mtime: Timespec, size: u64) -> u64 {
        let ino = self.inodes.len() as u64 + 1;
        self.inodes.push(FileAttr {
            ino: ino,
            size: size,
            blocks: (size/512) + 1, // TODO: const BLOCKSIZE
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
        });

        self.paths.push(path.to_string());
        ino
    }

}

impl Filesystem for AlgoFs {
    fn lookup (&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        println!("lookup: {} -> {}", parent, name.to_str().unwrap());
        match (parent, name.to_str()) {
            (1, Some("")) => reply.entry(&TTL, &self.inodes[0], 0),
            (1, Some("data")) => reply.entry(&TTL, &self.inodes[1], 0),
            (1, _) => reply.error(ENOENT), // TODO: check if connector exists, and cache that
            (_, Some(name)) => {
                match self.children.get(&parent) {
                    Some(children) => {
                        let ref parent_path = self.paths[(parent-1) as usize];
                        match children.iter().find(|child_ino| self.paths[(*child_ino-1) as usize] == format!("{}/{}", parent_path, name) ) {
                            Some(child_ino) => reply.entry(&TTL, &self.inodes[(child_ino-1) as usize], 0),
                            None => {
                                println!("lookup missing from cache: {} -> {}", parent, name);
                                reply.error(ENOENT);
                            }
                        }
                    }
                    None => {
                        // TODO: get metadata from algorithmia
                        println!("not-cached lookup: {} -> {}", parent, name);
                        reply.error(ENOENT);
                    }
                }
            }
            (_, None) => {
                println!("unnamed lookup for parent: {}", parent);
                reply.error(ENOENT);
            }
        };
    }

    fn getattr (&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.inodes.get((ino-1) as usize) {
            Some(attr) => reply.attr(&TTL, attr),
            None => {
                println!("getattr ENOENT: {}", ino);
                reply.error(ENOENT);
            }
        };
    }

    fn read (&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, _size: u32, reply: ReplyData) {
        println!("read {}", ino);
        if ino == 2 {
            reply.data(&"hello world".as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
        }
    }

    fn readdir (&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, mut reply: ReplyDirectory) {
        match (ino, offset) {
            (1, 0) => {
                reply.add(1, 0, FileType::Directory, ".");
                reply.add(1, 1, FileType::Directory, "..");
                reply.add(2, 2, FileType::Directory, "data");
                reply.ok();
            },
            (1, _) => reply.ok(),
            _ => match self.inodes.len() >= (ino as usize) {
                true => {
                    let children_res = match self.children.contains_key(&ino) {
                        true => self.cache_listdir(ino, offset),
                        false => self.algo_listdir(ino, offset),
                    };

                    match children_res {
                        Ok(children) => {
                            if offset == 0 {
                                reply.add(ino, 0, FileType::Directory, ".");
                                // TODO: fix offset i+2 when '..' parent dir is supported
                                // reply.add(get_parent(...), 1, FileType::Directory, "..");
                            }

                            for (i, child_attr) in children.iter().enumerate() {
                                let ref child_path = self.paths[(child_attr.ino-1) as usize];
                                reply.add(child_attr.ino, (i+1) as u64, child_attr.kind, get_basename(child_path));
                            }
                            reply.ok();
                        }
                        Err(err) => {
                            println!("readdir error: {}", err);
                            reply.error(EINTR);
                        }
                    }

                }
                false => {
                    println!("inode not found: {}", ino);
                    reply.error(ENOENT);
                }
            }
        }
    }
}

fn path_to_uri(path: &str) -> String {
    let parts: Vec<_> = path.splitn(2, "/").collect();
    match parts.len() {
        1 => format!("{}://", parts[0]),
        2 => parts.join("://"),
        _ => unreachable!(),
    }
}

fn uri_to_path(uri: &str) -> String {
    let parts: Vec<_> = uri.splitn(2, "://").collect();
    match parts.len() {
        1 => format!("data/{}", parts[0]),
        2 => parts.join("/"),
        _ => unreachable!(),
    }
}

fn get_basename(path: &str) -> String {
    path.rsplitn(2, "/").next().unwrap().to_string()
}


#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
    }
}
