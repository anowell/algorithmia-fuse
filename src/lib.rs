extern crate algorithmia;
extern crate fuse;
extern crate libc;
extern crate time;
extern crate sequence_trie;

use algorithmia::*;
use algorithmia::data::*;
use fuse::{FileType, FileAttr, Filesystem, Request, ReplyData, ReplyEntry, ReplyAttr, ReplyDirectory};
use libc::ENOENT;
use sequence_trie::SequenceTrie;
use std::collections::HashMap;
use std::env;
use std::path::Path;
use time::Timespec;

// 2015-03-12 00:00 PST Algorithmia Launch
const DEFAULT_TIME: Timespec = Timespec { sec: 1426147200, nsec: 0 };
const TTL: Timespec = Timespec { sec: 1, nsec: 0 };

#[derive(Debug, Clone)]
struct TrieNode {
    ino: u64,
    visited: bool
}
impl TrieNode {
    fn new(ino: u64) -> TrieNode {
        TrieNode {
            ino: ino,
            visited: false,
        }
    }
}

pub struct AlgoFs {
    // indexed by inode-1
    inodes: Vec<FileAttr>,
    // indexed by inode-1
    paths: Vec<String>,
    /// map of inodes to to data buffers - indexed by inode (NOT inode-1)
    _cache: HashMap<u64, Vec<u8>>,
    /// trie mapping path segments (e.g. (["data", "foo", "bar.txt"]`) to inode values
    fs_trie: SequenceTrie<String, TrieNode>,
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
            atime: DEFAULT_TIME,
            mtime: DEFAULT_TIME,
            ctime: DEFAULT_TIME,
            crtime: DEFAULT_TIME,
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
            atime: DEFAULT_TIME,
            mtime: DEFAULT_TIME,
            ctime: DEFAULT_TIME,
            crtime: DEFAULT_TIME,
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
        let mut fs_trie = SequenceTrie::new();
        inodes.push(adfs_root);
        inodes.push(data_root);
        paths.push("".into());
        paths.push("data".into());
        fs_trie.insert(&path_to_prefix("data"), TrieNode::new(2));

        let adfs = AlgoFs {
            client: client,
            inodes: inodes,
            paths: paths,
            _cache: HashMap::new(),
            fs_trie: fs_trie,
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

        let ref dir_path = self.paths[(ino-1) as usize];
        let dir_key = path_to_prefix(dir_path);
        let node = try!(self.fs_trie.get_node(&dir_key).ok_or("Cache miss - should not have called `cache_listdir`".to_string()));

        let attrs = node.children.values()
            .filter_map(|ref c| c.value.clone().map(|n| n.ino) )
            .map(|c_ino| self.inodes[(c_ino - 1) as usize].clone())
            .collect::<Vec<FileAttr>>();

        Ok(attrs)
    }

    // TODO: support a page/offset type of arg?
    fn algo_listdir(&mut self, ino: u64, offset: u64) -> Result<Vec<FileAttr>, String> {
        // TODO: support offset
        if offset > 0 {
            return Ok(vec![]);
        }

        let local_path = try!(self.paths
                .get((ino - 1) as usize)
                .ok_or(format!("path not found for inode {}", ino)))
            .clone();
        let path = path_to_uri(&local_path);
        println!("Fetching algo dir listing for inode: {} (+{}): {} => {}",
                 ino,
                 offset,
                 local_path,
                 path);

        let my_dir = self.client.dir(&path);
        let inos = my_dir.list()
            .map(|entry_result| {
                match entry_result {
                    Ok(DirEntry::Dir(d)) => self.insert_dir(&uri_to_path(&d.to_data_uri()), DEFAULT_TIME),
                    Ok(DirEntry::File(f)) => self.insert_file(&uri_to_path(&f.to_data_uri()),
                                                              Timespec::new(f.last_modified.timestamp(), 0),
                                                              f.size),
                    Err(err) => {
                        // TODO: should return Err(...)?
                        println!("Error listing directory: {}", err);
                        0
                    }
                }
            })
            .filter(|ino| *ino != 0)
            .collect::<Vec<_>>();

        {
            // Mark this node visited
            let dir_prefix = path_to_prefix(&self.paths[(ino-1) as usize]);
            let mut dir_node = self.fs_trie.get_mut(&dir_prefix).expect("node missing for dir just listed");
            dir_node.visited = true;
        }

        Ok(inos.iter().map(|ino| self.inodes[(ino - 1) as usize].clone()).collect())
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
        self.fs_trie.insert(&path_to_prefix(path), TrieNode::new(ino));
        ino
    }

    fn insert_file(&mut self, path: &str, mtime: Timespec, size: u64) -> u64 {
        let ino = self.inodes.len() as u64 + 1;
        self.inodes.push(FileAttr {
            ino: ino,
            size: size,
            blocks: (size / 512) + 1, // TODO: const BLOCKSIZE
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
        self.fs_trie.insert(&path_to_prefix(path), TrieNode::new(ino));
        ino
    }
}

impl Filesystem for AlgoFs {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &Path, reply: ReplyEntry) {
        let name = name.to_string_lossy();
        match (parent, name.as_ref()) {
            (1, "") => reply.entry(&TTL, &self.inodes[0], 0),
            (1, "data") => reply.entry(&TTL, &self.inodes[1], 0),
            (1, _) => reply.error(ENOENT), // TODO: check if connector exists, and cache that
            _ => {
                let ref parent_path = self.paths[(parent - 1) as usize];
                let child_path = format!("{}/{}", parent_path, name);
                let child_segment = path_to_prefix(&child_path);
                match self.fs_trie.get(&child_segment) {
                    Some(child) => reply.entry(&TTL, &self.inodes[(child.ino - 1) as usize], 0),
                    None => {
                        // TODO: get metadata from algorithmia
                        // let parent_segment = path_to_prefix(parent_path);
                        println!("not-cached lookup: {} -> {}", parent, name);
                        reply.error(ENOENT);
                    }
                }
            }
        };
    }

    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        match self.inodes.get((ino - 1) as usize) {
            Some(attr) => reply.attr(&TTL, attr),
            None => {
                println!("getattr ENOENT: {}", ino);
                reply.error(ENOENT);
            }
        };
    }

    fn read(&mut self, _req: &Request, ino: u64, _fh: u64, offset: u64, _size: u32, reply: ReplyData) {
        println!("read {}", ino);
        if ino == 2 {
            reply.data(&"hello world".as_bytes()[offset as usize..]);
        } else {
            reply.error(ENOENT);
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
            _ => match self.inodes.len() >= (ino as usize) {
                true => {
                    let dir_prefix = path_to_prefix(&self.paths[(ino-1) as usize]);
                    let dir_visited  = self.fs_trie.get(&dir_prefix).map(|n| n.visited).unwrap_or(false);
                    let children_res  = match dir_visited {
                        true => self.cache_listdir(ino, offset),
                        false => self.algo_listdir(ino, offset),
                    };

                    let parent_ino = self.fs_trie.get_ancestor(&dir_prefix)
                                         .expect("TODO: insert parent inode if not previously known")
                                         .ino
                                         .clone();

                    match children_res {
                        Ok(children) => {
                            if offset == 0 {
                                reply.add(ino, 0, FileType::Directory, ".");
                                reply.add(parent_ino, 1, FileType::Directory, "..");
                            }

                            for (i, child_attr) in children.iter().enumerate() {
                                let ref child_path = self.paths[(child_attr.ino - 1) as usize];
                                reply.add(child_attr.ino,
                                          (i + 2) as u64,
                                          child_attr.kind,
                                          get_basename(child_path));
                            }
                            reply.ok();
                        }
                        Err(err) => {
                            println!("readdir error: {}", err);
                            reply.error(ENOENT);
                        }
                    }

                }
                false => {
                    println!("inode not found: {}", ino);
                    reply.error(ENOENT);
                }
            },
        }
    }
}

pub fn path_to_uri(path: &str) -> String {
    let parts: Vec<_> = path.splitn(2, "/").collect();
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


pub fn path_to_prefix(path: &str) -> Vec<String> {
    path.split_terminator("/").map(String::from).collect()
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

    fn assert_segment(actual: PathSegments, expected: Vec<&'static str>) {
        assert_eq!(actual.0.iter().map(String::as_ref).collect::<Vec<&str>>(), expected);
    }

    #[test]
    fn test_path_to_segments() {
        assert_segment(path_to_segments("data"), vec!["data"]);
        assert_segment(path_to_segments("data/foo"), vec!["data", "foo"]);
        assert_segment(path_to_segments("data/foo/bar.txt"), vec!["data", "foo", "bar.txt"]);
    }

}
