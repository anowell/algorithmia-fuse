use fuse::{FileType, FileAttr};
use sequence_trie::SequenceTrie;
use std::collections::HashMap;
use std::ops::{Index, IndexMut};
use super::DEFAULT_TIME;

#[derive(Debug, Clone)]
pub struct Inode {
    pub path: String,
    pub attr: FileAttr,
    pub visited: bool,
}

impl Inode {
    pub fn new(path: &str, attr: FileAttr) -> Inode {
        Inode {
            path: path.into(),
            attr: attr,
            visited: false,
        }
    }
}

pub struct InodeStore {
    inode_map: HashMap<u64, Inode>,
    ino_trie: SequenceTrie<String, u64>,
}

impl InodeStore {
    pub fn new(perm: u16, uid: u32, gid: u32) -> InodeStore {
        let mut store = InodeStore {
            inode_map: HashMap::new(),
            ino_trie: SequenceTrie::new(),
        };

        let fs_root = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: DEFAULT_TIME,
            mtime: DEFAULT_TIME,
            ctime: DEFAULT_TIME,
            crtime: DEFAULT_TIME,
            kind: FileType::Directory,
            perm: perm,
            nlink: 2,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
        };

        store.insert(Inode::new("", fs_root));
        store
    }

    pub fn len(&self) -> usize {
        self.inode_map.len()
    }

    pub fn get(&self, ino: u64) -> Option<&Inode> {
        self.inode_map.get(&ino)
    }

    pub fn get_by_path(&self, path: &str) -> Option<&Inode> {
        let sequence = path_to_sequence(path);
        self.ino_trie.get(&sequence).and_then(|ino| self.get(*ino))
    }

    pub fn child(&self, ino: u64, name: &str) -> Option<&Inode> {
        self.get(ino)
            .and_then(|inode| {
                let mut sequence = path_to_sequence(&inode.path);
                sequence.push(name.into());
                self.ino_trie.get(&sequence).and_then(|ino| self.get(*ino) )
            })
    }

    pub fn children(&self, ino: u64) -> Vec<&Inode> {
        match self.get(ino) {
            Some(inode) => {
                let sequence = path_to_sequence(&inode.path);
                let node = self.ino_trie.get_node(&sequence)
                    .expect("inconsistent fs - failed to lookup by path after lookup by ino");
                node.children
                    .values()
                    .filter_map(|ref c| c.value.as_ref() )
                    .map(|ino| self.get(*ino).expect("inconsistent fs - found child without inode") )
                    .collect()
            }
            None => vec![],
        }
    }

    // All inodes have a parent (root parent is root)
    // Return value of None means the ino wasn't found
    pub fn parent(&self, ino: u64) -> Option<&Inode> {
        // parent of root is root
        if ino == 1 {
            return self.get(1);
        }

        self.get(ino)
            .and_then(|inode| {
                let sequence = path_to_sequence(&inode.path);
                match sequence.len() {
                    1 => self.get(1),
                    len => self.ino_trie.get(&sequence[0..(len-1)]).and_then(|p_ino| self.get(*p_ino) )
                }
            })
    }

    pub fn get_mut(&mut self, ino: u64) -> Option<&mut Inode> {
        self.inode_map.get_mut(&ino)
    }

    pub fn get_mut_by_path(&mut self, path: &str) -> Option<&mut Inode> {
        let sequence = path_to_sequence(path);
        self.ino_trie.get(&sequence).cloned()
            .and_then(move |ino| self.get_mut(ino))
    }

    // pub fn get_mut_parent(&mut self, ino: u64) -> &mut Inode {
    //     unimplemented!();
    // }

    // pub fn get_mut_child(&mut self, ino: u64, path: &str) -> &mut Inode {
    //     unimplemented!();
    // }

    pub fn insert(&mut self, inode: Inode) {
        let ino = inode.attr.ino;
        let sequence = path_to_sequence(&inode.path);

        // CONSIDER: we could just block inserts that would cause inconsistent store
        // assert_eq!(
        //     self.inode_map.get(&ino).map(|i| i.attr.ino),
        //     self.ino_trie.get(&sequence)
        // );

        let new_map_insert = self.inode_map.insert(ino, inode).is_none();
        let new_trie_insert = self.ino_trie.insert(&sequence, ino);

        if new_map_insert != new_trie_insert {
            panic!("inconsistent inode store after inserting {} ({})", ino, self[ino].path);
        }
    }
}

impl Index<u64> for InodeStore {
    type Output = Inode;

    fn index<'a>(&'a self, index: u64) -> &'a Inode {
        self.get(index).unwrap()
    }
}

impl IndexMut<u64> for InodeStore {
    fn index_mut<'a>(&'a mut self, index: u64) -> &'a mut Inode {
        self.get_mut(index).unwrap()
    }
}

fn path_to_sequence(path: &str) -> Vec<String> {
    path.split_terminator("/").map(String::from).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::*;
    use fuse::{FileType, FileAttr};

    fn new_dir_attr(ino: u64) -> FileAttr {
        FileAttr {
            ino: ino,
            size: 0,
            blocks: 0,
            atime: DEFAULT_TIME,
            mtime: DEFAULT_TIME,
            ctime: DEFAULT_TIME,
            crtime: DEFAULT_TIME,
            kind: FileType::Directory,
            perm: 0o750,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
        }
    }

    fn new_file_attr(ino: u64) -> FileAttr {
        FileAttr {
            ino: ino,
            size: 42,
            blocks: 0,
            atime: DEFAULT_TIME,
            mtime: DEFAULT_TIME,
            ctime: DEFAULT_TIME,
            crtime: DEFAULT_TIME,
            kind: FileType::Directory,
            perm: 0o640,
            nlink: 2,
            uid: 1000,
            gid: 1000,
            rdev: 0,
            flags: 0,
        }
    }

    fn build_basic_store() -> InodeStore {
        let mut store = InodeStore::new(0o750, 1000, 1000);
        store.insert(Inode::new("data", new_dir_attr(2)));
        store.insert(Inode::new("data/foo.txt", new_file_attr(3)));
        store.insert(Inode::new("data/bar.txt", new_file_attr(4)));
        store
    }

    #[test]
    fn test_inode_store_get() {
        let store = build_basic_store();
        assert_eq!(&store.get(1).unwrap().path, "");
        assert_eq!(&store.get(2).unwrap().path, "data");
        assert_eq!(&store.get(3).unwrap().path, "data/foo.txt");
    }

    #[test]
    fn test_inode_store_get_by_path() {
        let store = build_basic_store();
        assert_eq!(store.get_by_path("").unwrap().attr.ino, 1);
        assert_eq!(store.get_by_path("data").unwrap().attr.ino, 2);
        assert_eq!(store.get_by_path("data/foo.txt").unwrap().attr.ino, 3);
        assert_eq!(store.get_by_path("data/bar.txt").unwrap().attr.ino, 4);
    }

    #[test]
    fn test_inode_store_get_mut() {
        let mut store = build_basic_store();
        {
            let mut inode = store.get_mut(3).unwrap();
            assert_eq!(inode.attr.size, 42);
            inode.attr.size = 23;
        }
        assert_eq!(store.get(3).unwrap().attr.size, 23);
    }

    #[test]
    fn test_inode_store_get_mut_by_path() {
        let mut store = build_basic_store();
        {
            let mut inode = store.get_mut_by_path("data/foo.txt").unwrap();
            assert_eq!(inode.attr.size, 42);
            inode.attr.size = 23;
        }
        assert_eq!(store.get_by_path("data/foo.txt").unwrap().attr.size, 23);
    }

    #[test]
    fn test_inode_store_parent() {
        let store = build_basic_store();
        assert_eq!(&store.parent(3).unwrap().path, "data");
        assert_eq!(store.parent(2).unwrap().attr.ino, 1);
        assert_eq!(store.parent(1).unwrap().attr.ino, 1);
        assert!(&store.parent(999).is_none());
    }

    #[test]
    fn test_inode_store_children() {
        let store = build_basic_store();
        assert_eq!(store.children(1).len(), 1);
        assert_eq!(store.children(2).len(), 2);
        assert_eq!(store.children(3).len(), 0);
    }

    #[test]
    fn test_inode_store_child() {
        let store = build_basic_store();
        assert_eq!(store.child(2, "foo.txt").unwrap().path, "data/foo.txt");
        assert!(store.child(2, "notfound").is_none());
    }

}