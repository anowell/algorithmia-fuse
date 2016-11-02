extern crate algorithmia;
extern crate netfuse;
extern crate libc;
extern crate time;
extern crate fuse;

use algorithmia::*;
use algorithmia::data::*;
use netfuse::{Metadata, NetworkFilesystem, LibcError, DirEntry};
use fuse::FileType;
use libc::{EIO, ENOENT, EPERM};
use std::io::Read;
use std::path::{Path, PathBuf};
use time::Timespec;

pub use netfuse::MountOptions;

// 2015-03-12 00:00 PST Algorithmia Launch
pub const DEFAULT_TIME: Timespec = Timespec { sec: 1426147200, nsec: 0 };

macro_rules! eio {
    ($fmt:expr) => {{
        println!($fmt);
        Err(EIO)
    }};
    ($fmt:expr, $($arg:tt)*) => {{
        println!($fmt, $($arg)*);
        Err(EIO)
    }};
}

pub struct AlgoFs {
    client: Algorithmia,
}

impl AlgoFs {
    pub fn mount(options: MountOptions, client: Algorithmia) {
        let adfs = AlgoFs { client: client };
        netfuse::mount(adfs, options);
    }
}

fn build_dir_entry(item: &DataItem) -> DirEntry {
    match item {
        &DataItem::Dir(ref d) => {
            let meta = Metadata {
                size: 0,
                atime: DEFAULT_TIME,
                mtime: DEFAULT_TIME,
                ctime: DEFAULT_TIME,
                crtime: DEFAULT_TIME,
                kind: FileType::Directory,
                // TODO: API should indicate if dir is listable or not
                perm: 0o750,
            };
            DirEntry::new(d.basename().expect("dir has no name"), meta)
        }
        &DataItem::File(ref f) => {
            let mtime = Timespec::new(f.last_modified.timestamp(), 0);
            let meta = Metadata {
                size: f.size,
                atime: mtime,
                mtime: mtime,
                ctime: mtime,
                crtime: mtime,
                kind: FileType::RegularFile,
                perm: 0o640,
            };
            DirEntry::new(f.basename().expect("file has no name"), meta)
        }
    }
}

fn basic_dir_entry(path: &str, perm: u16) -> DirEntry {
    let meta = Metadata {
        size: 0,
        atime: DEFAULT_TIME,
        mtime: DEFAULT_TIME,
        ctime: DEFAULT_TIME,
        crtime: DEFAULT_TIME,
        kind: FileType::Directory,
        perm: perm,
    };
    DirEntry::new(path, meta)
}

impl NetworkFilesystem for AlgoFs {
    fn readdir(&mut self, path: &Path) -> Box<Iterator<Item=Result<DirEntry, LibcError>>> {
        let uri = match path_to_uri(&path) {
            Ok(u) => u,
            Err(_) => {
                // The default root listing
                return Box::new(vec![
                    Ok(basic_dir_entry("/data", 0o550)),
                ].into_iter());
            }
        };

        println!("AFS readdir:  {} -> {}", path.display(), uri);

        let dir = self.client.dir(&uri);
        let iter = dir.list()
                    .map( move |child_res| {
                        match child_res {
                            Ok(data_item) => Ok(build_dir_entry(&data_item)),
                            Err(err) => eio!("AFS readdir error: {}", err),
                        }
                    });

        // Returning an Iteratator Trait Object is a bit inflexible.
        // We can't return iter, because it references `dir` (which does NOT reference self)
        //   so it's lifetime ends with this function.
        // We could add `dir` to self, but may need to be able to track multiple dirs
        //   and dropping them becomes quite complicated
        //   so until the trait can return `impl Iterator<Item=Result<DirEntry, LibCError>>`
        //   we're just gonna kill the laziness by collecting early
        //   and to return an IntoIterator that owns all of it's data.
        let hack = iter.collect::<Vec<_>>().into_iter();
        Box::new(hack)
    }

    fn lookup(&mut self, path: &Path) -> Result<Metadata, LibcError> {
        if valid_connector(&path) {
            let uri = try!(path_to_uri(&path));
            println!("AFS lookup: {} -> {}", path.display(), uri);

            match self.client.data(&uri).into_type() {
                Ok(data_item) => Ok(build_dir_entry(&data_item).metadata),
                Err(algorithmia::Error::NotFound(_)) => Err(ENOENT),
                Err(err) => eio!("AFS lookup error: {}", err),
            }
        } else {
            Err(ENOENT)
        }

    }

    fn read(&mut self, path: &Path, mut buffer: &mut Vec<u8> ) -> Result<usize, LibcError> {
        let uri = try!(path_to_uri(&path));
        println!("AFS read: {} -> {}", path.display(), uri);
        match self.client.file(&uri).get() {
            Ok(mut response) => {
                let bytes = response.read_to_end(&mut buffer).expect("failed to read response bytes");
                Ok(bytes as usize)
            }
            Err(err) => eio!("AFS read error: {}", err),
        }
    }

    fn unlink(&mut self, path: &Path) -> Result<(), LibcError> {
        let uri = try!(path_to_uri(&path));
        println!("AFS unlink: {} -> {}", path.display(), uri);
        match self.client.file(&uri).delete() {
            Ok(_) => Ok(()),
            Err(err) => eio!("AFS unlink error: {}", err),
        }
    }

    fn rmdir(&mut self, path: &Path) -> Result<(), LibcError> {
        let uri = try!(path_to_uri(&path));
        println!("AFS rmdir: {} -> {}", path.display(), uri);
        match self.client.dir(&uri).delete(false) {
            Ok(_) => Ok(()),
            Err(err) => eio!("AFS rmdir error: {}", err),
        }
    }

    fn write(&mut self, path: &Path, data: &[u8]) -> Result<(), LibcError>{
        let uri = try!(path_to_uri(&path));
        println!("AFS write: {} -> {} ({} bytes)", path.display(), uri, data.len());
        match self.client.file(&uri).put(data) {
            Ok(_) => Ok(()),
            Err(err) => eio!("AFS write error: {}", err),
        }
    }

    fn mkdir(&mut self, path: &Path) -> Result<(), LibcError> {
        let uri = try!(path_to_uri(&path));
        println!("algo_mkdir: {} -> {}", path.display(), uri);
        match self.client.dir(&uri).create(DataAcl::default()) {
            Ok(_) => Ok(()),
            Err(err) => eio!("AFS mkdir error: {}", err),
        }
    }
}

pub fn valid_connector(path: &Path) -> bool {
    let mut iter = path.components();
    if path.has_root() {
        let _ = iter.next();
    }

    match iter.next().map(|c| c.as_os_str().to_string_lossy() ) {
        Some(p) => {
            p == "data"
                || p.starts_with("dropbox")
                || p.starts_with("s3")
        }
        _ => false,
    }
}

pub fn path_to_uri(path: &Path) -> Result<String, LibcError> {
    let mut iter = path.components();
    if path.has_root() {
        let _ = iter.next();
    }

    let protocol = match iter.next() {
        Some(p) => p.as_os_str(),
        None => { return Err(EPERM); },
    };
    let uri_path = iter.as_path();
    Ok(format!("{}://{}", protocol.to_string_lossy(), uri_path.to_string_lossy()))
}

pub fn uri_to_path(uri: &str) -> PathBuf {
    uri.splitn(2, "://")
        .fold(Path::new("/").to_owned(), |acc, p| acc.join(Path::new(p)) )
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    #[test]
    fn test_path_to_uri() {
        assert_eq!(&*path_to_uri(Path::new("/data")), "data://");
        assert_eq!(&*path_to_uri(Path::new("/data/foo")), "data://foo");
        assert_eq!(&*path_to_uri(Path::new("/data/foo/bar.txt")), "data://foo/bar.txt");
    }

    #[test]
    fn test_uri_to_path() {
        assert_eq!(&*uri_to_path("data://"), Path::new("/data"));
        assert_eq!(&*uri_to_path("data://foo"), Path::new("/data/foo"));
        assert_eq!(&*uri_to_path("data://foo/bar.txt"), Path::new("/data/foo/bar.txt"));
    }

}
