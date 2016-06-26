extern crate algorithmia_fuse;
use algorithmia_fuse::*;

use std::env;

fn main () {
    let mountpoint = env::args_os().nth(1).expect("Must specify mountpoint");
    AlgoFs::mount(&mountpoint);
}