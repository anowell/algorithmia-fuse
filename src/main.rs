extern crate algorithmia_fuse;
extern crate algorithmia;

use algorithmia::{Algorithmia, Url};
use algorithmia_fuse::*;

use std::env;

fn main() {
    let mountpoint = env::args_os().nth(1).expect("Must specify mountpoint");
    let api_key = env::var("ALGORITHMIA_API_KEY").expect("Must set ALGORITHMIA_API_KEY");

    let client = match env::var("ALGORITHMIA_API") {
        Ok(api_base) => {
            println!("Using alternate API endpoint: {}", &api_base);
            let api_base_url = Url::parse(&api_base).expect("Failed to parse ALGORITHMIA_API as a URL");
            Algorithmia::alt_client(api_base_url, &*api_key)
        }
        _ => Algorithmia::client(&*api_key),
    };

    let options = MountOptions::new(&mountpoint);
    AlgoFs::mount(options, client);
}
