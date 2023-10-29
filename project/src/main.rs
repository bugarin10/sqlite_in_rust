

use project::{create_db, fill_data, use_query};
use clap::Parser;
use std::time::Instant;

#[derive(Parser)]
#[clap(
    version = "1.0",
    author = "Rafael Davila Bugarin",
    about = "Query to Execute"
)]

struct Opts {
    #[clap(long)]
    query: String,
}

fn main() {
    let opts: Opts = Opts::parse();

    let query = opts.query;

    let now = Instant::now();
    {
        let _ = create_db();
        let _ = fill_data();
        let _ = use_query(query);
    }

    let elapsed = now.elapsed();
    println!("Time took: {:.2?}", elapsed);
}