use std::{env, fs::File, path::Path, process};

use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    schema::printer::{print_parquet_metadata},
};

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        println!("Usage: parquet-rs-test <file-path>");
        process::exit(1);
    }
    let path = Path::new(&args[1]);
    let file = match File::open(&path) {
        Err(e) => panic!("Error when opening file {}: {}", path.display(), e),
        Ok(f) => f,
    };
    match SerializedFileReader::new(file) {
        Err(e) => panic!("Error when parsing Parquet file: {}", e),
        Ok(parquet_reader) => {
            let metadata = parquet_reader.metadata();
            println!("Metadata for file: {}", &args[1]);
            println!("");
            print_parquet_metadata(&mut std::io::stdout(), &metadata);
        }
    }
}
