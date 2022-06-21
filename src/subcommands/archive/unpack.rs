mod download_stream;
mod file_stream;
#[cfg(test)]
mod tests;

use std::{io::Error as IoError, path::PathBuf};

use clap::{Arg, ArgMatches, Command};
use log::{error, warn};
use reqwest::Error as ReqwestError;
use thiserror::Error as ThisError;

use super::{tar_utils, zstd_utils::Error as ZstdError};

pub const COMMAND_NAME: &str = "unpack";
const FILE: &str = "file";
const OUTPUT: &str = "output";
const URL: &str = "url";

#[derive(Debug, ThisError)]
pub enum Error {
    #[error("Error creating destination archive file: {0}")]
    Destination(IoError),
    #[error("HTTP request error: {0}")]
    Request(#[from] ReqwestError),
    #[error("Error creating tokio runtime: {0}")]
    Runtime(IoError),
    #[error("Error reading source archive file: {0}")]
    Source(IoError),
    #[error("Error streaming from zstd decoder to destination file: {0}")]
    Streaming(IoError),
    #[error("Error unpacking tarball: {0}")]
    Tar(IoError),
    #[error("Zstd error: {0}")]
    ZstdDecoderSetup(#[from] ZstdError),
}

enum DisplayOrder {
    Url,
    File,
    Output,
}

enum Input {
    File(PathBuf),
    Url(String),
}

fn unpack(input: Input, dest: PathBuf) -> Result<(), Error> {
    let dest_archive_path = dest.as_path().join("casper_db_archive.tar.zst");
    match input {
        Input::Url(url) => {
            download_stream::download_archive(&url, dest_archive_path.clone())?;
        }
        Input::File(path) => {
            file_stream::stream_file_archive(path, dest_archive_path.clone())?;
        }
    }
    tar_utils::unarchive(dest_archive_path.clone(), dest).map_err(Error::Tar)?;
    if let Err(io_err) = std::fs::remove_file(dest_archive_path.clone()) {
        warn!(
            "Couldn't remove tarball at {} after unpacking: {}",
            dest_archive_path.as_os_str().to_string_lossy(),
            io_err
        );
    }

    Ok(())
}

pub fn command(display_order: usize) -> Command<'static> {
    Command::new(COMMAND_NAME)
        .display_order(display_order)
        .about("Downloads and decompresses a ZSTD TAR archive of a casper-node storage instance.")
        .arg(
            Arg::new(URL)
                .display_order(DisplayOrder::Url as usize)
                .required_unless_present(FILE)
                .short('u')
                .long(URL)
                .takes_value(true)
                .value_name("URL")
                .help("URL of the compressed archive."),
        )
        .arg(
            Arg::new(FILE)
                .display_order(DisplayOrder::File as usize)
                .required(true)
                .short('f')
                .long(FILE)
                .takes_value(true)
                .value_name("FILE_PATH")
                .required_unless_present(URL)
                .conflicts_with(URL)
                .help("Path to the compressed archive."),
        )
        .arg(
            Arg::new(OUTPUT)
                .display_order(DisplayOrder::Output as usize)
                .required(true)
                .short('o')
                .long(OUTPUT)
                .takes_value(true)
                .value_name("FILE_PATH")
                .help("Output file path for the decompressed TAR archive."),
        )
}

pub fn run(matches: &ArgMatches) -> bool {
    let input = matches
        .value_of(URL)
        .map(|url| Input::Url(url.to_string()))
        .unwrap_or_else(|| {
            matches
                .value_of(FILE)
                .map(|path| Input::File(path.into()))
                .unwrap_or_else(|| panic!("Should have one of {} or {}", FILE, URL))
        });
    let dest = matches.value_of(OUTPUT).unwrap();
    let result = unpack(input, dest.into());

    if let Err(error) = &result {
        error!("Archive unpack failed. {}", error);
    }

    result.is_ok()
}
