use std::{
    fs::OpenOptions,
    io::{BufReader, Error as IoError},
    path::PathBuf,
};

use tar::{Archive, Builder};

pub fn archive(dir: &PathBuf, tarball_path: &PathBuf) -> Result<(), IoError> {
    let temp_tarball_file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(tarball_path)?;
    let mut tarball_stream = Builder::new(temp_tarball_file);
    for file in std::fs::read_dir(dir).unwrap() {
        if let Ok(entry) = file {
            tarball_stream.append_path(entry.path())?;
        }
    }
    tarball_stream.finish()
}

pub fn unarchive(src: PathBuf, dest: PathBuf) -> Result<(), IoError> {
    let input = OpenOptions::new().read(true).open(src)?;
    let mut archive = Archive::new(BufReader::new(input));
    archive.unpack(dest)?;
    Ok(())
}
