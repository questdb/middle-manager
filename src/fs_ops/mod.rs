pub mod archive;

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};

#[cfg(unix)]
use std::os::unix::fs as unix_fs;

pub fn copy_entry(source: &Path, dest_dir: &Path) -> Result<()> {
    let file_name = source.file_name().context("source has no file name")?;
    let dest = dest_dir.join(file_name);

    if source.is_dir() {
        copy_dir_recursive(source, &dest)?;
    } else {
        fs::copy(source, &dest)
            .with_context(|| format!("Failed to copy {:?} to {:?}", source, dest))?;
    }
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst).with_context(|| format!("Failed to create directory {:?}", dst))?;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        let meta = src_path
            .symlink_metadata()
            .with_context(|| format!("Failed to read metadata for {:?}", src_path))?;

        if meta.file_type().is_symlink() {
            // Replicate the symlink rather than following it (prevents symlink loops).
            let target = fs::read_link(&src_path)
                .with_context(|| format!("Failed to read symlink {:?}", src_path))?;
            #[cfg(unix)]
            unix_fs::symlink(&target, &dst_path)
                .with_context(|| format!("Failed to create symlink {:?}", dst_path))?;
        } else if meta.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;
        }
    }
    Ok(())
}

pub fn move_entry(source: &Path, dest_dir: &Path) -> Result<()> {
    let file_name = source.file_name().context("source has no file name")?;
    let dest = dest_dir.join(file_name);

    // Try rename first (fast, same-filesystem move)
    match fs::rename(source, &dest) {
        Ok(()) => Ok(()),
        Err(err) if err.raw_os_error() == Some(18) => {
            // EXDEV (errno 18): cross-device link — fall back to copy + delete
            copy_entry(source, dest_dir)?;
            delete_entry(source)?;
            Ok(())
        }
        Err(err) => Err(err)
            .with_context(|| format!("Failed to move {:?} to {:?}", source, dest))?,
    }
}

pub fn delete_entry(path: &Path) -> Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)
            .with_context(|| format!("Failed to delete directory {:?}", path))?;
    } else {
        fs::remove_file(path).with_context(|| format!("Failed to delete file {:?}", path))?;
    }
    Ok(())
}

#[allow(dead_code)]
pub fn create_directory(parent: &Path, name: &str) -> Result<()> {
    let path = parent.join(name);
    fs::create_dir_all(&path).with_context(|| format!("Failed to create directory {:?}", path))?;
    Ok(())
}

#[allow(dead_code)]
pub fn rename_entry(path: &Path, new_name: &str) -> Result<()> {
    let parent = path.parent().context("path has no parent")?;
    let new_path = parent.join(new_name);
    fs::rename(path, &new_path)
        .with_context(|| format!("Failed to rename {:?} to {:?}", path, new_path))?;
    Ok(())
}
