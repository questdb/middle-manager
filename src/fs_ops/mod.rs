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

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    // ── move_entry tests ────────────────────────────────────────────

    #[test]
    fn move_entry_same_fs() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("source.txt");
        let dest_dir = tmp.path().join("dest");
        fs::create_dir_all(&dest_dir).unwrap();
        fs::write(&src, "hello").unwrap();

        move_entry(&src, &dest_dir).unwrap();

        assert!(!src.exists(), "source should no longer exist after move");
        let moved = dest_dir.join("source.txt");
        assert!(moved.exists(), "file should exist in destination");
        assert_eq!(fs::read_to_string(&moved).unwrap(), "hello");
    }

    #[test]
    fn move_entry_nonexistent_source_returns_error() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("does_not_exist.txt");
        let dest_dir = tmp.path().join("dest");
        fs::create_dir_all(&dest_dir).unwrap();

        let result = move_entry(&src, &dest_dir);
        assert!(result.is_err(), "moving a non-existent file should return an error");
    }

    #[test]
    fn move_entry_directory() {
        let tmp = tempfile::tempdir().unwrap();
        let src_dir = tmp.path().join("mydir");
        fs::create_dir_all(&src_dir).unwrap();
        fs::write(src_dir.join("inner.txt"), "content").unwrap();

        let dest_dir = tmp.path().join("dest");
        fs::create_dir_all(&dest_dir).unwrap();

        move_entry(&src_dir, &dest_dir).unwrap();

        assert!(!src_dir.exists());
        let moved = dest_dir.join("mydir").join("inner.txt");
        assert!(moved.exists());
        assert_eq!(fs::read_to_string(&moved).unwrap(), "content");
    }

    // ── copy_dir_recursive tests ────────────────────────────────────

    #[test]
    fn copy_dir_with_regular_file() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("file.txt"), "data").unwrap();

        copy_dir_recursive(&src, &dst).unwrap();

        let copied = dst.join("file.txt");
        assert!(copied.exists());
        assert_eq!(fs::read_to_string(&copied).unwrap(), "data");
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_with_symlink_to_file() {
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        fs::write(src.join("real.txt"), "symdata").unwrap();
        unix_fs::symlink("real.txt", src.join("link.txt")).unwrap();

        copy_dir_recursive(&src, &dst).unwrap();

        let link_path = dst.join("link.txt");
        let meta = link_path.symlink_metadata().unwrap();
        assert!(
            meta.file_type().is_symlink(),
            "copied entry should still be a symlink"
        );
        let target = fs::read_link(&link_path).unwrap();
        assert_eq!(target, Path::new("real.txt"));
    }

    #[cfg(unix)]
    #[test]
    fn copy_dir_with_symlink_loop_does_not_stackoverflow() {
        // Create a directory with a symlink pointing to its parent (a loop).
        let tmp = tempfile::tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        fs::create_dir_all(&src).unwrap();
        // Symlink pointing to parent directory — following it would loop forever.
        unix_fs::symlink("..", src.join("loop_link")).unwrap();

        // This must NOT stack-overflow; it should just recreate the symlink.
        copy_dir_recursive(&src, &dst).unwrap();

        let link_path = dst.join("loop_link");
        let meta = link_path.symlink_metadata().unwrap();
        assert!(
            meta.file_type().is_symlink(),
            "loop symlink should be copied as a symlink"
        );
        let target = fs::read_link(&link_path).unwrap();
        assert_eq!(target, Path::new(".."));
    }
}
