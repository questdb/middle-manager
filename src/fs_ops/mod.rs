pub mod archive;

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// How to handle destination files that already exist.
#[derive(Clone, Copy, PartialEq)]
pub enum ConflictPolicy {
    Overwrite,
    Skip,
    Rename,
    Append,
}

/// How to handle symlinks during copy.
#[derive(Clone, Copy, PartialEq)]
pub enum SymlinkCopyMode {
    /// Follow top-level symlink dirs, but preserve symlinks inside directories.
    Smart,
    /// Always dereference: copy the content the symlink points to.
    Follow,
    /// Always preserve: recreate the symlink pointing to the same target.
    Preserve,
}

#[derive(Clone)]
pub struct CopyOptions {
    pub sparse: bool,
    pub conflict: ConflictPolicy,
    pub copy_permissions: bool,
    pub copy_xattrs: bool,
    pub disable_write_cache: bool,
    pub use_cow: bool,
    pub symlink_mode: SymlinkCopyMode,
}

/// A single file-level copy operation (directories become mkdir items).
#[derive(Clone)]
pub struct CopyItem {
    pub src: PathBuf,
    pub dst: PathBuf,
    pub is_dir: bool,
    pub is_symlink: bool,
}

// ---------------------------------------------------------------------------
// Planning: flatten a source entry into individual copy items
// ---------------------------------------------------------------------------

/// Flatten a source entry into individual copy items.
pub fn plan_copy(
    source: &Path,
    dest_dir: &Path,
    symlink_mode: SymlinkCopyMode,
) -> Result<Vec<CopyItem>> {
    let file_name = source.file_name().context("source has no file name")?;
    let dest = dest_dir.join(file_name);
    let mut items = Vec::new();
    plan_entry(source, &dest, symlink_mode, true, &mut items)?;
    Ok(items)
}

fn plan_entry(
    src: &Path,
    dst: &Path,
    symlink_mode: SymlinkCopyMode,
    top_level: bool,
    items: &mut Vec<CopyItem>,
) -> Result<()> {
    let meta = src.symlink_metadata()?;
    let is_symlink = meta.is_symlink();

    if is_symlink {
        let should_preserve = match symlink_mode {
            SymlinkCopyMode::Preserve => true,
            SymlinkCopyMode::Follow => false,
            SymlinkCopyMode::Smart => !top_level,
        };

        if should_preserve {
            items.push(CopyItem {
                src: src.to_path_buf(),
                dst: dst.to_path_buf(),
                is_dir: false,
                is_symlink: true,
            });
            return Ok(());
        }
        // Follow the symlink — use the resolved metadata below
    }

    // Use metadata that follows symlinks for the actual type check
    let real_meta = fs::metadata(src)?;
    if real_meta.is_dir() {
        items.push(CopyItem {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            is_dir: true,
            is_symlink: false,
        });
        for entry in fs::read_dir(src)? {
            let entry = entry?;
            let src_path = entry.path();
            let dst_path = dst.join(entry.file_name());
            plan_entry(&src_path, &dst_path, symlink_mode, false, items)?;
        }
    } else {
        items.push(CopyItem {
            src: src.to_path_buf(),
            dst: dst.to_path_buf(),
            is_dir: false,
            is_symlink: false,
        });
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Execution
// ---------------------------------------------------------------------------

/// Execute a single planned copy item.
pub fn exec_copy_item(item: &CopyItem, opts: &CopyOptions) -> Result<()> {
    if item.is_symlink {
        copy_symlink(&item.src, &item.dst)?;
        return Ok(());
    }
    if item.is_dir {
        fs::create_dir_all(&item.dst)
            .with_context(|| format!("Failed to create directory {:?}", item.dst))?;
        if opts.copy_permissions {
            copy_permissions(&item.src, &item.dst);
        }
        if opts.copy_xattrs {
            copy_xattrs(&item.src, &item.dst);
        }
        return Ok(());
    }
    copy_single_file(&item.src, &item.dst, opts)
}

/// Copy a full entry (file or directory tree) respecting all options.
pub fn copy_entry(source: &Path, dest_dir: &Path, opts: &CopyOptions) -> Result<()> {
    let file_name = source.file_name().context("source has no file name")?;
    let dest = dest_dir.join(file_name);
    let meta = source.symlink_metadata()?;

    if meta.is_symlink() {
        let should_preserve = match opts.symlink_mode {
            SymlinkCopyMode::Preserve => true,
            SymlinkCopyMode::Follow => false,
            SymlinkCopyMode::Smart => false, // top-level: follow
        };
        if should_preserve {
            return copy_symlink(source, &dest);
        }
    }

    if source.is_dir() {
        copy_dir_recursive(source, &dest, opts)?;
    } else {
        copy_single_file(source, &dest, opts)
            .with_context(|| format!("Failed to copy {:?} to {:?}", source, dest))?;
    }
    Ok(())
}

fn copy_dir_recursive(src: &Path, dst: &Path, opts: &CopyOptions) -> Result<()> {
    fs::create_dir_all(dst).with_context(|| format!("Failed to create directory {:?}", dst))?;
    if opts.copy_permissions {
        copy_permissions(src, dst);
    }
    if opts.copy_xattrs {
        copy_xattrs(src, dst);
    }

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());
        let meta = src_path.symlink_metadata()?;

        if meta.is_symlink() {
            let should_preserve = match opts.symlink_mode {
                SymlinkCopyMode::Preserve => true,
                SymlinkCopyMode::Follow => false,
                SymlinkCopyMode::Smart => true, // inside dir: preserve
            };
            if should_preserve {
                copy_symlink(&src_path, &dst_path)?;
                continue;
            }
        }

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path, opts)?;
        } else {
            copy_single_file(&src_path, &dst_path, opts)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Single file copy with all options
// ---------------------------------------------------------------------------

fn copy_single_file(src: &Path, dst: &Path, opts: &CopyOptions) -> Result<()> {
    let resolved = match resolve_dest(dst, opts.conflict) {
        Some(p) => p,
        None => return Ok(()), // Skip
    };

    if opts.conflict == ConflictPolicy::Append && resolved.exists() {
        append_file(src, &resolved)?;
    } else if opts.use_cow {
        if !try_cow_copy(src, &resolved)? {
            // CoW not supported, fall back
            do_copy(src, &resolved, opts)?;
        }
    } else {
        do_copy(src, &resolved, opts)?;
    }

    if opts.copy_permissions {
        copy_permissions(src, &resolved);
    }
    if opts.copy_xattrs {
        copy_xattrs(src, &resolved);
    }
    Ok(())
}

/// Perform the actual data copy (sparse or regular, with optional write cache control).
fn do_copy(src: &Path, dst: &Path, opts: &CopyOptions) -> Result<()> {
    if opts.sparse {
        sparse_copy::copy_file_sparse(src, dst, opts.disable_write_cache)
    } else if opts.disable_write_cache {
        manual_copy(src, dst, true)
    } else {
        fs::copy(src, dst)?;
        Ok(())
    }
}

/// Manual copy with optional write cache bypass.
fn manual_copy(src: &Path, dst: &Path, disable_cache: bool) -> Result<()> {
    let mut src_file = fs::File::open(src).with_context(|| format!("Failed to open {:?}", src))?;
    let mut dst_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)
        .with_context(|| format!("Failed to create {:?}", dst))?;

    if disable_cache {
        set_nocache(&dst_file);
    }

    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = src_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst_file.write_all(&buf[..n])?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Conflict resolution
// ---------------------------------------------------------------------------

fn resolve_dest(dest: &Path, policy: ConflictPolicy) -> Option<PathBuf> {
    if !dest.exists() {
        return Some(dest.to_path_buf());
    }
    match policy {
        ConflictPolicy::Overwrite | ConflictPolicy::Append => Some(dest.to_path_buf()),
        ConflictPolicy::Skip => None,
        ConflictPolicy::Rename => Some(unique_name(dest)),
    }
}

fn unique_name(path: &Path) -> PathBuf {
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    let ext = path
        .extension()
        .map(|e| format!(".{}", e.to_string_lossy()))
        .unwrap_or_default();
    let parent = path.parent().unwrap_or(Path::new("."));

    for i in 1.. {
        let candidate = parent.join(format!("{}({}){}", stem, i, ext));
        if !candidate.exists() {
            return candidate;
        }
    }
    unreachable!()
}

// ---------------------------------------------------------------------------
// Append
// ---------------------------------------------------------------------------

fn append_file(src: &Path, dst: &Path) -> Result<()> {
    let mut src_file = fs::File::open(src).with_context(|| format!("Failed to open {:?}", src))?;
    let mut dst_file = fs::OpenOptions::new()
        .append(true)
        .open(dst)
        .with_context(|| format!("Failed to open {:?} for append", dst))?;

    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = src_file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        dst_file.write_all(&buf[..n])?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Symlink
// ---------------------------------------------------------------------------

fn copy_symlink(src: &Path, dst: &Path) -> Result<()> {
    let target = fs::read_link(src).with_context(|| format!("Failed to read symlink {:?}", src))?;
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&target, dst)
            .with_context(|| format!("Failed to create symlink {:?} -> {:?}", dst, target))?;
    }
    #[cfg(not(unix))]
    {
        // Fallback: copy the target content
        if target.is_dir() {
            fs::create_dir_all(dst)?;
        } else {
            fs::copy(src, dst)?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Permissions
// ---------------------------------------------------------------------------

fn copy_permissions(src: &Path, dst: &Path) {
    if let Ok(meta) = fs::metadata(src) {
        let _ = fs::set_permissions(dst, meta.permissions());
    }
}

// ---------------------------------------------------------------------------
// Extended attributes
// ---------------------------------------------------------------------------

fn copy_xattrs(src: &Path, dst: &Path) {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        if let Ok(attrs) = xattr::list(src) {
            for attr in attrs {
                if let Ok(Some(value)) = xattr::get(src, &attr) {
                    let _ = xattr::set(dst, &attr, &value);
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Copy-on-write
// ---------------------------------------------------------------------------

/// Attempt a CoW clone. Returns Ok(true) on success, Ok(false) if unsupported.
#[allow(clippy::needless_return)]
fn try_cow_copy(src: &Path, dst: &Path) -> Result<bool> {
    #[cfg(target_os = "macos")]
    {
        return cow_macos(src, dst);
    }
    #[cfg(target_os = "linux")]
    {
        return cow_linux(src, dst);
    }
    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        Ok(false)
    }
}

#[cfg(target_os = "macos")]
fn cow_macos(src: &Path, dst: &Path) -> Result<bool> {
    use std::ffi::CString;

    let src_c = CString::new(src.to_string_lossy().as_bytes()).context("invalid source path")?;
    let dst_c = CString::new(dst.to_string_lossy().as_bytes()).context("invalid dest path")?;

    // clonefile(src, dst, 0) — flag 0 means no special flags
    let ret = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
    if ret == 0 {
        return Ok(true);
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        // ENOTSUP (45), EXDEV (18) — not supported on this filesystem/across volumes
        Some(45) | Some(18) => Ok(false),
        // EEXIST — destination already exists, remove and retry
        Some(17) => {
            let _ = fs::remove_file(dst);
            let ret = unsafe { libc::clonefile(src_c.as_ptr(), dst_c.as_ptr(), 0) };
            if ret == 0 {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        _ => Ok(false),
    }
}

#[cfg(target_os = "linux")]
fn cow_linux(src: &Path, dst: &Path) -> Result<bool> {
    use std::os::unix::io::AsRawFd;

    let src_file = fs::File::open(src)?;
    let dst_file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dst)?;

    // FICLONE ioctl
    const FICLONE: libc::Ioctl = 0x40049409;
    let ret = unsafe { libc::ioctl(dst_file.as_raw_fd(), FICLONE, src_file.as_raw_fd()) };
    if ret == 0 {
        Ok(true)
    } else {
        // Not supported — caller will fall back to regular copy
        // Remove the empty file we created
        drop(dst_file);
        let _ = fs::remove_file(dst);
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Write cache bypass
// ---------------------------------------------------------------------------

#[allow(unused_variables)]
fn set_nocache(file: &fs::File) {
    #[cfg(target_os = "macos")]
    {
        use std::os::unix::io::AsRawFd;
        // F_NOCACHE = 48 on macOS
        unsafe { libc::fcntl(file.as_raw_fd(), 48, 1) };
    }
    // On Linux, O_DIRECT requires aligned buffers which adds complexity.
    // For now, we only support F_NOCACHE on macOS. Linux is a no-op.
}

// ---------------------------------------------------------------------------
// Move
// ---------------------------------------------------------------------------

pub fn move_entry(source: &Path, dest_dir: &Path, opts: &CopyOptions) -> Result<()> {
    let file_name = source.file_name().context("source has no file name")?;
    let dest = dest_dir.join(file_name);

    let resolved = match resolve_dest(&dest, opts.conflict) {
        Some(p) => p,
        None => return Ok(()), // Skip
    };

    // Try rename first (fast, same-filesystem move)
    match fs::rename(source, &resolved) {
        Ok(()) => Ok(()),
        Err(_) => {
            // Fall back to copy + delete (cross-filesystem).
            // Use Overwrite conflict since we already resolved the destination.
            let mut fallback_opts = opts.clone();
            fallback_opts.conflict = ConflictPolicy::Overwrite;
            if source.is_dir() {
                copy_dir_recursive(source, &resolved, &fallback_opts)?;
            } else {
                copy_single_file(source, &resolved, &fallback_opts)?;
            }
            delete_entry(source)?;
            Ok(())
        }
    }
}

// ---------------------------------------------------------------------------
// Other fs operations
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Sparse copy
// ---------------------------------------------------------------------------

mod sparse_copy {
    use anyhow::{Context, Result};
    use std::fs;
    use std::io::{Read, Seek, SeekFrom, Write};
    use std::path::Path;

    pub fn copy_file_sparse(src: &Path, dst: &Path, disable_cache: bool) -> Result<()> {
        let src_meta = fs::metadata(src).with_context(|| format!("Failed to stat {:?}", src))?;
        let src_len = src_meta.len();

        if src_len == 0 {
            fs::File::create(dst).with_context(|| format!("Failed to create {:?}", dst))?;
            return Ok(());
        }

        #[cfg(target_os = "linux")]
        {
            linux_sparse_copy(src, dst, src_len, disable_cache)
        }

        #[cfg(not(target_os = "linux"))]
        {
            generic_sparse_copy(src, dst, src_len, disable_cache)
        }
    }

    #[cfg(target_os = "linux")]
    fn linux_sparse_copy(src: &Path, dst: &Path, src_len: u64, disable_cache: bool) -> Result<()> {
        use std::os::unix::io::AsRawFd;

        let src_file = fs::File::open(src).with_context(|| format!("Failed to open {:?}", src))?;
        let mut dst_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dst)
            .with_context(|| format!("Failed to create {:?}", dst))?;

        if disable_cache {
            super::set_nocache(&dst_file);
        }

        dst_file.set_len(src_len)?;

        let fd = src_file.as_raw_fd();
        let mut pos: i64 = 0;
        let len = src_len as i64;
        let mut buf = vec![0u8; 256 * 1024];

        loop {
            if pos >= len {
                break;
            }

            let data_start = unsafe { libc::lseek(fd, pos, libc::SEEK_DATA) };
            if data_start < 0 {
                break;
            }

            let hole_start = unsafe { libc::lseek(fd, data_start, libc::SEEK_HOLE) };
            let data_end = if hole_start < 0 { len } else { hole_start };

            let mut src_reader = &src_file;
            src_reader.seek(SeekFrom::Start(data_start as u64))?;
            dst_file.seek(SeekFrom::Start(data_start as u64))?;

            let mut remaining = (data_end - data_start) as usize;
            while remaining > 0 {
                let to_read = remaining.min(buf.len());
                let n = src_reader.read(&mut buf[..to_read])?;
                if n == 0 {
                    break;
                }
                dst_file.write_all(&buf[..n])?;
                remaining -= n;
            }

            pos = data_end;
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn generic_sparse_copy(
        src: &Path,
        dst: &Path,
        src_len: u64,
        disable_cache: bool,
    ) -> Result<()> {
        let mut src_file =
            fs::File::open(src).with_context(|| format!("Failed to open {:?}", src))?;
        let mut dst_file = fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(dst)
            .with_context(|| format!("Failed to create {:?}", dst))?;

        if disable_cache {
            super::set_nocache(&dst_file);
        }

        dst_file.set_len(src_len)?;

        let mut buf = [0u8; 4096];
        let mut dst_pos: u64 = 0;
        let mut needs_seek = false;

        loop {
            let n = src_file.read(&mut buf)?;
            if n == 0 {
                break;
            }

            if is_zero(&buf[..n]) {
                dst_pos += n as u64;
                needs_seek = true;
            } else {
                if needs_seek {
                    dst_file.seek(SeekFrom::Start(dst_pos))?;
                    needs_seek = false;
                }
                dst_file.write_all(&buf[..n])?;
                dst_pos += n as u64;
            }
        }

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    fn is_zero(buf: &[u8]) -> bool {
        let (prefix, chunks, suffix) = unsafe { buf.align_to::<u64>() };
        prefix.iter().all(|&b| b == 0)
            && chunks.iter().all(|&w| w == 0)
            && suffix.iter().all(|&b| b == 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::path::Path;
    use tempfile::tempdir;

    /// Helper: default CopyOptions with sensible test defaults.
    fn default_opts() -> CopyOptions {
        CopyOptions {
            sparse: false,
            conflict: ConflictPolicy::Overwrite,
            copy_permissions: false,
            copy_xattrs: false,
            disable_write_cache: false,
            use_cow: false,
            symlink_mode: SymlinkCopyMode::Follow,
        }
    }

    /// Helper: write a file with given content.
    fn write_file(path: &Path, content: &str) {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).unwrap();
        }
        fs::write(path, content).unwrap();
    }

    /// Helper: read a file to string.
    fn read_file(path: &Path) -> String {
        fs::read_to_string(path).unwrap()
    }

    // =====================================================================
    // 1. Basic copy
    // =====================================================================

    #[test]
    fn copy_single_file_basic() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        write_file(&src, "hello world");
        copy_entry(&src, &dst_dir, &default_opts()).unwrap();

        let copied = dst_dir.join("src");
        assert!(copied.exists());
        assert_eq!(read_file(&copied), "hello world");
    }

    #[test]
    fn copy_directory_recursive() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("mydir");
        fs::create_dir_all(src_dir.join("sub")).unwrap();
        write_file(&src_dir.join("a.txt"), "aaa");
        write_file(&src_dir.join("sub").join("b.txt"), "bbb");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        copy_entry(&src_dir, &dst_dir, &default_opts()).unwrap();

        let copied = dst_dir.join("mydir");
        assert!(copied.is_dir());
        assert_eq!(read_file(&copied.join("a.txt")), "aaa");
        assert_eq!(read_file(&copied.join("sub").join("b.txt")), "bbb");
    }

    // =====================================================================
    // 2. Conflict policies
    // =====================================================================

    #[test]
    fn conflict_overwrite() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("file.txt");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&src, "new content");
        write_file(&dst_dir.join("file.txt"), "old content");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Overwrite;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        assert_eq!(read_file(&dst_dir.join("file.txt")), "new content");
    }

    #[test]
    fn conflict_skip() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("file.txt");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&src, "new content");
        write_file(&dst_dir.join("file.txt"), "old content");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Skip;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        assert_eq!(read_file(&dst_dir.join("file.txt")), "old content");
    }

    #[test]
    fn conflict_rename() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("file.txt");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&src, "new content");
        write_file(&dst_dir.join("file.txt"), "old content");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Rename;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        // Original should be unchanged.
        assert_eq!(read_file(&dst_dir.join("file.txt")), "old content");
        // Renamed copy should exist.
        let renamed = dst_dir.join("file(1).txt");
        assert!(renamed.exists(), "file(1).txt should exist");
        assert_eq!(read_file(&renamed), "new content");
    }

    #[test]
    fn conflict_append() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("file.txt");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&src, " world");
        write_file(&dst_dir.join("file.txt"), "hello");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Append;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        assert_eq!(read_file(&dst_dir.join("file.txt")), "hello world");
    }

    // =====================================================================
    // 3. Sparse copy
    // =====================================================================

    #[test]
    fn sparse_copy_preserves_content() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("sparse_src.bin");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        // Write a file with a block of zeros followed by data.
        let mut data = vec![0u8; 8192];
        data.extend_from_slice(b"non-zero payload here!");
        fs::write(&src, &data).unwrap();

        let mut opts = default_opts();
        opts.sparse = true;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("sparse_src.bin");
        assert!(copied.exists());
        let copied_data = fs::read(&copied).unwrap();
        assert_eq!(copied_data, data);
    }

    // =====================================================================
    // 4. CoW copy
    // =====================================================================

    #[test]
    fn cow_copy_falls_back_gracefully() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("cow_src.txt");
        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&src, "cow test data");

        let mut opts = default_opts();
        opts.use_cow = true;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("cow_src.txt");
        assert!(copied.exists());
        assert_eq!(read_file(&copied), "cow test data");
    }

    // =====================================================================
    // 5. Symlink modes
    // =====================================================================

    #[cfg(unix)]
    #[test]
    fn symlink_preserve_mode() {
        let tmp = tempdir().unwrap();
        let real_file = tmp.path().join("real.txt");
        write_file(&real_file, "real content");

        let link = tmp.path().join("link.txt");
        symlink(&real_file, &link).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.symlink_mode = SymlinkCopyMode::Preserve;
        copy_entry(&link, &dst_dir, &opts).unwrap();

        let copied_link = dst_dir.join("link.txt");
        assert!(copied_link.symlink_metadata().unwrap().is_symlink());
        assert_eq!(fs::read_link(&copied_link).unwrap(), real_file);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_follow_mode() {
        let tmp = tempdir().unwrap();
        let real_file = tmp.path().join("real.txt");
        write_file(&real_file, "real content");

        let link = tmp.path().join("link.txt");
        symlink(&real_file, &link).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.symlink_mode = SymlinkCopyMode::Follow;
        copy_entry(&link, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("link.txt");
        // Should be a regular file, not a symlink.
        assert!(copied.exists());
        assert!(!copied.symlink_metadata().unwrap().is_symlink());
        assert_eq!(read_file(&copied), "real content");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_smart_mode_follows_top_level_dir() {
        let tmp = tempdir().unwrap();

        // Create a real directory with a file and an inner symlink.
        let real_dir = tmp.path().join("realdir");
        fs::create_dir_all(&real_dir).unwrap();
        write_file(&real_dir.join("a.txt"), "aaa");

        let inner_target = tmp.path().join("inner_target.txt");
        write_file(&inner_target, "inner");
        symlink(&inner_target, &real_dir.join("inner_link.txt")).unwrap();

        // Create a top-level symlink to the directory.
        let dir_link = tmp.path().join("dir_link");
        symlink(&real_dir, &dir_link).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.symlink_mode = SymlinkCopyMode::Smart;
        copy_entry(&dir_link, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("dir_link");
        // Top-level symlink dir should be followed (copied as real dir).
        assert!(copied.is_dir());
        assert!(!copied.symlink_metadata().unwrap().is_symlink());
        assert_eq!(read_file(&copied.join("a.txt")), "aaa");

        // Inner symlink should be preserved.
        let inner = copied.join("inner_link.txt");
        assert!(inner.symlink_metadata().unwrap().is_symlink());
        assert_eq!(fs::read_link(&inner).unwrap(), inner_target);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_preserve_mode_dir() {
        let tmp = tempdir().unwrap();
        let real_dir = tmp.path().join("realdir");
        fs::create_dir_all(&real_dir).unwrap();

        let link = tmp.path().join("link_to_dir");
        symlink(&real_dir, &link).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.symlink_mode = SymlinkCopyMode::Preserve;
        copy_entry(&link, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("link_to_dir");
        assert!(copied.symlink_metadata().unwrap().is_symlink());
        assert_eq!(fs::read_link(&copied).unwrap(), real_dir);
    }

    // =====================================================================
    // 6. Permission preservation
    // =====================================================================

    #[cfg(unix)]
    #[test]
    fn copy_preserves_permissions() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("exec.sh");
        write_file(&src, "#!/bin/sh\necho hi");
        fs::set_permissions(&src, fs::Permissions::from_mode(0o755)).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.copy_permissions = true;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("exec.sh");
        let mode = copied.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
    }

    #[cfg(unix)]
    #[test]
    fn copy_preserves_directory_permissions() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("restricted");
        fs::create_dir_all(&src_dir).unwrap();
        write_file(&src_dir.join("f.txt"), "data");
        fs::set_permissions(&src_dir, fs::Permissions::from_mode(0o700)).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.copy_permissions = true;
        copy_entry(&src_dir, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("restricted");
        let mode = copied.metadata().unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o700);
    }

    // =====================================================================
    // 7. Extended attributes
    // =====================================================================

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn copy_preserves_xattrs() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("xattr_src.txt");
        write_file(&src, "xattr test");

        // Set an xattr (use user namespace on Linux, any name on macOS).
        let attr_name = if cfg!(target_os = "linux") {
            "user.test_attr"
        } else {
            "com.test.attr"
        };
        xattr::set(&src, attr_name, b"test_value").unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.copy_xattrs = true;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("xattr_src.txt");
        let val = xattr::get(&copied, attr_name).unwrap();
        assert_eq!(val, Some(b"test_value".to_vec()));
    }

    // =====================================================================
    // 8. plan_copy
    // =====================================================================

    #[test]
    fn plan_copy_flat_file() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("hello.txt");
        write_file(&src, "hi");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let items = plan_copy(&src, &dst_dir, SymlinkCopyMode::Follow).unwrap();
        assert_eq!(items.len(), 1);
        assert!(!items[0].is_dir);
        assert!(!items[0].is_symlink);
        assert_eq!(items[0].src, src);
        assert_eq!(items[0].dst, dst_dir.join("hello.txt"));
    }

    #[test]
    fn plan_copy_directory_tree() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("tree");
        fs::create_dir_all(src_dir.join("sub")).unwrap();
        write_file(&src_dir.join("a.txt"), "a");
        write_file(&src_dir.join("sub").join("b.txt"), "b");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let items = plan_copy(&src_dir, &dst_dir, SymlinkCopyMode::Follow).unwrap();

        // Should have: tree(dir), a.txt(file), sub(dir), b.txt(file)
        assert_eq!(items.len(), 4);

        // First item should be the top-level dir.
        assert!(items[0].is_dir);
        assert_eq!(items[0].dst, dst_dir.join("tree"));

        // Check that all expected paths are present.
        let dst_paths: Vec<PathBuf> = items.iter().map(|i| i.dst.clone()).collect();
        assert!(dst_paths.contains(&dst_dir.join("tree")));
        assert!(dst_paths.contains(&dst_dir.join("tree").join("a.txt")));
        assert!(dst_paths.contains(&dst_dir.join("tree").join("sub")));
        assert!(dst_paths.contains(&dst_dir.join("tree").join("sub").join("b.txt")));

        // Verify is_dir flags.
        for item in &items {
            if item.dst.ends_with("tree") || item.dst.ends_with("sub") {
                assert!(item.is_dir, "{:?} should be is_dir", item.dst);
            } else {
                assert!(!item.is_dir, "{:?} should not be is_dir", item.dst);
            }
        }
    }

    #[cfg(unix)]
    #[test]
    fn plan_copy_with_symlink_preserve() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("sdir");
        fs::create_dir_all(&src_dir).unwrap();

        let target = tmp.path().join("target.txt");
        write_file(&target, "t");
        symlink(&target, &src_dir.join("link.txt")).unwrap();
        write_file(&src_dir.join("real.txt"), "r");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let items = plan_copy(&src_dir, &dst_dir, SymlinkCopyMode::Preserve).unwrap();

        let link_item = items.iter().find(|i| i.dst.ends_with("link.txt")).unwrap();
        assert!(link_item.is_symlink);
        assert!(!link_item.is_dir);

        let real_item = items.iter().find(|i| i.dst.ends_with("real.txt")).unwrap();
        assert!(!real_item.is_symlink);
        assert!(!real_item.is_dir);
    }

    #[cfg(unix)]
    #[test]
    fn plan_copy_smart_mode_preserves_inner_symlinks() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("smartdir");
        fs::create_dir_all(&src_dir).unwrap();

        let target = tmp.path().join("target.txt");
        write_file(&target, "t");
        symlink(&target, &src_dir.join("inner_link.txt")).unwrap();
        write_file(&src_dir.join("real.txt"), "r");

        // Top-level symlink to the directory.
        let top_link = tmp.path().join("top_link");
        symlink(&src_dir, &top_link).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let items = plan_copy(&top_link, &dst_dir, SymlinkCopyMode::Smart).unwrap();

        // Top-level should be followed (dir, not symlink).
        assert!(items[0].is_dir);
        assert!(!items[0].is_symlink);

        // Inner symlink should be preserved.
        let inner = items
            .iter()
            .find(|i| i.dst.ends_with("inner_link.txt"))
            .unwrap();
        assert!(inner.is_symlink);
    }

    // =====================================================================
    // 9. exec_copy_item
    // =====================================================================

    #[test]
    fn exec_copy_item_file() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src.txt");
        write_file(&src, "payload");

        let dst = tmp.path().join("dst.txt");
        let item = CopyItem {
            src: src.clone(),
            dst: dst.clone(),
            is_dir: false,
            is_symlink: false,
        };
        exec_copy_item(&item, &default_opts()).unwrap();
        assert_eq!(read_file(&dst), "payload");
    }

    #[test]
    fn exec_copy_item_dir() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("srcdir");
        fs::create_dir_all(&src_dir).unwrap();

        let dst_dir = tmp.path().join("dstdir");
        let item = CopyItem {
            src: src_dir.clone(),
            dst: dst_dir.clone(),
            is_dir: true,
            is_symlink: false,
        };
        exec_copy_item(&item, &default_opts()).unwrap();
        assert!(dst_dir.is_dir());
    }

    #[cfg(unix)]
    #[test]
    fn exec_copy_item_symlink() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("target.txt");
        write_file(&target, "target data");

        let link_src = tmp.path().join("link");
        symlink(&target, &link_src).unwrap();

        let link_dst = tmp.path().join("link_copy");
        let item = CopyItem {
            src: link_src.clone(),
            dst: link_dst.clone(),
            is_dir: false,
            is_symlink: true,
        };
        exec_copy_item(&item, &default_opts()).unwrap();
        assert!(link_dst.symlink_metadata().unwrap().is_symlink());
        assert_eq!(fs::read_link(&link_dst).unwrap(), target);
    }

    // =====================================================================
    // 10. unique_name
    // =====================================================================

    #[test]
    fn unique_name_basic() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.txt");
        write_file(&path, "exists");

        let result = unique_name(&path);
        assert_eq!(result, tmp.path().join("file(1).txt"));
    }

    #[test]
    fn unique_name_increments() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("file.txt");
        write_file(&path, "exists");
        write_file(&tmp.path().join("file(1).txt"), "also exists");

        let result = unique_name(&path);
        assert_eq!(result, tmp.path().join("file(2).txt"));
    }

    #[test]
    fn unique_name_no_extension() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("README");
        write_file(&path, "exists");

        let result = unique_name(&path);
        assert_eq!(result, tmp.path().join("README(1)"));
    }

    #[test]
    fn unique_name_nonexistent_returns_self() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("nofile.txt");
        // Should not be called when file doesn't exist (resolve_dest handles that),
        // but the function still returns (1) variant since the loop starts at 1.
        let result = unique_name(&path);
        assert_eq!(result, tmp.path().join("nofile(1).txt"));
    }

    // =====================================================================
    // 11. move_entry
    // =====================================================================

    #[test]
    fn move_entry_same_fs_rename() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("moveme.txt");
        write_file(&src, "move data");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        move_entry(&src, &dst_dir, &default_opts()).unwrap();

        assert!(!src.exists());
        assert_eq!(read_file(&dst_dir.join("moveme.txt")), "move data");
    }

    #[test]
    fn move_entry_with_conflict_skip() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("file.txt");
        write_file(&src, "new");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&dst_dir.join("file.txt"), "old");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Skip;
        move_entry(&src, &dst_dir, &opts).unwrap();

        // Source should still exist (skip means no move).
        assert!(src.exists());
        assert_eq!(read_file(&dst_dir.join("file.txt")), "old");
    }

    #[test]
    fn move_entry_with_conflict_rename() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("file.txt");
        write_file(&src, "new");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&dst_dir.join("file.txt"), "old");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Rename;
        move_entry(&src, &dst_dir, &opts).unwrap();

        assert!(!src.exists());
        assert_eq!(read_file(&dst_dir.join("file.txt")), "old");
        assert_eq!(read_file(&dst_dir.join("file(1).txt")), "new");
    }

    #[test]
    fn move_entry_directory() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("movedir");
        fs::create_dir_all(&src_dir).unwrap();
        write_file(&src_dir.join("inner.txt"), "inner");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        move_entry(&src_dir, &dst_dir, &default_opts()).unwrap();

        assert!(!src_dir.exists());
        assert_eq!(
            read_file(&dst_dir.join("movedir").join("inner.txt")),
            "inner"
        );
    }

    // =====================================================================
    // 12. copy_symlink
    // =====================================================================

    #[cfg(unix)]
    #[test]
    fn copy_symlink_preserves_target() {
        let tmp = tempdir().unwrap();
        let target = tmp.path().join("original.txt");
        write_file(&target, "original");

        let link_src = tmp.path().join("mylink");
        symlink(&target, &link_src).unwrap();

        let link_dst = tmp.path().join("mylink_copy");
        copy_symlink(&link_src, &link_dst).unwrap();

        assert!(link_dst.symlink_metadata().unwrap().is_symlink());
        assert_eq!(fs::read_link(&link_dst).unwrap(), target);
        // Reading through the symlink should work.
        assert_eq!(read_file(&link_dst), "original");
    }

    #[cfg(unix)]
    #[test]
    fn copy_symlink_relative_target() {
        let tmp = tempdir().unwrap();
        write_file(&tmp.path().join("target.txt"), "rel content");

        let link_src = tmp.path().join("rel_link");
        symlink(Path::new("target.txt"), &link_src).unwrap();

        let link_dst = tmp.path().join("rel_link_copy");
        copy_symlink(&link_src, &link_dst).unwrap();

        assert!(link_dst.symlink_metadata().unwrap().is_symlink());
        // The symlink target is relative, so it should be the same relative path.
        assert_eq!(fs::read_link(&link_dst).unwrap(), Path::new("target.txt"));
    }

    // =====================================================================
    // 13. Multi-level directory copy
    // =====================================================================

    #[test]
    fn multi_level_directory_copy() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        fs::create_dir_all(root.join("a").join("b").join("c")).unwrap();
        write_file(&root.join("top.txt"), "top");
        write_file(&root.join("a").join("mid.txt"), "mid");
        write_file(&root.join("a").join("b").join("deep.txt"), "deep");
        write_file(
            &root.join("a").join("b").join("c").join("deepest.txt"),
            "deepest",
        );

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        copy_entry(&root, &dst_dir, &default_opts()).unwrap();

        let copied = dst_dir.join("root");
        assert_eq!(read_file(&copied.join("top.txt")), "top");
        assert_eq!(read_file(&copied.join("a").join("mid.txt")), "mid");
        assert_eq!(
            read_file(&copied.join("a").join("b").join("deep.txt")),
            "deep"
        );
        assert_eq!(
            read_file(&copied.join("a").join("b").join("c").join("deepest.txt")),
            "deepest"
        );
    }

    #[cfg(unix)]
    #[test]
    fn multi_level_with_symlinks() {
        let tmp = tempdir().unwrap();
        let root = tmp.path().join("root");
        fs::create_dir_all(root.join("sub")).unwrap();
        write_file(&root.join("file.txt"), "f");

        let external = tmp.path().join("external.txt");
        write_file(&external, "ext");
        symlink(&external, &root.join("ext_link.txt")).unwrap();
        symlink(&external, &root.join("sub").join("nested_link.txt")).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.symlink_mode = SymlinkCopyMode::Preserve;
        copy_entry(&root, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("root");
        assert_eq!(read_file(&copied.join("file.txt")), "f");

        // Both symlinks should be preserved.
        assert!(copied
            .join("ext_link.txt")
            .symlink_metadata()
            .unwrap()
            .is_symlink());
        assert!(copied
            .join("sub")
            .join("nested_link.txt")
            .symlink_metadata()
            .unwrap()
            .is_symlink());
    }

    // =====================================================================
    // 14. Edge cases
    // =====================================================================

    #[test]
    fn copy_empty_file() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("empty.txt");
        write_file(&src, "");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        copy_entry(&src, &dst_dir, &default_opts()).unwrap();

        let copied = dst_dir.join("empty.txt");
        assert!(copied.exists());
        assert_eq!(read_file(&copied), "");
        assert_eq!(copied.metadata().unwrap().len(), 0);
    }

    #[test]
    fn copy_empty_file_sparse() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("empty_sparse.txt");
        write_file(&src, "");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.sparse = true;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        let copied = dst_dir.join("empty_sparse.txt");
        assert!(copied.exists());
        assert_eq!(copied.metadata().unwrap().len(), 0);
    }

    #[test]
    fn rename_no_extension() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("Makefile");
        write_file(&src, "new");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&dst_dir.join("Makefile"), "old");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Rename;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        assert_eq!(read_file(&dst_dir.join("Makefile")), "old");
        assert_eq!(read_file(&dst_dir.join("Makefile(1)")), "new");
    }

    #[test]
    fn copy_to_same_directory_with_rename() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("dup.txt");
        write_file(&src, "original");

        // Copy to the same directory, which means the dest will conflict.
        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Rename;
        copy_entry(&src, tmp.path(), &opts).unwrap();

        // Original unchanged.
        assert_eq!(read_file(&src), "original");
        // Renamed copy.
        assert_eq!(read_file(&tmp.path().join("dup(1).txt")), "original");
    }

    #[test]
    fn copy_to_same_directory_with_skip() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("dup.txt");
        write_file(&src, "original");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Skip;
        copy_entry(&src, tmp.path(), &opts).unwrap();

        // Only original should exist.
        assert_eq!(read_file(&src), "original");
        assert!(!tmp.path().join("dup(1).txt").exists());
    }

    #[test]
    fn conflict_rename_multiple_copies() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("data.txt");
        write_file(&src, "content");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        write_file(&dst_dir.join("data.txt"), "v0");

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Rename;

        // Copy three times — should create data(1).txt, data(2).txt, data(3).txt.
        copy_entry(&src, &dst_dir, &opts).unwrap();
        copy_entry(&src, &dst_dir, &opts).unwrap();
        copy_entry(&src, &dst_dir, &opts).unwrap();

        assert_eq!(read_file(&dst_dir.join("data.txt")), "v0");
        assert_eq!(read_file(&dst_dir.join("data(1).txt")), "content");
        assert_eq!(read_file(&dst_dir.join("data(2).txt")), "content");
        assert_eq!(read_file(&dst_dir.join("data(3).txt")), "content");
    }

    #[test]
    fn append_to_nonexistent_creates_file() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("src.txt");
        write_file(&src, "new content");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        // No existing file in dst_dir.

        let mut opts = default_opts();
        opts.conflict = ConflictPolicy::Append;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        // When destination doesn't exist, Append should create the file normally.
        assert_eq!(read_file(&dst_dir.join("src.txt")), "new content");
    }

    #[test]
    fn copy_large_file() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("big.bin");
        // 256KB of patterned data.
        let data: Vec<u8> = (0..256 * 1024).map(|i| (i % 251) as u8).collect();
        fs::write(&src, &data).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        copy_entry(&src, &dst_dir, &default_opts()).unwrap();

        let copied_data = fs::read(&dst_dir.join("big.bin")).unwrap();
        assert_eq!(copied_data, data);
    }

    #[test]
    fn copy_with_manual_copy_disable_write_cache() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("cached.txt");
        write_file(&src, "test data for manual copy");

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let mut opts = default_opts();
        opts.disable_write_cache = true;
        copy_entry(&src, &dst_dir, &opts).unwrap();

        assert_eq!(
            read_file(&dst_dir.join("cached.txt")),
            "test data for manual copy"
        );
    }

    #[test]
    fn copy_empty_directory() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("empty_dir");
        fs::create_dir_all(&src_dir).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();
        copy_entry(&src_dir, &dst_dir, &default_opts()).unwrap();

        let copied = dst_dir.join("empty_dir");
        assert!(copied.is_dir());
        assert_eq!(fs::read_dir(&copied).unwrap().count(), 0);
    }

    #[test]
    fn plan_copy_empty_directory() {
        let tmp = tempdir().unwrap();
        let src_dir = tmp.path().join("empty_plan");
        fs::create_dir_all(&src_dir).unwrap();

        let dst_dir = tmp.path().join("dst");
        fs::create_dir_all(&dst_dir).unwrap();

        let items = plan_copy(&src_dir, &dst_dir, SymlinkCopyMode::Follow).unwrap();
        assert_eq!(items.len(), 1);
        assert!(items[0].is_dir);
    }

    #[test]
    fn exec_copy_item_with_permissions() {
        let tmp = tempdir().unwrap();
        let src = tmp.path().join("perm_src.txt");
        write_file(&src, "perm test");

        let dst = tmp.path().join("perm_dst.txt");
        let item = CopyItem {
            src: src.clone(),
            dst: dst.clone(),
            is_dir: false,
            is_symlink: false,
        };
        let mut opts = default_opts();
        opts.copy_permissions = true;
        exec_copy_item(&item, &opts).unwrap();
        assert_eq!(read_file(&dst), "perm test");
    }

    #[test]
    fn resolve_dest_returns_some_for_new_file() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("nonexistent.txt");
        let result = resolve_dest(&path, ConflictPolicy::Skip);
        assert_eq!(result, Some(path));
    }

    #[test]
    fn resolve_dest_returns_none_for_skip() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("exists.txt");
        write_file(&path, "hi");
        let result = resolve_dest(&path, ConflictPolicy::Skip);
        assert!(result.is_none());
    }

    #[test]
    fn resolve_dest_returns_same_for_overwrite() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("exists.txt");
        write_file(&path, "hi");
        let result = resolve_dest(&path, ConflictPolicy::Overwrite);
        assert_eq!(result, Some(path));
    }

    #[test]
    fn resolve_dest_returns_renamed_for_rename() {
        let tmp = tempdir().unwrap();
        let path = tmp.path().join("exists.txt");
        write_file(&path, "hi");
        let result = resolve_dest(&path, ConflictPolicy::Rename);
        assert_eq!(result, Some(tmp.path().join("exists(1).txt")));
    }
}
