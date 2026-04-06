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
    const FICLONE: libc::c_ulong = 0x40049409;
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

pub fn create_directory(parent: &Path, name: &str) -> Result<()> {
    let path = parent.join(name);
    fs::create_dir_all(&path).with_context(|| format!("Failed to create directory {:?}", path))?;
    Ok(())
}

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
