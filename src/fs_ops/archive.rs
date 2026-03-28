use std::collections::HashMap;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};

// ---------------------------------------------------------------------------
// Archive format
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq)]
pub enum ArchiveFormat {
    TarZst,
    TarGz,
    TarXz,
    Zip,
}

impl ArchiveFormat {
    pub fn label(self) -> &'static str {
        match self {
            Self::TarZst => "tar.zst",
            Self::TarGz => "tar.gz",
            Self::TarXz => "tar.xz",
            Self::Zip => "zip",
        }
    }

    pub fn extension(self) -> &'static str {
        match self {
            Self::TarZst => ".tar.zst",
            Self::TarGz => ".tar.gz",
            Self::TarXz => ".tar.xz",
            Self::Zip => ".zip",
        }
    }

    pub fn next(self) -> Self {
        match self {
            Self::TarZst => Self::TarGz,
            Self::TarGz => Self::TarXz,
            Self::TarXz => Self::Zip,
            Self::Zip => Self::TarZst,
        }
    }
}

// ---------------------------------------------------------------------------
// Smart archive naming
// ---------------------------------------------------------------------------

pub fn suggest_archive_name(paths: &[PathBuf], format: ArchiveFormat) -> String {
    let ext = format.extension();

    if paths.is_empty() {
        return format!("archive{}", ext);
    }

    // Single item
    if paths.len() == 1 {
        let name = paths[0]
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "archive".into());
        return format!("{}{}", name, ext);
    }

    let n = paths.len();
    let filenames: Vec<String> = paths
        .iter()
        .filter_map(|p| p.file_name().map(|n| n.to_string_lossy().into_owned()))
        .collect();

    // Try longest common prefix (word-boundary aware)
    if let Some(prefix) = longest_common_prefix(&filenames) {
        if prefix.len() >= 3 {
            return format!("{}-{}files{}", prefix, n, ext);
        }
    }

    // Try dominant extension
    if let Some(dominant_ext) = dominant_extension(paths) {
        return format!("{}-{}-files{}", n, dominant_ext, ext);
    }

    // Try common parent directory name
    if let Some(parent) = common_parent_name(paths) {
        return format!("{}-{}files{}", parent, n, ext);
    }

    // Fallback
    format!("archive-{}files{}", n, ext)
}

/// Find the longest common prefix of filenames, trimming at word boundaries.
/// Uses char-level comparison to avoid slicing mid-UTF-8.
fn longest_common_prefix(names: &[String]) -> Option<String> {
    if names.is_empty() {
        return None;
    }

    let first = &names[0];
    let mut prefix_chars = first.chars().count();

    for name in &names[1..] {
        prefix_chars = prefix_chars.min(name.chars().count());
        for (i, (a, b)) in first.chars().zip(name.chars()).enumerate() {
            if a != b {
                prefix_chars = prefix_chars.min(i);
                break;
            }
        }
    }

    if prefix_chars == 0 {
        return None;
    }

    let raw: String = first.chars().take(prefix_chars).collect();
    // Trim trailing separators: '-', '_', '.'
    let trimmed = raw.trim_end_matches(['-', '_', '.']);
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

/// Find the most common file extension if >50% of files share it.
fn dominant_extension(paths: &[PathBuf]) -> Option<String> {
    let mut counts: HashMap<String, usize> = HashMap::new();
    for p in paths {
        if let Some(ext) = p.extension() {
            *counts
                .entry(ext.to_string_lossy().to_lowercase())
                .or_default() += 1;
        }
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > paths.len() / 2)
        .max_by_key(|(_, count)| *count)
        .map(|(ext, _)| ext)
}

/// Get the name of the common parent directory.
fn common_parent_name(paths: &[PathBuf]) -> Option<String> {
    let first_parent = paths[0].parent()?;
    if paths[1..].iter().all(|p| p.parent() == Some(first_parent)) {
        first_parent
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
    } else {
        None
    }
}

/// Resolve filename collision by appending date, then -2, -3, etc.
pub fn resolve_collision(dir: &Path, base_name: &str, ext: &str) -> String {
    let candidate = format!("{}{}", base_name, ext);
    if !dir.join(&candidate).exists() {
        return candidate;
    }

    let date = chrono::Local::now().format("%Y%m%d").to_string();
    let candidate = format!("{}-{}{}", base_name, date, ext);
    if !dir.join(&candidate).exists() {
        return candidate;
    }

    for i in 2.. {
        let candidate = format!("{}-{}-{}{}", base_name, date, i, ext);
        if !dir.join(&candidate).exists() {
            return candidate;
        }
    }
    unreachable!()
}

// ---------------------------------------------------------------------------
// Archive creation
// ---------------------------------------------------------------------------

/// Recursively compute total size of all files in the given paths.
/// Skips symlinks to prevent infinite loops.
pub fn compute_total_size(paths: &[PathBuf]) -> u64 {
    let mut total = 0u64;
    for path in paths {
        // Use symlink_metadata to avoid following symlinks
        let meta = match path.symlink_metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.is_symlink() {
            continue;
        }
        if meta.is_dir() {
            if let Ok(entries) = std::fs::read_dir(path) {
                let children: Vec<PathBuf> =
                    entries.filter_map(|e| e.ok().map(|e| e.path())).collect();
                total += compute_total_size(&children);
            }
        } else {
            total += meta.len();
        }
    }
    total
}

/// Create an archive from the given paths. Updates `done_bytes` as files are added.
/// Set `cancel` to true to abort.
pub fn create_archive(
    paths: &[PathBuf],
    output: &Path,
    format: ArchiveFormat,
    done_bytes: Arc<AtomicU64>,
    cancel: Arc<AtomicBool>,
) -> Result<()> {
    // Determine base directory for relative paths in the archive
    let base_dir = paths[0]
        .parent()
        .unwrap_or_else(|| Path::new("/"))
        .to_path_buf();

    match format {
        ArchiveFormat::Zip => create_zip(paths, &base_dir, output, &done_bytes, &cancel),
        _ => create_tar(paths, &base_dir, output, format, &done_bytes, &cancel),
    }
}

fn create_tar(
    paths: &[PathBuf],
    base_dir: &Path,
    output: &Path,
    format: ArchiveFormat,
    done_bytes: &AtomicU64,
    cancel: &AtomicBool,
) -> Result<()> {
    let file = File::create(output).context("Failed to create archive file")?;
    let buf = BufWriter::new(file);

    // Wrap in compressor
    let writer: Box<dyn Write> = match format {
        ArchiveFormat::TarZst => {
            let enc = zstd::stream::write::Encoder::new(buf, 3)
                .context("Failed to create zstd encoder")?;
            Box::new(enc.auto_finish())
        }
        ArchiveFormat::TarGz => {
            let enc = flate2::write::GzEncoder::new(buf, flate2::Compression::default());
            Box::new(enc)
        }
        ArchiveFormat::TarXz => {
            let enc = xz2::write::XzEncoder::new(buf, 6);
            Box::new(enc)
        }
        ArchiveFormat::Zip => unreachable!(),
    };

    let mut tar = tar::Builder::new(writer);

    for path in paths {
        if cancel.load(Ordering::Relaxed) {
            // Clean up partial file
            drop(tar);
            let _ = std::fs::remove_file(output);
            return Ok(());
        }
        // Skip symlinks at top level too
        if path
            .symlink_metadata()
            .map(|m| m.is_symlink())
            .unwrap_or(false)
        {
            continue;
        }
        let rel = path.strip_prefix(base_dir).unwrap_or(path);
        if path.is_dir() {
            append_dir_recursive(&mut tar, path, rel, done_bytes, cancel)?;
        } else {
            tar.append_path_with_name(path, rel)
                .with_context(|| format!("Failed to add {:?}", path))?;
            if let Ok(meta) = std::fs::metadata(path) {
                done_bytes.fetch_add(meta.len(), Ordering::Relaxed);
            }
        }
    }

    tar.finish().context("Failed to finalize tar archive")?;
    Ok(())
}

fn append_dir_recursive<W: Write>(
    tar: &mut tar::Builder<W>,
    dir: &Path,
    rel_base: &Path,
    done_bytes: &AtomicU64,
    cancel: &AtomicBool,
) -> Result<()> {
    tar.append_dir(rel_base, dir)
        .with_context(|| format!("Failed to add directory {:?}", dir))?;

    let mut entries: Vec<_> = std::fs::read_dir(dir)
        .with_context(|| format!("Failed to read directory {:?}", dir))?
        .filter_map(|e| e.ok())
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let path = entry.path();
        // Skip symlinks to prevent infinite loops
        if path
            .symlink_metadata()
            .map(|m| m.is_symlink())
            .unwrap_or(false)
        {
            continue;
        }
        let rel = rel_base.join(entry.file_name());
        if path.is_dir() {
            append_dir_recursive(tar, &path, &rel, done_bytes, cancel)?;
        } else {
            tar.append_path_with_name(&path, &rel)
                .with_context(|| format!("Failed to add {:?}", path))?;
            if let Ok(meta) = std::fs::metadata(&path) {
                done_bytes.fetch_add(meta.len(), Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

fn create_zip(
    paths: &[PathBuf],
    base_dir: &Path,
    output: &Path,
    done_bytes: &AtomicU64,
    cancel: &AtomicBool,
) -> Result<()> {
    let file = File::create(output).context("Failed to create zip file")?;
    let mut zip = zip::ZipWriter::new(BufWriter::new(file));
    let options = zip::write::SimpleFileOptions::default()
        .compression_method(zip::CompressionMethod::Deflated);

    for path in paths {
        if cancel.load(Ordering::Relaxed) {
            drop(zip);
            let _ = std::fs::remove_file(output);
            return Ok(());
        }
        // Skip symlinks at top level too
        if path
            .symlink_metadata()
            .map(|m| m.is_symlink())
            .unwrap_or(false)
        {
            continue;
        }
        let rel = path.strip_prefix(base_dir).unwrap_or(path);
        if path.is_dir() {
            zip_dir_recursive(&mut zip, path, rel, options, done_bytes, cancel)?;
        } else {
            let name = rel.to_string_lossy().to_string();
            zip.start_file(&name, options)
                .with_context(|| format!("Failed to add {:?} to zip", path))?;
            let mut f = File::open(path)?;
            let copied = io::copy(&mut f, &mut zip)?;
            done_bytes.fetch_add(copied, Ordering::Relaxed);
        }
    }

    zip.finish().context("Failed to finalize zip")?;
    Ok(())
}

fn zip_dir_recursive<W: Write + io::Seek>(
    zip: &mut zip::ZipWriter<W>,
    dir: &Path,
    rel_base: &Path,
    options: zip::write::SimpleFileOptions,
    done_bytes: &AtomicU64,
    cancel: &AtomicBool,
) -> Result<()> {
    let dir_name = format!("{}/", rel_base.to_string_lossy());
    zip.add_directory(&dir_name, options)
        .with_context(|| format!("Failed to add directory {:?}", dir))?;

    let mut entries: Vec<_> = std::fs::read_dir(dir)?.filter_map(|e| e.ok()).collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        if cancel.load(Ordering::Relaxed) {
            return Ok(());
        }
        let path = entry.path();
        // Skip symlinks to prevent infinite loops
        if path
            .symlink_metadata()
            .map(|m| m.is_symlink())
            .unwrap_or(false)
        {
            continue;
        }
        let rel = rel_base.join(entry.file_name());
        if path.is_dir() {
            zip_dir_recursive(zip, &path, &rel, options, done_bytes, cancel)?;
        } else {
            let name = rel.to_string_lossy().to_string();
            zip.start_file(&name, options)?;
            let mut f = File::open(&path)?;
            let copied = io::copy(&mut f, zip)?;
            done_bytes.fetch_add(copied, Ordering::Relaxed);
        }
    }
    Ok(())
}
