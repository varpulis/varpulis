//! Security module for Varpulis CLI
//!
//! Provides path validation, authentication helpers, and security utilities.

use std::path::{Path, PathBuf};

/// Error types for security operations
#[derive(Debug, Clone, PartialEq)]
pub enum SecurityError {
    /// Path is outside the allowed working directory
    PathTraversal { path: String },
    /// Path does not exist or is inaccessible
    InvalidPath { path: String, reason: String },
    /// Working directory is invalid
    InvalidWorkdir { path: String, reason: String },
    /// Authentication failed
    AuthenticationFailed { reason: String },
    /// Rate limit exceeded
    RateLimitExceeded { ip: String },
}

impl std::fmt::Display for SecurityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityError::PathTraversal { .. } => {
                // Generic message to avoid information disclosure
                write!(f, "Access denied: path outside allowed directory")
            }
            SecurityError::InvalidPath { reason, .. } => {
                write!(f, "Invalid path: {}", reason)
            }
            SecurityError::InvalidWorkdir { path, reason } => {
                write!(f, "Invalid workdir '{}': {}", path, reason)
            }
            SecurityError::AuthenticationFailed { reason } => {
                write!(f, "Authentication failed: {}", reason)
            }
            SecurityError::RateLimitExceeded { ip } => {
                write!(f, "Rate limit exceeded for IP: {}", ip)
            }
        }
    }
}

impl std::error::Error for SecurityError {}

/// Result type for security operations
pub type SecurityResult<T> = Result<T, SecurityError>;

/// Validate that a path is within the allowed working directory.
///
/// This function prevents path traversal attacks by:
/// 1. Converting the path to absolute (relative to workdir if not absolute)
/// 2. Canonicalizing to resolve `..`, `.`, and symlinks
/// 3. Verifying the canonical path starts with the canonical workdir
///
/// # Arguments
/// * `path` - The path to validate (can be relative or absolute)
/// * `workdir` - The allowed working directory
///
/// # Returns
/// * `Ok(PathBuf)` - The canonical, validated path
/// * `Err(SecurityError)` - If the path is invalid or outside workdir
///
/// # Examples
/// ```
/// use std::path::PathBuf;
/// use varpulis_cli::security::validate_path;
///
/// let workdir = std::env::current_dir().unwrap();
/// // Valid path within workdir
/// let result = validate_path("src/main.rs", &workdir);
/// // Note: Result depends on whether the file exists
/// ```
pub fn validate_path(path: &str, workdir: &Path) -> SecurityResult<PathBuf> {
    let requested = PathBuf::from(path);

    // Resolve to absolute path
    let absolute = if requested.is_absolute() {
        requested
    } else {
        workdir.join(&requested)
    };

    // Canonicalize workdir first to ensure it's valid
    let workdir_canonical = workdir
        .canonicalize()
        .map_err(|e| SecurityError::InvalidWorkdir {
            path: workdir.display().to_string(),
            reason: e.to_string(),
        })?;

    // Canonicalize to resolve .. and symlinks
    let canonical = absolute
        .canonicalize()
        .map_err(|e| SecurityError::InvalidPath {
            path: path.to_string(),
            reason: e.to_string(),
        })?;

    // Ensure the canonical path starts with workdir
    if !canonical.starts_with(&workdir_canonical) {
        return Err(SecurityError::PathTraversal {
            path: path.to_string(),
        });
    }

    Ok(canonical)
}

/// Validate and canonicalize a workdir path.
///
/// # Arguments
/// * `workdir` - Optional workdir path, defaults to current directory
///
/// # Returns
/// * `Ok(PathBuf)` - The canonical workdir path
/// * `Err(SecurityError)` - If the workdir is invalid
pub fn validate_workdir(workdir: Option<PathBuf>) -> SecurityResult<PathBuf> {
    let path =
        workdir.unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));

    path.canonicalize()
        .map_err(|e| SecurityError::InvalidWorkdir {
            path: path.display().to_string(),
            reason: e.to_string(),
        })
}

/// Check if a path contains potentially dangerous patterns.
///
/// This is a fast pre-check before canonicalization.
/// It detects obvious path traversal attempts.
///
/// # Arguments
/// * `path` - The path string to check
///
/// # Returns
/// * `true` if the path looks suspicious
/// * `false` if the path looks safe (but still needs full validation)
pub fn is_suspicious_path(path: &str) -> bool {
    // Check for obvious path traversal patterns
    path.contains("..")
        || path.contains("//")
        || path.starts_with('/')
        || path.contains('\0')
        || path.contains("~")
}

/// Sanitize a filename by removing dangerous characters.
///
/// This is useful for user-provided filenames in file creation.
///
/// # Arguments
/// * `filename` - The filename to sanitize
///
/// # Returns
/// * Sanitized filename safe for use in file operations
pub fn sanitize_filename(filename: &str) -> String {
    filename
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '.' || *c == '_' || *c == '-')
        .collect::<String>()
        .trim_start_matches('.')
        .to_string()
}

/// Generate a simple UUID-like identifier.
///
/// Uses timestamp-based generation for simplicity.
/// Not cryptographically secure, suitable for request IDs.
///
/// # Returns
/// * A hex string identifier
pub fn generate_request_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| std::time::Duration::from_secs(0));

    format!("{:x}{:x}", duration.as_secs(), duration.subsec_nanos())
}

// =============================================================================
// Tests - TDD approach: tests written first!
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    // -------------------------------------------------------------------------
    // validate_path tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_path_simple_relative() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path();

        // Create a file to validate
        let test_file = workdir.join("test.txt");
        fs::write(&test_file, "test content").expect("Failed to write test file");

        let result = validate_path("test.txt", workdir);
        assert!(result.is_ok());
        assert_eq!(
            result.expect("should succeed"),
            test_file.canonicalize().expect("should canonicalize")
        );
    }

    #[test]
    fn test_validate_path_nested_relative() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path();

        // Create nested directory and file
        let subdir = workdir.join("subdir");
        fs::create_dir(&subdir).expect("Failed to create subdir");
        let test_file = subdir.join("nested.txt");
        fs::write(&test_file, "nested content").expect("Failed to write test file");

        let result = validate_path("subdir/nested.txt", workdir);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_path_traversal_blocked() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path().join("subdir");
        fs::create_dir(&workdir).expect("Failed to create workdir");

        // Try to escape with ..
        let result = validate_path("../escape.txt", &workdir);
        assert!(result.is_err());

        if let Err(SecurityError::PathTraversal { .. }) = result {
            // Expected
        } else if let Err(SecurityError::InvalidPath { .. }) = result {
            // Also acceptable - file doesn't exist
        } else {
            panic!("Expected PathTraversal or InvalidPath error");
        }
    }

    #[test]
    fn test_validate_path_absolute_outside_workdir() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path().join("allowed");
        fs::create_dir(&workdir).expect("Failed to create workdir");

        // Create file outside workdir
        let outside_file = temp_dir.path().join("outside.txt");
        fs::write(&outside_file, "outside").expect("Failed to write");

        // Try to access with absolute path
        let result = validate_path(outside_file.to_str().expect("should convert"), &workdir);
        assert!(result.is_err());

        match result {
            Err(SecurityError::PathTraversal { .. }) => {}
            _ => panic!("Expected PathTraversal error"),
        }
    }

    #[test]
    fn test_validate_path_nonexistent_file() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path();

        let result = validate_path("nonexistent.txt", workdir);
        assert!(result.is_err());

        match result {
            Err(SecurityError::InvalidPath { reason, .. }) => {
                assert!(reason.contains("No such file") || reason.contains("cannot find"));
            }
            _ => panic!("Expected InvalidPath error"),
        }
    }

    #[test]
    fn test_validate_path_invalid_workdir() {
        let result = validate_path("test.txt", Path::new("/nonexistent/workdir/xyz123"));
        assert!(result.is_err());

        match result {
            Err(SecurityError::InvalidWorkdir { .. }) => {}
            _ => panic!("Expected InvalidWorkdir error"),
        }
    }

    #[test]
    fn test_validate_path_dot_dot_in_middle() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let workdir = temp_dir.path();

        // Create structure: workdir/a/b/file.txt
        let dir_a = workdir.join("a");
        let dir_b = dir_a.join("b");
        fs::create_dir_all(&dir_b).expect("Failed to create dirs");
        let file = dir_b.join("file.txt");
        fs::write(&file, "content").expect("Failed to write");

        // Path that goes down then up but stays in workdir: a/b/../b/file.txt
        let result = validate_path("a/b/../b/file.txt", workdir);
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------------
    // validate_workdir tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_validate_workdir_none_uses_current() {
        let result = validate_workdir(None);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_workdir_valid_path() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let result = validate_workdir(Some(temp_dir.path().to_path_buf()));
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_workdir_invalid_path() {
        let result = validate_workdir(Some(PathBuf::from("/nonexistent/path/xyz123")));
        assert!(result.is_err());

        match result {
            Err(SecurityError::InvalidWorkdir { .. }) => {}
            _ => panic!("Expected InvalidWorkdir error"),
        }
    }

    // -------------------------------------------------------------------------
    // is_suspicious_path tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_is_suspicious_path_double_dot() {
        assert!(is_suspicious_path("../etc/passwd"));
        assert!(is_suspicious_path("foo/../bar"));
        assert!(is_suspicious_path("foo/bar/.."));
    }

    #[test]
    fn test_is_suspicious_path_double_slash() {
        assert!(is_suspicious_path("foo//bar"));
        assert!(is_suspicious_path("//etc/passwd"));
    }

    #[test]
    fn test_is_suspicious_path_absolute() {
        assert!(is_suspicious_path("/etc/passwd"));
        assert!(is_suspicious_path("/home/user/file"));
    }

    #[test]
    fn test_is_suspicious_path_null_byte() {
        assert!(is_suspicious_path("file.txt\0.jpg"));
    }

    #[test]
    fn test_is_suspicious_path_tilde() {
        assert!(is_suspicious_path("~/secrets"));
        assert!(is_suspicious_path("~user/file"));
    }

    #[test]
    fn test_is_suspicious_path_safe_paths() {
        assert!(!is_suspicious_path("file.txt"));
        assert!(!is_suspicious_path("dir/file.txt"));
        assert!(!is_suspicious_path("a/b/c/d.txt"));
        assert!(!is_suspicious_path("my-file_name.rs"));
    }

    // -------------------------------------------------------------------------
    // sanitize_filename tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_sanitize_filename_safe() {
        assert_eq!(sanitize_filename("file.txt"), "file.txt");
        assert_eq!(sanitize_filename("my_file-2.rs"), "my_file-2.rs");
    }

    #[test]
    fn test_sanitize_filename_removes_slashes() {
        assert_eq!(sanitize_filename("../etc/passwd"), "etcpasswd");
        assert_eq!(sanitize_filename("foo/bar"), "foobar");
    }

    #[test]
    fn test_sanitize_filename_removes_special_chars() {
        assert_eq!(sanitize_filename("file<>:\"|?*.txt"), "file.txt");
        assert_eq!(sanitize_filename("hello\0world"), "helloworld");
    }

    #[test]
    fn test_sanitize_filename_removes_leading_dots() {
        assert_eq!(sanitize_filename(".htaccess"), "htaccess");
        assert_eq!(sanitize_filename("...test"), "test");
    }

    #[test]
    fn test_sanitize_filename_preserves_internal_dots() {
        assert_eq!(sanitize_filename("file.name.txt"), "file.name.txt");
    }

    // -------------------------------------------------------------------------
    // generate_request_id tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_generate_request_id_not_empty() {
        let id = generate_request_id();
        assert!(!id.is_empty());
    }

    #[test]
    fn test_generate_request_id_is_hex() {
        let id = generate_request_id();
        assert!(id.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_generate_request_id_unique() {
        let id1 = generate_request_id();
        std::thread::sleep(std::time::Duration::from_millis(1));
        let id2 = generate_request_id();
        // Not guaranteed unique but very likely different
        // In practice, subsec_nanos should differ
        assert_ne!(id1, id2);
    }

    // -------------------------------------------------------------------------
    // SecurityError Display tests
    // -------------------------------------------------------------------------

    #[test]
    fn test_security_error_display_path_traversal() {
        let err = SecurityError::PathTraversal {
            path: "../etc/passwd".to_string(),
        };
        let msg = format!("{}", err);
        // Should NOT reveal the actual path
        assert!(!msg.contains("passwd"));
        assert!(msg.contains("Access denied"));
    }

    #[test]
    fn test_security_error_display_invalid_path() {
        let err = SecurityError::InvalidPath {
            path: "test.txt".to_string(),
            reason: "file not found".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Invalid path"));
        assert!(msg.contains("file not found"));
    }

    #[test]
    fn test_security_error_display_auth_failed() {
        let err = SecurityError::AuthenticationFailed {
            reason: "invalid token".to_string(),
        };
        let msg = format!("{}", err);
        assert!(msg.contains("Authentication failed"));
    }
}
