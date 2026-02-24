#!/bin/sh
# Varpulis installer script
# Usage: curl -sSf https://raw.githubusercontent.com/varpulis/varpulis/main/scripts/install.sh | sh
#
# Environment variables:
#   VARPULIS_VERSION     - Version to install (default: 0.4.0)
#   VARPULIS_INSTALL_DIR - Installation directory (default: /usr/local/bin or ~/.local/bin)
#   VARPULIS_NO_LSP      - Set to 1 to skip LSP binary installation

set -e

VERSION="${VARPULIS_VERSION:-0.4.0}"
REPO="varpulis/varpulis"
BASE_URL="https://github.com/${REPO}/releases/download/v${VERSION}"

# ============================================
# Helpers
# ============================================

say() {
    printf '%s\n' "$1"
}

err() {
    say "error: $1" >&2
    exit 1
}

need() {
    if ! command -v "$1" > /dev/null 2>&1; then
        err "need '$1' (command not found)"
    fi
}

# ============================================
# Detect OS and architecture
# ============================================

detect_platform() {
    OS="$(uname -s)"
    ARCH="$(uname -m)"

    case "$OS" in
        Linux)  OS="linux" ;;
        Darwin) OS="darwin" ;;
        *)      err "unsupported OS: $OS" ;;
    esac

    case "$ARCH" in
        x86_64|amd64) ARCH="x86_64" ;;
        aarch64|arm64) ARCH="aarch64" ;;
        *)             err "unsupported architecture: $ARCH" ;;
    esac
}

# Map to release binary names
binary_name() {
    echo "varpulis-${OS}-${ARCH}"
}

lsp_binary_name() {
    case "${OS}-${ARCH}" in
        linux-x86_64)   echo "varpulis-lsp-x86_64-unknown-linux-gnu" ;;
        linux-aarch64)  echo "varpulis-lsp-aarch64-unknown-linux-gnu" ;;
        darwin-x86_64)  echo "varpulis-lsp-x86_64-apple-darwin" ;;
        darwin-aarch64) echo "varpulis-lsp-aarch64-apple-darwin" ;;
        *)              err "unsupported platform for LSP: ${OS}-${ARCH}" ;;
    esac
}

# ============================================
# Download helper (curl with wget fallback)
# ============================================

download() {
    url="$1"
    dest="$2"

    if command -v curl > /dev/null 2>&1; then
        curl -fsSL "$url" -o "$dest"
    elif command -v wget > /dev/null 2>&1; then
        wget -q "$url" -O "$dest"
    else
        err "need 'curl' or 'wget' to download files"
    fi
}

# ============================================
# Checksum verification
# ============================================

verify_checksum() {
    file="$1"
    expected="$2"

    if command -v sha256sum > /dev/null 2>&1; then
        actual="$(sha256sum "$file" | cut -d' ' -f1)"
    elif command -v shasum > /dev/null 2>&1; then
        actual="$(shasum -a 256 "$file" | cut -d' ' -f1)"
    else
        say "warning: no sha256sum or shasum found, skipping checksum verification"
        return 0
    fi

    if [ "$actual" != "$expected" ]; then
        err "checksum mismatch for $(basename "$file"): expected $expected, got $actual"
    fi
}

# ============================================
# Determine install directory
# ============================================

determine_install_dir() {
    if [ -n "$VARPULIS_INSTALL_DIR" ]; then
        INSTALL_DIR="$VARPULIS_INSTALL_DIR"
        return
    fi

    # Try /usr/local/bin first (may need sudo)
    if [ -w /usr/local/bin ]; then
        INSTALL_DIR="/usr/local/bin"
    elif [ "$(id -u)" = "0" ]; then
        INSTALL_DIR="/usr/local/bin"
    else
        # Fall back to user-local directory
        INSTALL_DIR="${HOME}/.local/bin"
        mkdir -p "$INSTALL_DIR"
    fi
}

# ============================================
# Check if install dir needs sudo
# ============================================

maybe_sudo() {
    if [ -w "$INSTALL_DIR" ]; then
        "$@"
    elif command -v sudo > /dev/null 2>&1; then
        say "Installing to $INSTALL_DIR (requires sudo)"
        sudo "$@"
    else
        err "cannot write to $INSTALL_DIR and sudo is not available"
    fi
}

# ============================================
# Main installation
# ============================================

main() {
    say "Varpulis installer v${VERSION}"
    say ""

    detect_platform
    say "Platform: ${OS}/${ARCH}"

    BIN_NAME="$(binary_name)"
    LSP_BIN_NAME="$(lsp_binary_name)"

    determine_install_dir
    say "Install dir: ${INSTALL_DIR}"

    # Create temp directory with cleanup trap
    TMPDIR="$(mktemp -d)"
    trap 'rm -rf "$TMPDIR"' EXIT

    # Download checksums
    say ""
    say "Downloading checksums..."
    download "${BASE_URL}/checksums.txt" "${TMPDIR}/checksums.txt"

    # Download main binary
    say "Downloading ${BIN_NAME}..."
    download "${BASE_URL}/${BIN_NAME}" "${TMPDIR}/${BIN_NAME}"

    # Verify checksum
    EXPECTED="$(grep "${BIN_NAME}$" "${TMPDIR}/checksums.txt" | cut -d' ' -f1 || true)"
    if [ -n "$EXPECTED" ]; then
        say "Verifying checksum..."
        verify_checksum "${TMPDIR}/${BIN_NAME}" "$EXPECTED"
        say "Checksum OK"
    else
        say "warning: no checksum found for ${BIN_NAME}, skipping verification"
    fi

    # Install main binary
    chmod +x "${TMPDIR}/${BIN_NAME}"
    maybe_sudo cp "${TMPDIR}/${BIN_NAME}" "${INSTALL_DIR}/varpulis"
    say "Installed varpulis to ${INSTALL_DIR}/varpulis"

    # Install LSP binary (unless opted out)
    if [ "${VARPULIS_NO_LSP:-0}" != "1" ]; then
        say ""
        say "Downloading ${LSP_BIN_NAME}..."
        if download "${BASE_URL}/${LSP_BIN_NAME}" "${TMPDIR}/${LSP_BIN_NAME}" 2>/dev/null; then
            LSP_EXPECTED="$(grep "${LSP_BIN_NAME}$" "${TMPDIR}/checksums.txt" | cut -d' ' -f1 || true)"
            if [ -n "$LSP_EXPECTED" ]; then
                verify_checksum "${TMPDIR}/${LSP_BIN_NAME}" "$LSP_EXPECTED"
            fi
            chmod +x "${TMPDIR}/${LSP_BIN_NAME}"
            maybe_sudo cp "${TMPDIR}/${LSP_BIN_NAME}" "${INSTALL_DIR}/varpulis-lsp"
            say "Installed varpulis-lsp to ${INSTALL_DIR}/varpulis-lsp"
        else
            say "LSP binary not available for this platform, skipping"
        fi
    fi

    # Check PATH
    say ""
    case ":${PATH}:" in
        *":${INSTALL_DIR}:"*) ;;
        *)
            say "NOTE: ${INSTALL_DIR} is not in your PATH."
            say "Add it with:"
            say ""
            say "  export PATH=\"${INSTALL_DIR}:\$PATH\""
            say ""
            say "Or add that line to your shell profile (~/.bashrc, ~/.zshrc, etc.)"
            say ""
            ;;
    esac

    # Success message
    say "Installation complete!"
    say ""
    say "Next steps:"
    say "  varpulis --version          # Verify installation"
    say "  varpulis run -f hello.vpl   # Run a VPL pipeline"
    say ""
    say "Get started:"
    say "  https://github.com/${REPO}/tree/main/starters"
    say "  https://github.com/${REPO}/tree/main/docs"
}

main "$@"
