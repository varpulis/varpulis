class Varpulis < Formula
  desc "Modern streaming analytics engine with advanced pattern detection"
  homepage "https://github.com/varpulis/varpulis"
  version "0.4.0"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_intel do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-macos-x86_64"
      sha256 "PLACEHOLDER_MACOS_X86_64_SHA256"

      def install
        bin.install "varpulis-macos-x86_64" => "varpulis"

        # Download and install LSP binary
        lsp_url = "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-lsp-x86_64-apple-darwin"
        curl_download lsp_url, to: buildpath/"varpulis-lsp"
        bin.install "varpulis-lsp"
      end
    end

    on_arm do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-macos-aarch64"
      sha256 "PLACEHOLDER_MACOS_AARCH64_SHA256"

      def install
        bin.install "varpulis-macos-aarch64" => "varpulis"

        # Download and install LSP binary
        lsp_url = "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-lsp-aarch64-apple-darwin"
        curl_download lsp_url, to: buildpath/"varpulis-lsp"
        bin.install "varpulis-lsp"
      end
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-linux-x86_64"
      sha256 "PLACEHOLDER_LINUX_X86_64_SHA256"

      def install
        bin.install "varpulis-linux-x86_64" => "varpulis"

        # Download and install LSP binary
        lsp_url = "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-lsp-x86_64-unknown-linux-gnu"
        curl_download lsp_url, to: buildpath/"varpulis-lsp"
        bin.install "varpulis-lsp"
      end
    end

    on_arm do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-linux-aarch64"
      sha256 "PLACEHOLDER_LINUX_AARCH64_SHA256"

      def install
        bin.install "varpulis-linux-aarch64" => "varpulis"

        # Download and install LSP binary
        lsp_url = "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-lsp-aarch64-unknown-linux-gnu"
        curl_download lsp_url, to: buildpath/"varpulis-lsp"
        bin.install "varpulis-lsp"
      end
    end
  end

  test do
    assert_match "varpulis #{version}", shell_output("#{bin}/varpulis --version")
  end
end
