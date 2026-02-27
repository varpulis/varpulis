class Varpulis < Formula
  desc "Modern streaming analytics engine with advanced pattern detection"
  homepage "https://github.com/varpulis/varpulis"
  version "0.4.1"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_intel do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-macos-x86_64"
      sha256 "PLACEHOLDER"
    end

    on_arm do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-macos-aarch64"
      sha256 "PLACEHOLDER"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-linux-x86_64"
      sha256 "PLACEHOLDER"
    end

    on_arm do
      url "https://github.com/varpulis/varpulis/releases/download/v#{version}/varpulis-linux-aarch64"
      sha256 "PLACEHOLDER"
    end
  end

  def install
    if OS.mac? && Hardware::CPU.intel?
      bin.install "varpulis-macos-x86_64" => "varpulis"
    elsif OS.mac? && Hardware::CPU.arm?
      bin.install "varpulis-macos-aarch64" => "varpulis"
    elsif OS.linux? && Hardware::CPU.intel?
      bin.install "varpulis-linux-x86_64" => "varpulis"
    elsif OS.linux? && Hardware::CPU.arm?
      bin.install "varpulis-linux-aarch64" => "varpulis"
    end
  end

  test do
    assert_match "varpulis #{version}", shell_output("#{bin}/varpulis --version")
  end
end
