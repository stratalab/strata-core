class Strata < Formula
  desc "Production-grade embedded database for AI agents"
  homepage "https://stratadb.org"
  version "0.6.0"
  license "Apache-2.0"

  on_macos do
    on_arm do
      url "https://github.com/strata-ai-labs/strata-core/releases/download/v#{version}/strata-v#{version}-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER"
    end
    on_intel do
      url "https://github.com/strata-ai-labs/strata-core/releases/download/v#{version}/strata-v#{version}-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER"
    end
  end

  on_linux do
    on_intel do
      url "https://github.com/strata-ai-labs/strata-core/releases/download/v#{version}/strata-v#{version}-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER"
    end
  end

  def install
    bin.install "strata"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/strata --version")
  end
end
