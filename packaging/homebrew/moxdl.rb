# Goes in a tap repository named moxart/homebrew-tap, as Formula/moxdl.rb.
# Then: brew install moxart/tap/moxdl
class Moxdl < Formula
  include Language::Python::Shebang

  desc "Fast, resumable, multi-connection download manager for the terminal"
  homepage "https://github.com/moxart/moxdl"
  url "https://github.com/moxart/moxdl/archive/refs/tags/v1.4.tar.gz"
  sha256 "REPLACE_WITH_THE_SHA256_OF_THE_TARBALL"
  license "MIT"

  depends_on "python@3.13"

  def install
    rewrite_shebang detected_python_shebang, "mox.py"
    bin.install "mox.py" => "mox"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/mox --version")
  end
end
