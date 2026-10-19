# Releasing

## Every release

1. Set the new version in both `VERSION` in `mox.py` and `version` in `pyproject.toml`.
2. Run the tests: `python3 -m unittest discover -s tests`.
3. Commit, then tag and push:

   ```bash
   git tag v1.3
   git push origin main v1.3
   ```

The `release` workflow checks that the tag matches both version numbers, runs the tests, builds the package, publishes it to PyPI and makes a GitHub release with `mox.py` attached.

Then update the AUR and Homebrew packages, below, if you publish there.

## Once: PyPI

The workflow publishes with PyPI's trusted publishing, so no API token lives in the repository.

1. Make an account on [pypi.org](https://pypi.org) and turn on two-factor authentication.
2. Go to *Your account → Publishing* and add a pending publisher:
   - PyPI project name: `moxdl`
   - Owner: `moxart`
   - Repository name: `moxdl`
   - Workflow name: `release.yml`
   - Environment name: `pypi`

   Enter the repository and workflow as bare names, not `moxart/moxdl` or `.github/workflows/release.yml`. If any field is off, the `pypi` job fails with `invalid-publisher` and prints the claims GitHub sent; fix the publisher to match them, then use *Re-run failed jobs* on that run instead of tagging again.
3. On GitHub, open the repository's *Settings → Environments* and create an environment named `pypi`.

The first tag pushed after that creates the project on PyPI. From then on `pipx install moxdl` works, and the README can say so.

## Once per release: AUR

The first time, make an account on [aur.archlinux.org](https://aur.archlinux.org) and add your SSH key to it. Then, on an Arch machine:

```bash
git clone ssh://aur@aur.archlinux.org/moxdl.git aur-moxdl
cp packaging/aur/PKGBUILD aur-moxdl/
cd aur-moxdl
updpkgsums                         # puts the real sha256 of the tagged tarball in place of SKIP
makepkg -si                        # builds, runs the tests, installs; check that `mox --version` works
makepkg --printsrcinfo > .SRCINFO
git add PKGBUILD .SRCINFO && git commit -m "moxdl 1.3" && git push
```

For later releases, change `pkgver`, set `pkgrel=1`, and repeat from `updpkgsums`.

## Once per release: Homebrew

The first time, create a public repository named `homebrew-tap` under `moxart`. Then:

```bash
curl -fsSL https://github.com/moxart/moxdl/archive/refs/tags/v1.3.tar.gz | shasum -a 256
```

Put that sum and the new version in `packaging/homebrew/moxdl.rb`, copy the file to `Formula/moxdl.rb` in the tap repository, and push. Check it with `brew install moxart/tap/moxdl && brew test moxdl`.

## After the first release

Add the package managers to the install section of both READMEs:

```bash
pipx install moxdl
yay -S moxdl
brew install moxart/tap/moxdl
```
