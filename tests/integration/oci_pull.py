"""Download and unpack a container image's filesystem without a container runtime.

The CloudBeaver tier needs CloudBeaver's server distribution. Upstream ships no
standalone archive -- its GitHub releases carry source only -- so the built
server exists solely inside a container image. This container has no docker,
no podman, and no daemon socket, and cannot install one.

A registry serves image layers as ordinary gzipped tarballs over HTTPS, so the
rootfs can be reconstructed by fetching and extracting them in order, which is
all this module does. It is not a container runtime: nothing is namespaced or
executed here, the files are just unpacked so the JDK already in the toolchain
can run the application inside.

Pinning is by content digest. The manifest is requested by its sha256 rather
than by tag, and every blob is verified against the digest the manifest lists,
so a cold rebuild either reproduces the exact same bytes or fails loudly -- the
same guarantee the sha256-pinned tarballs in setup_toolchain.sh give.
"""
import gzip
import hashlib
import json
import os
import shutil
import tarfile
import urllib.request

REGISTRY = "https://registry-1.docker.io"
AUTH_URL = "https://auth.docker.io/token"

# Manifest media types to accept. The image may be published as either an OCI
# manifest or a Docker v2 manifest, and asking for both avoids a redirect to a
# format this module does not parse.
_MANIFEST_ACCEPT = ", ".join([
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])

# Layer entries whose name starts with this marker delete a path from the
# layers below rather than adding a file. Honouring them keeps the assembled
# rootfs equal to what a runtime would mount.
_WHITEOUT_PREFIX = ".wh."
_OPAQUE_WHITEOUT = ".wh..wh..opq"

# Read blobs in chunks so a several-hundred-megabyte layer never has to be held
# in memory in full.
_CHUNK_BYTES = 1024 * 1024


def _fetch_pull_token(repository):
    """Get an anonymous pull token for one repository.

    Args:
        repository: The image repository, for example "dbeaver/cloudbeaver".

    Returns:
        A bearer token string valid for pulling that repository.
    """
    url = f"{AUTH_URL}?service=registry.docker.io&scope=repository:{repository}:pull"
    with urllib.request.urlopen(url, timeout=60) as response:
        return json.load(response)["token"]


def _request(url, token, accept=None, timeout=600):
    """Open an authenticated registry request.

    Args:
        url: Absolute URL to fetch.
        token: Bearer token from _fetch_pull_token.
        accept: Optional Accept header value.
        timeout: Socket timeout in seconds.

    Returns:
        The open response object; the caller closes it.
    """
    request = urllib.request.Request(url)
    request.add_header("Authorization", f"Bearer {token}")
    if accept:
        request.add_header("Accept", accept)
    return urllib.request.urlopen(request, timeout=timeout)


def fetch_manifest(repository, digest):
    """Fetch and verify one image manifest by its content digest.

    Args:
        repository: The image repository.
        digest: The manifest's "sha256:..." digest.

    Returns:
        The parsed manifest document.

    Raises:
        ValueError: If the bytes returned do not hash to the requested digest,
            which would mean the registry served something other than what was
            pinned.
    """
    token = _fetch_pull_token(repository)
    url = f"{REGISTRY}/v2/{repository}/manifests/{digest}"
    with _request(url, token, accept=_MANIFEST_ACCEPT) as response:
        raw = response.read()
    actual = "sha256:" + hashlib.sha256(raw).hexdigest()
    if actual != digest:
        raise ValueError(f"manifest digest mismatch: wanted {digest}, got {actual}")
    return json.loads(raw)


def download_blob(repository, digest, destination):
    """Download one blob, verifying it against its digest.

    Skips the download when the destination already holds bytes with the right
    digest, so re-running the setup script does not refetch hundreds of
    megabytes.

    Args:
        repository: The image repository.
        digest: The blob's "sha256:..." digest.
        destination: Path to write the blob to.

    Raises:
        ValueError: If the downloaded bytes do not match the digest.
    """
    if os.path.exists(destination) and _file_digest(destination) == digest:
        return

    token = _fetch_pull_token(repository)
    url = f"{REGISTRY}/v2/{repository}/blobs/{digest}"
    digester = hashlib.sha256()
    with _request(url, token) as response, open(destination, "wb") as handle:
        while True:
            chunk = response.read(_CHUNK_BYTES)
            if not chunk:
                break
            digester.update(chunk)
            handle.write(chunk)

    actual = "sha256:" + digester.hexdigest()
    if actual != digest:
        os.remove(destination)
        raise ValueError(f"blob digest mismatch: wanted {digest}, got {actual}")


def _file_digest(path):
    """Return a file's "sha256:..." digest, reading it in chunks."""
    digester = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(_CHUNK_BYTES), b""):
            digester.update(chunk)
    return "sha256:" + digester.hexdigest()


def _apply_whiteouts(members, root):
    """Delete paths a layer marks as removed, returning the members to extract.

    Args:
        members: The tar members of one layer.
        root: The directory the rootfs is being assembled in.

    Returns:
        The members that should actually be extracted.
    """
    keep = []
    for member in members:
        name = os.path.basename(member.name)
        if name == _OPAQUE_WHITEOUT:
            # Everything below this directory from lower layers is hidden.
            target = os.path.join(root, os.path.dirname(member.name))
            if os.path.isdir(target):
                shutil.rmtree(target, ignore_errors=True)
                os.makedirs(target, exist_ok=True)
            continue
        if name.startswith(_WHITEOUT_PREFIX):
            removed = os.path.join(
                root, os.path.dirname(member.name), name[len(_WHITEOUT_PREFIX):]
            )
            if os.path.isdir(removed) and not os.path.islink(removed):
                shutil.rmtree(removed, ignore_errors=True)
            elif os.path.lexists(removed):
                os.remove(removed)
            continue
        keep.append(member)
    return keep


def extract_layer(archive_path, root):
    """Extract one gzipped layer tarball over the assembled rootfs.

    File modes are preserved: the image's launcher scripts and its bundled JRE
    are only usable with their execute bits intact. Ownership is not applied,
    which tarfile already skips when not running as root.

    Args:
        archive_path: Path to the layer blob.
        root: Directory to extract into.
    """
    with gzip.open(archive_path, "rb") as raw, tarfile.open(fileobj=raw, mode="r|") as tar:
        # A streaming tar cannot be rewound, so members are materialised once
        # and whiteouts applied against that list.
        members = list(tar)
    keep = {member.name for member in _apply_whiteouts(members, root)}

    def selected(tar):
        """Yield the members to extract, clearing any path they replace.

        A layer may overwrite a file an earlier layer wrote read-only (the
        JRE's class-data archive is one), and tarfile cannot open such a file
        for writing. Unlinking first makes replacement work the way stacking
        layers in a runtime would.
        """
        for member in tar:
            if member.name not in keep:
                continue
            target = os.path.join(root, member.name)
            if os.path.lexists(target) and not os.path.isdir(target):
                os.remove(target)
            yield member

    with gzip.open(archive_path, "rb") as raw, tarfile.open(fileobj=raw, mode="r|") as tar:
        # extractall defers directory permissions to the end, so a directory
        # extracted read-only cannot block the entries that follow it. Layers
        # legitimately contain symlinks and hardlinks, which the stricter
        # "data" filter rejects; "tar" still refuses paths that escape the
        # destination.
        tar.extractall(path=root, members=selected(tar), filter="tar")


def pull_rootfs(repository, manifest_digest, blob_cache, root):
    """Assemble an image's filesystem into a directory.

    Args:
        repository: The image repository, for example "dbeaver/cloudbeaver".
        manifest_digest: The pinned "sha256:..." digest of the platform
            manifest to pull.
        blob_cache: Directory to keep downloaded layer blobs in.
        root: Directory to assemble the rootfs in.

    Returns:
        The number of layers applied.
    """
    manifest = fetch_manifest(repository, manifest_digest)
    os.makedirs(blob_cache, exist_ok=True)
    os.makedirs(root, exist_ok=True)

    for index, layer in enumerate(manifest["layers"]):
        digest = layer["digest"]
        blob_path = os.path.join(blob_cache, digest.replace(":", "_"))
        print(f"  layer {index + 1}/{len(manifest['layers'])} {digest[:19]}")
        download_blob(repository, digest, blob_path)
        extract_layer(blob_path, root)

    return len(manifest["layers"])
