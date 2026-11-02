"""
Centralised path-containment and archive-security logic.

Every path derived from user input must pass through ``assert_safe_path`` before
any filesystem operation. Every zip extracted for a user must be extracted via
``safe_unzip``. No other code should perform raw path manipulation without
going through these primitives.
"""

import os
import stat
import zipfile
from pathlib import Path, PurePosixPath


class PathEscapeError(ValueError):
    """Raised when a path would escape its permitted base directory."""


def assert_safe_path(base: Path, target: Path) -> None:
    """
    Verify that *target* is contained within *base*.

    Symlinks in *target* are resolved so that a link pointing outside *base*
    is caught. Raises ``PathEscapeError`` on any violation.
    """
    base_resolved = base.resolve()

    if not target.is_absolute():
        # Treat relative targets as relative to base
        candidate = (base_resolved / target).resolve()
    else:
        candidate = target.resolve()

    try:
        candidate.relative_to(base_resolved)
    except ValueError:
        raise PathEscapeError(
            f"Path escapes allowed base: {candidate!r} is not under {base_resolved!r}"
        )


def assert_safe_upload_filename(filename: str, *, allow_subdirs: bool = False) -> None:
    """
    Validate a filename supplied via multipart upload.

    Rejects:
    - Empty or missing filenames.
    - Absolute paths (``/foo`` or Windows ``C:\\foo``).
    - Path traversal via ``..`` components.
    - Directory separators when *allow_subdirs* is False (flat uploads only).

    Does not touch the filesystem — purely lexical.
    """
    if not filename:
        raise PathEscapeError("Empty filename in upload")

    # Normalise backslashes so Windows paths are caught uniformly
    normalised = filename.replace("\\", "/")

    if os.path.isabs(normalised):
        raise PathEscapeError(f"Absolute filename not permitted: {filename!r}")

    if not allow_subdirs and "/" in normalised:
        raise PathEscapeError(
            f"Directory separators not permitted in filename: {filename!r}"
        )

    for part in PurePosixPath(normalised).parts:
        if part == "..":
            raise PathEscapeError(f"Path traversal in filename: {filename!r}")


_MACOS_METADATA_DIR = "__MACOSX"


def is_skipped_member(name: str) -> bool:
    """True for upload members that are dropped rather than rejected.

    Any path component beginning with ``.`` (``.DS_Store``, ``.git``,
    ``._resource`` forks) and anything under ``__MACOSX/``. Finder-made zips
    almost always contain these, so failing the whole upload over them would
    break ordinary macOS users every time; they are never written, and the
    caller reports them separately from genuine rejections.

    Purely lexical, and deliberately *not* a security check: traversal,
    absolute paths and symlinks are rejected outright elsewhere.
    """
    if not name:
        return False
    normalised = name.replace("\\", "/")
    for part in PurePosixPath(normalised).parts:
        # "." and ".." are path navigation, not dotfiles. Treating them as
        # skippable would silently drop a traversal attempt instead of failing
        # the request, so they are left for the security checks to reject.
        if part in ("/", "", ".", ".."):
            continue
        if part == _MACOS_METADATA_DIR or part.startswith("."):
            return True
    return False


def assert_no_symlinks(root: Path) -> None:
    """Verify no symlink exists anywhere under *root*.

    Run after writing a staged upload and before moving it into place. The
    lexical checks cannot see a link that only exists once bytes are on disk,
    and a link created inside the staging area could otherwise redirect the
    subsequent move. Directories are checked too, since a linked directory
    would carry its whole subtree outside *root*.
    """
    if root.is_symlink():
        raise PathEscapeError(f"Staged upload root is a symlink: {root!r}")
    for entry in root.rglob("*"):
        if entry.is_symlink():
            raise PathEscapeError(
                f"Symlink not permitted in upload: {entry.relative_to(root)!r}"
            )


def safe_unzip_filtered(archive: Path, dest: Path) -> tuple[list[str], list[str]]:
    """Extract *archive* into *dest*, skipping macOS/dotfile members.

    Applies every check ``safe_unzip`` does — absolute paths, traversal and
    symlink members are all rejected before anything is written — but members
    matching :func:`is_skipped_member` are omitted from the extraction instead
    of causing a failure.

    Returns ``(extracted_names, skipped_names)``. Directory entries are not
    reported in either list.
    """
    dest_resolved = dest.resolve()
    extracted: list[str] = []
    skipped: list[str] = []

    with zipfile.ZipFile(archive, "r") as zf:
        members = []
        for member in zf.infolist():
            name = member.filename

            if os.path.isabs(name):
                raise PathEscapeError(f"Zip entry has absolute path: {name!r}")

            target = (dest_resolved / name).resolve()
            try:
                target.relative_to(dest_resolved)
            except ValueError:
                raise PathEscapeError(f"Zip entry escapes destination: {name!r}")

            unix_mode = member.external_attr >> 16
            if unix_mode != 0 and stat.S_ISLNK(unix_mode):
                raise PathEscapeError(f"Zip entry is a symlink: {name!r}")

            if member.is_dir():
                continue
            if is_skipped_member(name):
                skipped.append(name)
                continue
            members.append(member)

        # Every surviving entry is safe; extract only those.
        for member in members:
            zf.extract(member, dest)
            extracted.append(member.filename)

    return extracted, skipped


def safe_unzip(archive: Path, dest: Path) -> None:
    """
    Extract *archive* into *dest*, rejecting any entry that:

    - Has an absolute member path.
    - Would resolve outside *dest* via path traversal (``..`` components).
    - Is a Unix symlink (detected via the external_attr field).

    All member paths are checked before any extraction begins, so a malicious
    archive cannot partially extract before the violation is detected.
    """
    dest_resolved = dest.resolve()

    with zipfile.ZipFile(archive, "r") as zf:
        for member in zf.infolist():
            name = member.filename

            # Reject absolute paths
            if os.path.isabs(name):
                raise PathEscapeError(
                    f"Zip entry has absolute path: {name!r}"
                )

            # Reject path traversal
            target = (dest_resolved / name).resolve()
            try:
                target.relative_to(dest_resolved)
            except ValueError:
                raise PathEscapeError(
                    f"Zip entry escapes destination: {name!r}"
                )

            # Reject symlinks — Unix file mode is in the upper 16 bits of external_attr
            unix_mode = member.external_attr >> 16
            if unix_mode != 0 and stat.S_ISLNK(unix_mode):
                raise PathEscapeError(
                    f"Zip entry is a symlink: {name!r}"
                )

        # All entries are safe; extract
        zf.extractall(dest)
