"""
All methods here return `Iterator[FileProperties]`
"""

import os.path
from datetime import datetime, timezone
from typing import Iterator, Optional
from xml.etree.ElementTree import iterparse
from zipfile import ZipFile

from genutility.datetime import datetime_from_utc_timestamp, datetime_from_utc_timestamp_ns
from genutility.filesystem import FileProperties, scandir_rec
from genutility.hash import hash_filelike

from .utils import OpenFileOrUrl


def iter_dir(
    path: str,
    extra: bool = True,
    hashfunc: Optional[str] = None,
    dirs: bool = True,
    recurse_archives: bool = False,
    hash_from_meta: bool = True,
) -> Iterator[FileProperties]:

    """Returns correct device id and file inode for py > 3.5 on windows if `extras=True`"""

    for entry in scandir_rec(path, files=True, dirs=dirs, relative=True):

        relpath = entry.relpath.replace("\\", "/")
        abspath = entry.path

        if recurse_archives:
            try:
                for prop in iter_archive(entry.path, hashfunc=hashfunc, hash_from_meta=hash_from_meta):
                    prop.relpath = f"{relpath}:/{prop.relpath}"
                    prop.abspath = f"{abspath}:/{prop.relpath}"
                    yield prop
                continue
            except ValueError:
                pass

        if extra:
            stat = os.stat(entry.path)
        else:
            stat = entry.stat()

        modtime = datetime_from_utc_timestamp_ns(stat.st_mtime_ns)

        yield FileProperties(
            relpath,
            stat.st_size,
            entry.is_dir(),
            abspath,
            (stat.st_dev, stat.st_ino),
            modtime,
        )


def iter_archiveorg_xml(path: str, hashfunc: str = "sha1", dirs: Optional[bool] = None) -> Iterator[FileProperties]:

    assert hashfunc in ("sha1", "md5", "crc32")

    skip_formats = {"Metadata", "Archive BitTorrent", "Item Tile"}

    with OpenFileOrUrl(path) as fr:
        for event, element in iterparse(fr, ["end"]):
            if element.tag != "file":
                continue
            if element.get("source") != "original":
                continue
            if element.find("format").text in skip_formats:
                continue

            relpath = element.get("name")
            size = int(element.find("size").text)
            modtime = datetime_from_utc_timestamp(int(element.find("mtime").text))
            hash = element.find(hashfunc).text

            yield FileProperties(relpath, size, False, modtime=modtime, hash=hash)


def iter_gamedat_xml(path: str, hashfunc: str = "sha1", dirs: Optional[bool] = None) -> Iterator[FileProperties]:

    assert hashfunc in ("sha1", "md5", "crc32")

    hashfunc = {
        "crc32": "crc",
    }.get(hashfunc, hashfunc)

    with open(path) as fr:
        for event, element in iterparse(fr, ["end"]):
            if element.tag != "game":
                continue

            rom = element.find("rom")
            relpath = rom.get("name")
            size = int(rom.get("size"))
            hash = rom.get(hashfunc)
            yield FileProperties(relpath, size, False, hash=hash)


def _archive_hash(af, f, hashfunc: Optional[str], hash_from_meta: bool):
    if hash_from_meta:
        return f.CRC
    else:
        if hashfunc is None:
            return None
        else:
            with af.open(f, "r") as fr:
                return hash_filelike(fr, hashfunc)


def iter_zip(
    archivefile: str,
    topleveldir: Optional[str] = None,
    hashfunc: Optional[str] = "crc32",
    assume_utc: bool = False,
    dirs: Optional[bool] = None,
    hash_from_meta: bool = True,
) -> Iterator[FileProperties]:

    """If `topleveldir` is given, returned file paths will be relativ to this directory within the archive.

    If `assume_utc` is False (the default), it is assumed that local time is stored
    in the zip file. Otherwise it's assumed to be UTC.
    """

    if hash_from_meta and hashfunc is not None and hashfunc not in {"crc32"}:
        raise ValueError(f"Unsupported hash function: {hashfunc}")

    with ZipFile(archivefile, "r") as af:
        for f in af.infolist():

            if topleveldir:
                relpath = os.path.relpath(f.filename, topleveldir)
            else:
                relpath = f.filename

            year, month, day, hour, minute, second = f.date_time
            if assume_utc:
                # interpret as utc
                modtime = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
            else:
                # interpret as local time
                modtime = datetime(year, month, day, hour, minute, second).astimezone(timezone.utc)

            hash = _archive_hash(af, f, hashfunc, hash_from_meta)
            yield FileProperties(relpath, f.file_size, f.is_dir(), modtime=modtime, hash=hash)


def iter_rar(
    archivefile: str,
    topleveldir: Optional[str] = None,
    hashfunc: Optional[str] = "crc32",
    dirs: Optional[bool] = None,
    hash_from_meta: bool = True,
) -> Iterator[FileProperties]:

    from rarfile import RarFile

    if hash_from_meta and hashfunc is not None and hashfunc not in {"crc32"}:
        raise ValueError(f"Unsupported hash function: {hashfunc}")

    with RarFile(archivefile, "r") as af:
        for f in af.infolist():

            if topleveldir:
                relpath = os.path.relpath(f.filename, topleveldir)
            else:
                relpath = f.filename

            hash = _archive_hash(af, f, hashfunc, hash_from_meta)
            yield FileProperties(relpath, f.file_size, f.is_dir(), modtime=f.mtime, hash=hash)


def iter_archive(
    archivefile: str,
    topleveldir: Optional[str] = None,
    hashfunc: str = "crc32",
    hash_from_meta: bool = True,
) -> Iterator[FileProperties]:

    if archivefile.endswith(".zip"):
        return iter_zip(archivefile, topleveldir, hashfunc, hash_from_meta=hash_from_meta)
    elif archivefile.endswith(".rar"):
        return iter_rar(archivefile, topleveldir, hashfunc, hash_from_meta=hash_from_meta)
    else:
        basename = os.path.basename(archivefile)
        raise ValueError(f"Unsupported archive format: {basename}")


def iter_syncthing(
    path: str, extra: bool = True, versions: str = ".stversions", hashfunc: Optional[str] = None
) -> Iterator[FileProperties]:

    """skips syncthing versions folder"""

    for entry in scandir_rec(path, files=True, dirs=True, relative=True, allow_skip=True):
        if entry.name == versions and entry.is_dir():
            entry.follow = False
            continue

        if extra:
            stat = os.stat(entry.path)
        else:
            stat = entry.stat()

        modtime = datetime_from_utc_timestamp_ns(stat.st_mtime_ns)

        yield FileProperties(
            entry.relpath, stat.st_size, entry.is_dir(), entry.path, (stat.st_dev, stat.st_ino), modtime
        )
