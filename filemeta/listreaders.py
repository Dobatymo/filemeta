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

from .utils import OpenFileOrUrl


def iter_dir(path, extra=True, hashfunc=None, dirs=True):
	# type: (str, bool, Optional[str], bool) -> Iterator[FileProperties]

	""" Returns correct device id and file inode for py > 3.5 on windows if `extras=True` """

	for entry in scandir_rec(path, files=True, dirs=dirs, relative=True):

		if extra:
			stat = os.stat(entry.path)
		else:
			stat = entry.stat()

		modtime = datetime_from_utc_timestamp_ns(stat.st_mtime_ns)

		yield FileProperties(entry.relpath.replace("\\", "/"), stat.st_size, entry.is_dir(), entry.path, (stat.st_dev, stat.st_ino), modtime)

def iter_archiveorg_xml(path, hashfunc="sha1", dirs=None):
	# type: (str, str, Optional[bool]) -> Iterator[FileProperties]

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

def iter_gamedat_xml(path, hashfunc="sha1", dirs=None):
	# type: (str, str, Optional[bool]) -> Iterator[FileProperties]

	assert hashfunc in ("sha1", "md5", "crc32")

	hashfunc = {
		"crc32": "crc",
	}.get(hashfunc, hashfunc)

	with open(path, "rt") as fr:
		for event, element in iterparse(fr, ["end"]):
			if element.tag != "game":
				continue

			rom = element.find("rom")
			relpath = rom.get("name")
			size = int(rom.get("size"))
			hash = rom.get(hashfunc)
			yield FileProperties(relpath, size, False, hash=hash)

def iter_zip(archivefile, topleveldir=None, hashfunc="crc32", assume_utc=False, dirs=None):
	# type: (str, Optional[str], str, bool, Optional[bool]) -> Iterator[FileProperties]

	""" If `topleveldir` is given, returned file paths will be relativ to this directory within the archive.

		If `assume_utc` is False (the default), it is assumed that local time is stored
		in the zip file. Otherwise it's assumed to be UTC.
	"""

	assert hashfunc in {"crc32"}

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

			yield FileProperties(relpath, f.file_size, f.is_dir(), modtime=modtime, hash=f.CRC)

def iter_rar(archivefile, topleveldir=None, hashfunc="crc32", dirs=None):
	# type: (str, Optional[str], str, Optional[bool]) -> Iterator[FileProperties]

	from rarfile import RarFile

	assert hashfunc in {"crc32"}

	with RarFile(archivefile, "r") as af:
		for f in af.infolist():

			if topleveldir:
				relpath = os.path.relpath(f.filename, topleveldir)
			else:
				relpath = f.filename

			yield FileProperties(relpath, f.file_size, f.is_dir(), modtime=f.mtime, hash=f.CRC)

def iter_archive(archivefile, topleveldir=None, hashfunc="crc32"):
	# type: (str, Optional[str], str) -> Iterator[FileProperties]

	if archivefile.endswith(".zip"):
		return iter_zip(archivefile, topleveldir, hashfunc)
	elif archivefile.endswith(".rar"):
		return iter_rar(archivefile, topleveldir, hashfunc)
	else:
		raise ValueError("Unsupported archive format")

def iter_syncthing(path, extra=True, versions=".stversions", hashfunc=None):
	# type: (str, bool, str, Optional[str]) -> Iterator[FileProperties]

	""" skips syncthing versions folder """

	for entry in scandir_rec(path, files=True, dirs=True, relative=True, allow_skip=True):
		if entry.name == versions and entry.is_dir():
			entry.follow = False
			continue

		if extra:
			stat = os.stat(entry.path)
		else:
			stat = entry.stat()

		modtime = datetime_from_utc_timestamp_ns(stat.st_mtime_ns)

		yield FileProperties(entry.relpath, stat.st_size, entry.is_dir(), entry.path, (stat.st_dev, stat.st_ino), modtime)
