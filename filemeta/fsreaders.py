from __future__ import generator_stop

import logging
import platform
import sqlite3
from os import fspath, stat
from typing import TYPE_CHECKING, Dict, Iterator, Optional, Tuple

from genutility.filesystem import scandir_rec
from genutility.sql import CursorContext

from .utils import is_signed_int_64, unsigned_to_signed_int_64

if TYPE_CHECKING:
	from os import PathLike

	from genutility.typing import Connection

	FileID = Tuple[int, int]
	FilesDict = Dict[FileID, Tuple[str, str, int, int]]
	FilesTuple = Tuple[int, int, str, str, int, int]

logger = logging.getLogger(__name__)

def read_dir(path):
	# type: (str, ) -> FilesDict

	root = path.replace("\\", "/")

	def scandir_error_log(entry, exception):
		logger.warning("Error in %s: %s", entry.path, exception)

	def it():
		for entry in scandir_rec(path, files=True, dirs=False, relative=True, follow_symlinks=False, errorfunc=scandir_error_log):

			try:
				stats = stat(entry.path)
			except (PermissionError, FileNotFoundError) as e:
				logger.warning("Ignoring '%s' because of: %s", entry.path, e)
				continue
			except OSError as e:
				logger.error("Ignoring '%s' because of: %s", entry.path, e)
				continue

			relpath = entry.relpath.replace("\\", "/")
			if stats.st_dev != 0 and stats.st_ino != 0:
				# On windows st_dev and st_ino is unsigned 64 bit int, but sqlite only supports signed 64 bit ints.
				# I don't know about linux
				device = unsigned_to_signed_int_64(stats.st_dev)
				inode = unsigned_to_signed_int_64(stats.st_ino)
				assert is_signed_int_64(inode) and is_signed_int_64(device) and is_signed_int_64(stats.st_size) and is_signed_int_64(stats.st_mtime_ns), (stats.st_dev, stats.st_ino, stats.st_size, stats.st_mtime_ns)
				yield (device, inode), (root, relpath, stats.st_size, stats.st_mtime_ns)
			else:
				logger.warning("Ignoring '%s' because of invalid id (device ID=%s, file inode=%s)", entry.path, stats.st_dev, stats.st_ino)

	return dict(it())


class FilesDB:

	def __init__(self, path, case_insensitive=None):
		# type: (str, Optional[bool]) -> None

		self.conn = sqlite3.connect(path, detect_types=sqlite3.PARSE_DECLTYPES)
		self.conn.isolation_level = None

		if case_insensitive is None:
			self.case_insensitive = platform.system() in ("Windows", "Darwin")
		else:
			self.case_insensitive = case_insensitive

	def init(self):
		# type: () -> None

		create_table_query = """CREATE TABLE IF NOT EXISTS files (
			id INTEGER PRIMARY KEY,
			begin_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			end_date TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
			device INTEGER NOT NULL,
			inode INTEGER NOT NULL,
			root TEXT NOT NULL,
			path TEXT NOT NULL,
			filesize INTEGER NOT NULL,
			utcmtime INTEGER NOT NULL,
			deleted INTEGER NOT NULL DEFAULT 0
		);"""

		idx_files = "CREATE INDEX IF NOT EXISTS idx_files_id ON files (device, inode);"
		if self.case_insensitive:
			idx_root = "CREATE INDEX IF NOT EXISTS idx_root ON files (root COLLATE NOCASE);"
		else:
			idx_root = "CREATE INDEX IF NOT EXISTS idx_root ON files (root);"
		idx_deleted = "CREATE INDEX IF NOT EXISTS idx_deleted ON files (deleted);"

		create_index_queries = [idx_files, idx_root, idx_deleted]

		with CursorContext(self.conn) as cur:
			cur.execute(create_table_query)
			for query in create_index_queries:
				cur.execute(query)

	def get_connection(self):
		# type: () -> Connection

		return self.conn

	def get(self, deleted=False):
		# type: (bool, ) -> Iterator[FilesTuple]

		query = "SELECT device, inode, root, path, filesize, utcmtime FROM files WHERE deleted=?"

		with CursorContext(self.conn) as cursor:
			yield from cursor.execute(query, (int(deleted), ))

	def get_by_root(self, root, deleted=False):
		# type: (PathLike, bool) -> Iterator[FilesTuple]

		root = fspath(root).replace("\\", "/")
		if self.case_insensitive:
			query = "SELECT device, inode, root, path, filesize, utcmtime FROM files WHERE deleted=? AND root=? COLLATE NOCASE"
		else:
			query = "SELECT device, inode, root, path, filesize, utcmtime FROM files WHERE deleted=? AND root=?"

		with CursorContext(self.conn) as cursor:
			yield from cursor.execute(query, (int(deleted), root))

	def read_database(self, path, deleted=False):
		# type: (PathLike, bool) -> FilesDict

		return {(device, inode): (root, path, filesize, utcmtime)
			for device, inode, root, path, filesize, utcmtime in self.get_by_root(path, deleted)
		}
