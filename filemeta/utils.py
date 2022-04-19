import platform
from builtins import print as _print
from gzip import GzipFile
from io import TextIOWrapper
from pathlib import Path
from string import ascii_uppercase
from typing import IO, Any, Dict, Iterable, List, Optional, Protocol, TypeVar, Union

import requests
from genutility.file import _check_arguments

T = TypeVar("T")


class HashableLessThan(Protocol):
    def __lt__(self, __other: Any) -> bool:
        ...

    def __hash__(self) -> int:
        ...


DEFAULT_DB_PATH = f"{platform.node()}-catalog.db"


def get_all_drives_windows() -> List[Path]:

    return [drive for driveletter in ascii_uppercase if (drive := Path(driveletter + ":\\")).is_dir()]


def print(*msg, end="\x1b[0K\n", **kwargs):
    """Same as `print` but it clears the reminder of the line.
    Requires ANSI escapes either though a supporting terminal or `colorama`.
    """

    _print(*msg, end=end, **kwargs)


def is_signed_int_64(num):
    # type: (int, ) -> bool

    return -(2**63) <= num <= 2**63 - 1


def unsigned_to_signed_int_64(num):
    # type: (int, ) -> int

    return num - 2**63


def signed_to_unsigned_int_64(num):
    # type: (int, ) -> int

    return num + 2**63


class OpenFileOrUrl:
    def __init__(self, path, mode="rt", encoding="utf-8"):
        # type: (str, str, Optional[str]) -> None

        self.encoding = encoding

        encoding = _check_arguments(mode, encoding)

        if path.startswith(("http://", "https://")):

            if "w" in mode:
                raise ValueError("Cannot write mode for URLs")

            r = requests.get(path, stream=True)
            r.raise_for_status()
            if r.headers["content-encoding"] == "gzip":
                fileobj = GzipFile(fileobj=r.raw)
            else:
                fileobj = r.raw

            if "t" in mode:
                self.f = TextIOWrapper(fileobj, encoding=self.encoding)  # type: Union[GzipFile, IO]
            else:
                self.f = fileobj
        else:
            self.f = open(path, mode, encoding=self.encoding)

    def __enter__(self):
        # type: () -> Union[GzipFile, IO]

        return self.f

    def __exit__(self, *args):
        self.close()

    def close(self):
        # type: () -> None

        self.f.close()


def iterable_to_dict_by_key(by, it):
    # type: (str, Iterable[T]) -> Dict[HashableLessThan, T]

    # todo: check if there are duplicated keys and warn about them

    return {getattr(props, by): props for props in it}
