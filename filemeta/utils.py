import platform
from builtins import print as _print
from gzip import GzipFile
from io import TextIOWrapper
from pathlib import Path
from string import ascii_uppercase
from typing import IO, Any, Callable, Dict, Iterable, List, Optional, Protocol, TypeVar, Union

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


def is_signed_int_64(num: int) -> bool:
    return -(2**63) <= num <= 2**63 - 1


def unsigned_to_signed_int_64(num: int) -> int:
    return num - 2**63


def signed_to_unsigned_int_64(num: int) -> int:
    return num + 2**63


class OpenFileOrUrl:
    def __init__(self, path: str, mode: str = "rt", encoding: Optional[str] = "utf-8") -> None:
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
                self.f: Union[GzipFile, IO] = TextIOWrapper(fileobj, encoding=self.encoding)
            else:
                self.f = fileobj
        else:
            self.f = open(path, mode, encoding=self.encoding)

    def __enter__(self) -> Union[GzipFile, IO]:
        return self.f

    def __exit__(self, *args):
        self.close()

    def close(self) -> None:
        self.f.close()


def getattrnotnone(obj: Any, attr: str) -> Any:
    value = getattr(obj, attr)
    if value is None:
        raise AttributeError(f"'{type(obj).__name__}' object has no attribute '{attr}'")
    return value


def iterable_to_dict_by_key(
    by: str, it: Iterable[T], apply: Optional[Callable] = None, check_none: bool = True
) -> Dict[HashableLessThan, T]:
    # todo: check if there are duplicated keys and warn about them

    if check_none:
        getattr = getattrnotnone

    if apply is None:
        return {getattr(props, by): props for props in it}
    else:
        return {apply(getattr(props, by)): props for props in it}
