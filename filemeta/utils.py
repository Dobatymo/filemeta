import platform
import warnings
from gzip import GzipFile
from io import TextIOWrapper
from pathlib import Path
from string import ascii_uppercase
from typing import IO, Any, Callable, Dict, Iterable, List, Optional, TypeVar, Union, cast

import requests
from genutility.file import _check_arguments
from typing_extensions import Protocol

T = TypeVar("T")


class HashableLessThan(Protocol):
    def __lt__(self, __other: Any) -> bool: ...

    def __hash__(self) -> int: ...


DEFAULT_DB_NAME = f"{platform.node()}-catalog.db"
DEFAULT_TIMEOUT = 60


def get_all_drives_windows() -> List[Path]:
    # replace with os.listdrives() for python 3.12+
    out: List[Path] = []
    for driveletter in ascii_uppercase:
        drive = Path(driveletter + ":\\")
        if drive.is_dir():
            out.append(drive)
    return out


def get_url_fp(path: str, mode: str, encoding: Optional[str]) -> Union[GzipFile, IO]:
    if "w" in mode:
        raise ValueError("Cannot write mode for URLs")

    r = requests.get(path, timeout=DEFAULT_TIMEOUT, stream=True)
    r.raise_for_status()
    if r.headers.get("content-encoding", None) == "gzip":
        fileobj = cast(IO[bytes], GzipFile(fileobj=r.raw))
    else:
        fileobj = r.raw

    if "t" in mode:
        return TextIOWrapper(fileobj, encoding)
    else:
        return fileobj


class SmartPath:
    def __init__(self, path: str):
        self.path = path

    def open(self, mode: str = "rt", encoding: Optional[str] = None):
        encoding = _check_arguments(mode, encoding)
        if self.path.startswith(("http://", "https://")):
            return get_url_fp(self.path, mode, encoding)
        else:
            return open(self.path, mode, encoding=encoding)


class OpenFileOrUrl:
    def __init__(self, path: str, mode: str = "rt", encoding: Optional[str] = "utf-8") -> None:
        encoding = _check_arguments(mode, encoding)

        if path.startswith(("http://", "https://")):
            self.f = get_url_fp(path, mode, encoding)
        else:
            self.f = open(path, mode, encoding=encoding)

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
    by: str, it: Iterable[T], apply: Optional[Callable] = None, check_none: bool = True, warn_dups: bool = True
) -> Dict[HashableLessThan, T]:
    if check_none:
        getattr = getattrnotnone

    if warn_dups:
        ret = {}
        for props in it:
            if apply is None:
                key = getattr(props, by)
            else:
                key = apply(getattr(props, by))
            if key in ret:
                warnings.warn("Ignoring duplicated entries", stacklevel=2)
            ret[key] = props
        return ret
    else:
        if apply is None:
            return {getattr(props, by): props for props in it}
        else:
            return {apply(getattr(props, by)): props for props in it}
