import logging
from fractions import Fraction
from typing import Any, Iterator, Tuple, Union

import importlib_resources
import piexif
from genutility.json import read_json
from piexif._exceptions import InvalidImageDataError  # noqa: F401

logger = logging.getLogger(__name__)

exif_key_labels = read_json(importlib_resources.files(__package__).joinpath("data/exif-key-labels.json"))
exif_value_labels = read_json(importlib_resources.files(__package__).joinpath("data/exif-value-labels.json"))


def exif_key_label(name: str) -> str:
    return exif_key_labels.get(name, name)  # type: ignore[no-any-return]


def exif_value_label(name: str, value: int, reserved: str = "reserved") -> str:
    try:
        return exif_value_labels[name].get(str(value), reserved)  # type: ignore[no-any-return]
    except KeyError:
        return str(value)


def recstr(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    elif isinstance(obj, tuple):
        return "(" + ", ".join(map(recstr, obj)) + ")"
    elif isinstance(obj, list):
        return "[" + ", ".join(map(recstr, obj)) + "]"
    else:
        return str(obj)


def decode_ascii(value: bytes, encoding: str) -> Union[str, Tuple[str, ...]]:
    if b"\0" not in value:
        return value.decode(encoding)
    else:
        return tuple(elm.decode(encoding) for elm in value.split(b"\0"))


def piexif_load(path_or_data):
    try:
        return piexif.load(path_or_data)
    except ValueError as e:
        if e.args and e.args[0] == "doesnot have exif":
            return None
        raise


def exif_table(path: str, encoding: str = "utf-8") -> Iterator[Tuple[str, str, str, Any, str]]:
    # see Exif Version 2.2 docs

    if path.lower().endswith(".heic"):
        from pi_heif import register_heif_opener
        from PIL import Image

        register_heif_opener()

        with Image.open(path) as img:
            img.load()
            if "exif" in img.info:
                exif = piexif_load(img.info["exif"])
    else:
        exif = piexif_load(path)

    if exif is None:
        return

    for ifd in ("0th", "Exif", "GPS", "1st"):
        for tag in exif[ifd]:
            name = piexif.TAGS[ifd][tag]["name"]
            type = piexif.TAGS[ifd][tag]["type"]
            value = exif[ifd][tag]
            key_label = exif_key_label(name)

            if type == piexif.TYPES.Ascii:
                value = value.rstrip(b"\0")
                try:
                    value_label: Any = decode_ascii(value, "ascii")
                except UnicodeDecodeError:
                    try:
                        value_label = decode_ascii(value, encoding)
                    except UnicodeDecodeError:
                        logging.warning("Failed to decode name=%s in file %s: %s", name, path, value.decode("latin-1"))
                        continue

            elif type in (piexif.TYPES.Short, piexif.TYPES.SShort, piexif.TYPES.Long, piexif.TYPES.SLong):
                value_label = exif_value_label(name, value)

            elif type in (piexif.TYPES.Rational, piexif.TYPES.SRational):
                try:
                    if len(value) == 2 and not isinstance(value[0], tuple):
                        value_label = Fraction(*value)
                    else:
                        value_label = tuple(Fraction(*elm) for elm in value)
                except ZeroDivisionError:
                    logger.warning("Invalid fraction name=%s in file %s: %s", name, path, value)
                    continue

            elif type in (piexif.TYPES.Undefined, piexif.TYPES.Float, piexif.TYPES.DFloat):
                if name in exif_value_labels and isinstance(value, bytes) and len(value) == 1:
                    value_label = exif_value_label(name, value[0])
                else:
                    value_label = value

            elif type in (piexif.TYPES.Byte, piexif.TYPES.SByte):
                if name in exif_value_labels and isinstance(value, int):
                    value_label = exif_value_label(name, value)
                else:
                    value_label = value

            else:
                logger.error("Invalid exif type name=%s type=% in file %s", name, type, path)
                continue

            yield ifd, name, key_label, value, recstr(value_label)
