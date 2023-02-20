import logging
from fractions import Fraction
from typing import Any, Iterator, Tuple

import piexif
import pkg_resources
from genutility.json import read_json
from piexif._exceptions import InvalidImageDataError  # noqa: F401

logger = logging.getLogger(__name__)

exif_key_labels = read_json(pkg_resources.resource_filename(__package__, "data/exif-key-labels.json"))
exif_value_labels = read_json(pkg_resources.resource_filename(__package__, "data/exif-value-labels.json"))


def exif_key_label(name: str) -> str:
    return exif_key_labels.get(name, name)


def exif_value_label(name: str, value: int, reserved="reserved") -> str:
    try:
        return exif_value_labels[name].get(str(value), reserved)
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


def exif_table(path: str) -> Iterator[Tuple[str, str, str, Any, str]]:
    # see Exif Version 2.2 docs

    exif = piexif.load(path)

    for ifd in ("0th", "Exif", "GPS", "1st"):
        for tag in exif[ifd]:
            name = piexif.TAGS[ifd][tag]["name"]
            type = piexif.TAGS[ifd][tag]["type"]
            value = exif[ifd][tag]
            key_label = exif_key_label(name)

            if type == piexif.TYPES.Ascii:
                value = value.rstrip(b"\0")
                if b"\0" not in value:
                    value_label = value.decode("ascii")
                else:
                    value_label = tuple(elm.decode("ascii") for elm in value.split(b"\0"))

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
                value_label = value

            elif type in (piexif.TYPES.Byte, piexif.TYPES.SByte):
                value_label = value

            else:
                logger.error("Invalid exif type name=%s type=% in file %s", name, type, path)
                continue

            yield ifd, name, key_label, value, recstr(value_label)
