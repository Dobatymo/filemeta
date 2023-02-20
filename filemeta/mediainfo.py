import logging
import re
from fractions import Fraction
from typing import Any, Callable, Dict, FrozenSet, Iterator, Optional, Set, Tuple, Union
from xml.etree import ElementTree

import pkg_resources
from genutility.exceptions import ParseError
from genutility.json import BuiltinEncoder, read_json, write_json
from pymediainfo import MediaInfo

logger = logging.getLogger(__name__)


def yesno(s: str) -> bool:
    try:
        return {
            "Yes": True,
            "Yes / Yes": True,
            "Yes (Explicit)": True,
            "Yes (Implicit)": True,
            "Yes (NBC)": True,
            "No": False,
            "No (Explicit)": False,
            "No (Implicit)": False,
        }[s]
    except KeyError as e:
        raise ValueError(e)


is_integer = re.compile(r"^(-?[0-9]+)(\.0*)?$")
is_two_integers = re.compile(r"^([0-9]+) \/ ([0-9]+)$")


def integer(s: Union[str, int]) -> int:
    if isinstance(s, str):
        m1 = is_integer.match(s)
        m2 = is_two_integers.match(s)
        if m1:
            return int(m1.group(1))
        elif m2:
            a, b = int(m2.group(1)), int(m2.group(2))
            if a == b:
                return a
            else:
                raise ValueError(f"Found two non-equal integers: {a}, {b}")
        else:
            raise ValueError(f"{s} is not a integer")
    elif isinstance(s, int):
        return s
    else:
        raise TypeError(f"{s} is not a integer")


def get_conversion_func(s: str) -> Callable[[Any], Any]:
    return {
        "integer": integer,
        "boolean": yesno,
        "fraction": Fraction,
        "string": str,
    }[s]


def guess_type(s: str) -> Optional[str]:
    if "Yes" in s or "No" in s:
        return "boolean"

    try:
        integer(s)
        return "integer"
    except ValueError:
        pass

    try:
        float(s)
        return "fraction"
    except ValueError:
        pass

    return None


widen_type_to = {
    "integer": "fraction",
    "fraction": "string",
    "boolean": "string",
}


class MediaInfoFields:
    mi_fields_name = "data/mediainfo-fields.json"
    mi_types_name = "data/mediainfo-types.json"

    def __init__(self):
        self.mi_fields_path = pkg_resources.resource_filename(__package__, self.mi_fields_name)
        self.mi_types_path = pkg_resources.resource_filename(__package__, self.mi_types_name)

        self.mi_fields = self.get_mi_fields(self.mi_fields_path)
        self.mi_types = self.get_mi_types(self.mi_types_path)

    def persist(self):
        self.put_mi_fields(self.mi_fields_path, self.mi_fields)
        self.put_mi_types(self.mi_types_path, self.mi_types)

    @staticmethod
    def get_mi_fields(path: str) -> Dict[str, Dict[str, FrozenSet[str]]]:
        mi_fields = read_json(path)
        for k, v in mi_fields.items():
            mi_fields[k] = {track: frozenset(fields) for track, fields in v.items()}

        return mi_fields

    @staticmethod
    def put_mi_fields(path: str, mi_fields) -> None:
        write_json(mi_fields, path, indent="\t", sort_keys=True, cls=BuiltinEncoder, safe=True, sort_sets=True)

    @staticmethod
    def get_mi_types(path: str) -> Dict[str, Dict[str, Callable]]:
        mi_types = read_json(path)
        return mi_types

    @staticmethod
    def put_mi_types(path: str, mi_types) -> None:
        write_json(mi_types, path, indent="\t", sort_keys=True, safe=True)

    def mediainfo(
        self, path: str, logged_keys: Optional[Dict[str, Set[Tuple[str, str]]]]
    ) -> Iterator[Tuple[Tuple[str, str], Any]]:
        try:
            media_info = MediaInfo.parse(path)
        except ElementTree.ParseError as e:
            raise ParseError(e)
        except RuntimeError as e:
            logger.exception("Parsing %s failed", path)
            raise ParseError(e)

        for t in media_info.tracks:
            for k, v in t.to_data().items():
                if k in self.mi_fields["use"][t.track_type]:
                    field_type = self.mi_types[t.track_type].get(k, None)

                    if field_type:
                        try:
                            v = get_conversion_func(field_type)(v)
                        except ValueError as e:
                            if field_type in widen_type_to:
                                new_type = widen_type_to[field_type]
                                self.mi_types[t.track_type][k] = new_type
                                logger.warning(
                                    "Converting %s/%s: `%s` to %s failed: %s. Mapping to %s instead.",
                                    t.track_type,
                                    k,
                                    v,
                                    field_type,
                                    e,
                                    new_type,
                                )
                                field_type = new_type
                            elif field_type == "string":
                                raise ValueError(
                                    f"Converting {t.track_type}/{k}: `{v[:10]}` to {field_type} failed: {e}"
                                )
                            else:
                                raise ValueError(f"Invalid field type: {field_type}")

                    if field_type != "string" and isinstance(v, str):
                        guessed_type = guess_type(v)
                        if guessed_type:
                            logger.warning("Guessing type %s for %s/%s", guessed_type, t.track_type, k)
                            self.mi_types[t.track_type][k] = guessed_type

                    yield (t.track_type, k), v
                elif k in self.mi_fields["ignore"][t.track_type]:
                    pass
                else:
                    key = (t.track_type, k)
                    if logged_keys is None or key not in logged_keys["unhandled"]:
                        if t.track_type == "Menu":
                            logger.debug("Found unhandled key %s/%s: %s (%s)", t.track_type, k, v, path)
                        else:
                            logger.warning("Found unhandled key %s/%s: %s (%s)", t.track_type, k, v, path)

                        if logged_keys is not None:
                            logged_keys["unhandled"].add(key)
