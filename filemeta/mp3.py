import logging
import struct
from collections import namedtuple
from enum import IntEnum
from os import SEEK_CUR, SEEK_SET
from typing import IO, Callable, Dict, Iterable, List, Optional, Tuple, Type, Union

import bitstruct
from genutility.exceptions import ParseError
from genutility.file import Empty, read_or_raise

logger = logging.getLogger(__name__)

# general exceptions


class WrongSignature(ParseError):
    pass


# helpers


def read_until(fr: IO[bytes], delimiter: bytes) -> bytes:
    ret: List[int] = []
    delim = list(delimiter)
    delim_len = len(delimiter)

    while ret[-delim_len:] != delim:
        ret.append(fr.read(1)[0])

    return bytes(ret[:delim_len])


def transform_iterable(it: Iterable, conv: Dict[int, Callable]) -> list:
    fields = list(it)
    for i, func in conv.items():
        fields[i] = func(fields[i])

    return fields


def funpack(fr: IO[bytes], fmt: str) -> tuple:
    num_bytes = struct.calcsize(fmt)
    return struct.unpack(fmt, read_or_raise(fr, num_bytes))


def read_bytes_to_namedtuple(
    fr: IO[bytes], fmt: str, namedtuplecls: Type[namedtuple], conv: Optional[Dict[int, Callable]] = None
) -> namedtuple:
    fields = funpack(fr, fmt)

    if conv:
        fields = transform_iterable(fields, conv)

    return namedtuplecls._make(fields)


def fbitunpack(fr: IO[bytes], fmt: str) -> tuple:
    num_bits = bitstruct.calcsize(fmt)
    num_bytes, remainder = divmod(num_bits, 8)
    assert remainder == 0

    return bitstruct.unpack(fmt, read_or_raise(fr, num_bytes))


def read_bits_to_namedtuple(
    fr: IO[bytes], fmt: str, namedtuplecls: Type[namedtuple], conv: Optional[Dict[int, Callable]] = None
) -> namedtuple:
    fields = fbitunpack(fr, fmt)

    if conv:
        fields = transform_iterable(fields, conv)

    return namedtuplecls._make(fields)


# named tuples

Mp3Header = namedtuple(
    "Mp3Header",
    [
        "sync",
        "id",
        "layer",
        "protection",
        "bitrate",
        "frequency",
        "padding",
        "private",
        "mode",
        "mode_extension",
        "copyright",
        "home_original",
        "emphasis",
    ],
)

Mp3SideInformation = namedtuple(
    "Mp3SideInformation",
    [
        "main_data_begin",
        "private_bits",
        "scfsi",
        "par2_3_length",
        "big_values",
        "global_gain",
        "scalefac_compress",
        "windows_switching_flag",
        "block_type",
        "mixed_blockflag",
        "table_select",
        "subblock_gain",
        "region0_count",
        "region1_count",
        "preflag",
        "scalfac_scale",
        "count1table_select",
    ],
)

Id3v2Header = namedtuple(
    "Id3v2Header", ["major_version", "revision_number", "a", "b", "c", "d", "e", "f", "g", "h", "size"]
)
APEv2Header = namedtuple("APEv2Header", ["version", "size", "item_count", "flags", "reserved"])

Id3v1Fields = namedtuple("Id3v1Fields", ["song_title", "artist", "album", "year", "comment", "genre"])
Id3v11Fields = namedtuple("Id3v11Fields", ["song_title", "artist", "album", "year", "comment", "track", "genre"])

# enums


class ID(IntEnum):
    MPEG_25 = 0
    RESERVED = 1
    MPEG_2 = 2
    MPEG_1 = 3


class Layer(IntEnum):
    RESERVED = 0
    LAYER_III = 1
    LAYER_II = 2
    LAYER_I = 3


class Mode(IntEnum):
    STEREO = 0
    JOINT_STEREO = 1
    DUAL_CHANNEL = 2
    SINGLE_CHANNEL = 3


# dicts

SAMPLE_RATE_MAP = {
    0: {
        ID.MPEG_25: 11025,
        ID.MPEG_2: 22050,
        ID.MPEG_1: 44100,
    },
    1: {
        ID.MPEG_25: 12000,
        ID.MPEG_2: 24000,
        ID.MPEG_1: 48000,
    },
    2: {
        ID.MPEG_25: 8000,
        ID.MPEG_2: 16000,
        ID.MPEG_1: 32000,
    },
}

BITRATE_MAP = {
    1: {
        ID.MPEG_1: {Layer.LAYER_I: 32, Layer.LAYER_II: 32, Layer.LAYER_III: 32},
        ID.MPEG_2: {Layer.LAYER_I: 32, Layer.LAYER_II: 32, Layer.LAYER_III: 8},
    },
    2: {
        ID.MPEG_1: {Layer.LAYER_I: 64, Layer.LAYER_II: 48, Layer.LAYER_III: 40},
        ID.MPEG_2: {Layer.LAYER_I: 64, Layer.LAYER_II: 48, Layer.LAYER_III: 16},
    },
    3: {
        ID.MPEG_1: {Layer.LAYER_I: 96, Layer.LAYER_II: 56, Layer.LAYER_III: 48},
        ID.MPEG_2: {Layer.LAYER_I: 96, Layer.LAYER_II: 56, Layer.LAYER_III: 24},
    },
    4: {
        ID.MPEG_1: {Layer.LAYER_I: 128, Layer.LAYER_II: 64, Layer.LAYER_III: 56},
        ID.MPEG_2: {Layer.LAYER_I: 128, Layer.LAYER_II: 64, Layer.LAYER_III: 32},
    },
    5: {
        ID.MPEG_1: {Layer.LAYER_I: 160, Layer.LAYER_II: 80, Layer.LAYER_III: 64},
        ID.MPEG_2: {Layer.LAYER_I: 160, Layer.LAYER_II: 80, Layer.LAYER_III: 64},
    },
    6: {
        ID.MPEG_1: {Layer.LAYER_I: 192, Layer.LAYER_II: 96, Layer.LAYER_III: 80},
        ID.MPEG_2: {Layer.LAYER_I: 192, Layer.LAYER_II: 96, Layer.LAYER_III: 80},
    },
    7: {
        ID.MPEG_1: {Layer.LAYER_I: 224, Layer.LAYER_II: 112, Layer.LAYER_III: 96},
        ID.MPEG_2: {Layer.LAYER_I: 224, Layer.LAYER_II: 112, Layer.LAYER_III: 56},
    },
    8: {
        ID.MPEG_1: {Layer.LAYER_I: 256, Layer.LAYER_II: 128, Layer.LAYER_III: 112},
        ID.MPEG_2: {Layer.LAYER_I: 256, Layer.LAYER_II: 128, Layer.LAYER_III: 64},
    },
    9: {
        ID.MPEG_1: {Layer.LAYER_I: 288, Layer.LAYER_II: 160, Layer.LAYER_III: 128},
        ID.MPEG_2: {Layer.LAYER_I: 288, Layer.LAYER_II: 160, Layer.LAYER_III: 128},
    },
    10: {
        ID.MPEG_1: {Layer.LAYER_I: 320, Layer.LAYER_II: 192, Layer.LAYER_III: 160},
        ID.MPEG_2: {Layer.LAYER_I: 320, Layer.LAYER_II: 192, Layer.LAYER_III: 160},
    },
    11: {
        ID.MPEG_1: {Layer.LAYER_I: 352, Layer.LAYER_II: 224, Layer.LAYER_III: 192},
        ID.MPEG_2: {Layer.LAYER_I: 352, Layer.LAYER_II: 224, Layer.LAYER_III: 112},
    },
    12: {
        ID.MPEG_1: {Layer.LAYER_I: 384, Layer.LAYER_II: 256, Layer.LAYER_III: 224},
        ID.MPEG_2: {Layer.LAYER_I: 384, Layer.LAYER_II: 256, Layer.LAYER_III: 128},
    },
    13: {
        ID.MPEG_1: {Layer.LAYER_I: 416, Layer.LAYER_II: 320, Layer.LAYER_III: 256},
        ID.MPEG_2: {Layer.LAYER_I: 416, Layer.LAYER_II: 320, Layer.LAYER_III: 256},
    },
    14: {
        ID.MPEG_1: {Layer.LAYER_I: 448, Layer.LAYER_II: 384, Layer.LAYER_III: 320},
        ID.MPEG_2: {Layer.LAYER_I: 448, Layer.LAYER_II: 384, Layer.LAYER_III: 320},
    },
}

# mp3 exceptions


class Reserved(ParseError):
    pass


class OutOfSync(ParseError):
    def __init__(self, pos: int, header: Mp3Header):
        self.pos = pos
        ParseError.__init__(self, f"Out of sync at {pos}: {header}")


class MaybeNotMp3(ParseError):
    pass


class InvalidFrame(ParseError):
    pass


# mp3 helper


def id3_sync_safe_to_int(sync_safe: bytes) -> int:
    byte0 = sync_safe[0]
    byte1 = sync_safe[1]
    byte2 = sync_safe[2]
    byte3 = sync_safe[3]

    return byte0 << 21 | byte1 << 14 | byte2 << 7 | byte3


# mp3


def get_frame_length(header: Mp3Header) -> int:
    try:
        samplerate = SAMPLE_RATE_MAP[header.frequency][header.id]
    except KeyError:
        raise Reserved(f"Found reserved value frequency={header.frequency} or id={header.id}")

    layer = ID.MPEG_2 if header.layer == ID.MPEG_25 else header.layer
    try:
        bitrate = BITRATE_MAP[header.bitrate][header.id][layer]
    except KeyError:
        raise ParseError(f"Invalid values bitrate={header.bitrate} id={header.id} layer={header.layer}")

    return int(144 * bitrate * 1000 / samplerate + header.padding)


def read_sideinfo(fr: IO[bytes], header: Mp3Header) -> Mp3SideInformation:
    single_channel = header.mode == Mode.SINGLE_CHANNEL

    if single_channel:
        SIDE_INFORMATION_SINGLE_FMT_1 = "u9 r5 r4"  # 18
        # -> 18
        SIDE_INFORMATION_SINGLE_FMT_2 = "r12 r9 r8 r4 b1"  # 34
        SIDE_INFORMATION_SINGLE_FMT_3_SWITCH = "r2 r1 r10 r9"  # 22
        SIDE_INFORMATION_SINGLE_FMT_3_NOSWITCH = "r15 u4 u3"  # 22
        SIDE_INFORMATION_SINGLE_FMT_4 = "b1 r1 r1"  # 3
        # -> 59
        # total: 18 + 59 * 2 = 136 bits == 17 bytes

        data = fr.read(17)
        delta = 0

        main_data_begin, private_bits, scfsi = bitstruct.unpack_from(SIDE_INFORMATION_SINGLE_FMT_1, data, delta)
        delta += 18

        for i in range(2):
            par2_3_length, big_values, global_gain, scalefac_compress, windows_switching_flag = bitstruct.unpack_from(
                SIDE_INFORMATION_SINGLE_FMT_2, data, delta
            )
            delta += 34

            if windows_switching_flag == 1:
                block_type, mixed_blockflag, table_select, subblock_gain = bitstruct.unpack_from(
                    SIDE_INFORMATION_SINGLE_FMT_3_SWITCH, data, delta
                )
                region0_count = None
                region1_count = None
            else:
                block_type = 10
                mixed_blockflag = None
                table_select, region0_count, region1_count = bitstruct.unpack_from(
                    SIDE_INFORMATION_SINGLE_FMT_3_NOSWITCH, data, delta
                )
                subblock_gain = None
            delta += 22

            preflag, scalfac_scale, count1table_select = bitstruct.unpack_from(
                SIDE_INFORMATION_SINGLE_FMT_4, data, delta
            )
            delta += 3

    else:
        SIDE_INFORMATION_NONSINGLE_FMT_1 = "u9 r3 r8"  # 20
        # -> 20
        SIDE_INFORMATION_NONSINGLE_FMT_2 = "r24 r18 r16 r8 r2"  # 68
        SIDE_INFORMATION_NONSINGLE_FMT_3_SWITCH = "r4 r2 r20 r18"  # 44
        SIDE_INFORMATION_NONSINGLE_FMT_3_NOSWITCH = "r30 u8 u6"  # 44
        SIDE_INFORMATION_NONSINGLE_FMT_4 = "r2 r2 r2"  # 6
        # -> 118
        # total: 20 + 118 * 2 = 256 bits == 32 bytes

        data = fr.read(32)
        delta = 0

        main_data_begin, private_bits, scfsi = bitstruct.unpack_from(SIDE_INFORMATION_NONSINGLE_FMT_1, data, delta)
        delta += 20

        for i in range(2):
            par2_3_length, big_values, global_gain, scalefac_compress, windows_switching_flag = bitstruct.unpack_from(
                SIDE_INFORMATION_NONSINGLE_FMT_2, data, delta
            )
            delta += 68

            if windows_switching_flag == 1:
                block_type, mixed_blockflag, table_select, subblock_gain = bitstruct.unpack_from(
                    SIDE_INFORMATION_NONSINGLE_FMT_3_SWITCH, data, delta
                )
                delta += 26
            else:
                block_type, mixed_blockflag = None, None
                table_select, region0_count, region1_count = bitstruct.unpack_from(
                    SIDE_INFORMATION_NONSINGLE_FMT_3_NOSWITCH, data, delta
                )
                delta += 30
                subblock_gain = None

            preflag, scalfac_scale, count1table_select = bitstruct.unpack_from(
                SIDE_INFORMATION_NONSINGLE_FMT_4, data, delta
            )
            delta += 6

    side_information_tuple = (
        main_data_begin,
        private_bits,
        scfsi,
        par2_3_length,
        big_values,
        global_gain,
        scalefac_compress,
        windows_switching_flag,
        block_type,
        mixed_blockflag,
        table_select,
        subblock_gain,
        region0_count,
        region1_count,
        preflag,
        scalfac_scale,
        count1table_select,
    )

    return Mp3SideInformation._make(side_information_tuple)


def read_frame(fr: IO[bytes]):
    HEADER_FMT = "u11 u2 u2 b1 u4 u2 u1 b1 u2 u2 b1 b1 u2"

    pos = fr.tell()

    try:
        header = fbitunpack(fr, HEADER_FMT)
    except Empty:  # end of file
        return

    header = Mp3Header._make(header)

    if header.sync != 2047:
        raise OutOfSync(pos, header)

    if header.protection:
        crc = fr.read(2)
    else:
        crc = None

    sideinfo = read_sideinfo(fr, header)

    # main_data, ancillary data

    frame_length = get_frame_length(header)
    fr.seek(pos + frame_length, SEEK_SET)

    return pos, header, crc, sideinfo


def read_id3v2(fr: IO[bytes], parse: bool = False) -> Tuple[int, Id3v2Header]:
    pos = fr.tell()

    if fr.read(3) != b"ID3":
        raise WrongSignature("No ID3v2")

    fmt = ">u8 u8 b1 b1 b1 b1 b1 b1 b1 b1 r32"
    id3_header = read_bits_to_namedtuple(fr, fmt, Id3v2Header, {10: id3_sync_safe_to_int})

    # If the 0x10 bit of byte 5 is set, let OFFSET = OFFSET + 10 (for the footer).

    if parse:
        raise RuntimeError("Not implemented yet")
        # return pos, id3_header, Id3v2Fields._make()

    else:
        fr.seek(id3_header.size, SEEK_CUR)
        return pos, id3_header, None


def read_id3v1(fr: IO[bytes], parse: bool = True) -> Tuple[int, Optional[Union[Id3v1Fields, Id3v11Fields]]]:
    # see <http://id3lib.sourceforge.net/id3/id3v1.html>

    pos = fr.tell()
    tag_begin = b"TAG"
    tag = read_or_raise(fr, len(tag_begin))

    if tag != tag_begin:
        fr.seek(pos)
        raise WrongSignature("No ID3v1")

    if parse:
        fmt = "30s30s30s4s30sB"
        zero = b"\0"
        song_title, artist, album, year, comment, genre = funpack(fr, fmt)
        song_title = song_title.rstrip(zero)
        artist = artist.rstrip(zero)
        album = album.rstrip(zero)

        if comment[-1] != zero and comment[-2] == zero:
            comment = comment[:-2].rstrip(zero)
            track = comment[-1]
            return pos, Id3v11Fields._make(song_title, artist, album, year, comment, track, genre)
        else:
            comment = comment.rstrip(zero)
            return pos, Id3v1Fields._make(song_title, artist, album, year, comment, genre)

    else:
        fr.seek(125, SEEK_CUR)
        return pos, None


def read_apev2(fr: IO[bytes]) -> Tuple[int, APEv2Header]:
    pos = fr.tell()
    tag = read_or_raise(fr, 8)

    if tag != b"APETAGEX":
        fr.seek(pos)
        raise WrongSignature("No APEv2")

    header = APEv2Header._make(funpack(fr, "<LLL4sQ"))
    assert header.version in (1000, 2000)
    print(header)
    fr.seek(header.size, SEEK_CUR)
    return pos, header


def read_lyrics3v1(fr: IO[bytes]) -> Tuple[int, Dict[str, str]]:
    # see <https://id3.org/Lyrics3>
    # see <http://id3lib.sourceforge.net/id3/lyrics3.html>

    tag_begin = b"LYRICSBEGIN"
    tag_end = b"LYRICSEND"

    pos = fr.tell()
    tag = read_or_raise(fr, len(tag_begin))

    if tag != tag_begin:
        fr.seek(pos)
        raise WrongSignature("No Lyrics3 v1")

    lyrics = read_until(fr, tag_end).decode("iso-8859-1")

    return pos, lyrics


def read_lyrics3v2(fr: IO[bytes]) -> Tuple[int, Dict[str, str]]:
    # see <https://id3.org/Lyrics3v2>
    # see <http://id3lib.sourceforge.net/id3/lyrics3200.html>

    pos = fr.tell()
    tag_begin = b"LYRICSBEGIN"

    tag = read_or_raise(fr, len(tag_begin))

    if tag != tag_begin:
        fr.seek(pos)
        raise WrongSignature("No Lyrics3 v2")

    tags: Dict[str, str] = {}
    defined_fields = {"IND", "LYR", "INF", "AUT", "EAL", "EAR", "ETT", "IMG"}

    try:
        while True:
            size_, tag = funpack(fr, "6s9s")
            if tag == b"LYRICS200":
                break
            else:
                fr.seek(-15, SEEK_CUR)

            field_id_, size_ = funpack(fr, "3s6s")
            field_id = field_id_.decode("ascii")
            if field_id not in defined_fields:
                logger.warning("Found undefined field: %s", field_id)

            size = int(size_)
            information = read_or_raise(fr, size).decode("iso-8859-1")
            tag[field_id] = information  # fixme: duplicated fields are overwritten. what's the spec here?

    except Exception:
        raise RuntimeError("Invalid Lyrics v2 tag")

    return pos, tags


def read_mp3(path):
    with open(path, "rb") as fr:
        try:
            yield read_id3v2(fr)
        except RuntimeError:
            fr.seek(0)

        while True:
            try:
                frame = read_frame(fr)
                if frame is None:
                    break
                else:
                    yield frame
            except OutOfSync as e:
                fr.seek(e.pos)

                for func in [read_apev2, read_lyrics3v2, read_lyrics3v1, read_id3v1]:
                    try:
                        yield func(fr)
                        continue
                    except WrongSignature:
                        pass
                    except Empty:
                        return

                first_8_bytes = fr.read(8)

                if e.pos == 0:
                    raise MaybeNotMp3(f"File doesn't start with a valid frame: {first_8_bytes}")
                else:
                    raise InvalidFrame(f"Invalid frame found in file: {first_8_bytes}")


if __name__ == "__main__":
    import sys
    from argparse import ArgumentParser
    from os import fspath

    from genutility.args import is_dir
    from genutility.filesystem import scandir_ext
    from genutility.iter import list_except, progress

    parser = ArgumentParser()
    parser.add_argument("path", type=is_dir)
    args = parser.parse_args()

    total_count = 0
    errors_count = 0

    def progress_callback(total, length):
        return f"{errors_count} failed ({errors_count*100/total:.2f}%)"

    for path in progress(scandir_ext(args.path, {".mp3"}), extra_info_callback=progress_callback):
        total_count += 1

        exc, res = list_except(read_mp3(path))
        if exc:
            for i in res[:1] + res[-2:]:
                print(*i, file=sys.stderr)
            logging.exception("Enumerating frames of <%s> failed", fspath(path), exc_info=exc)
            errors_count += 1
            print("-----")

    print(f"{errors_count}/{total_count} files failed to parse")
