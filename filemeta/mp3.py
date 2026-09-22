import logging
import os
import struct
from enum import IntEnum
from io import BytesIO
from typing import Callable, Dict, Iterable, Iterator, List, NamedTuple, Optional, Tuple, Type, TypeVar, Union

import bitstruct
from anycrc import CRC
from bitarray import bitarray
from genutility.exceptions import ParseError
from genutility.file import BufferedBinaryIoT, Empty, PathType, read_or_raise

logger = logging.getLogger(__name__)

T_nt = TypeVar("T_nt", bound=NamedTuple)

# general exceptions


class WrongSignature(ParseError):
    pass


# helpers


def read_until(fr: BufferedBinaryIoT, delimiter: bytes) -> bytes:
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


def funpack(fr: BufferedBinaryIoT, fmt: str) -> tuple:
    num_bytes = struct.calcsize(fmt)
    return struct.unpack(fmt, read_or_raise(fr, num_bytes))


def read_bytes_to_namedtuple(
    fr: BufferedBinaryIoT, fmt: str, namedtuplecls: Type[T_nt], conv: Optional[Dict[int, Callable]] = None
) -> T_nt:
    fields = funpack(fr, fmt)

    if conv:
        fields = tuple(transform_iterable(fields, conv))

    return namedtuplecls._make(fields)


def fbitunpack(fr: BufferedBinaryIoT, fmt: str) -> Tuple[bytes, tuple]:
    num_bits = bitstruct.calcsize(fmt)
    num_bytes, remainder = divmod(num_bits, 8)
    assert remainder == 0

    raw = read_or_raise(fr, num_bytes)
    parsed = bitstruct.unpack(fmt, raw)
    return (raw, parsed)


def read_bits_to_namedtuple(
    fr: BufferedBinaryIoT, fmt: str, namedtuplecls: Type[T_nt], conv: Optional[Dict[int, Callable]] = None
) -> Tuple[bytes, T_nt]:
    raw, fields = fbitunpack(fr, fmt)

    if conv:
        fields = tuple(transform_iterable(fields, conv))

    parsed = namedtuplecls._make(fields)
    return raw, parsed


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


# named tuples


IntRows = Tuple[Tuple[int, ...], ...]
OptionalIntRows = Tuple[Tuple[Optional[int], ...], ...]
OptionalIntTupleRows = Tuple[Tuple[Optional[Tuple[int, ...]], ...], ...]
ScalefactorRows = Tuple[Tuple[Optional[Tuple[int, int, int]], ...], ...]
IntCube = Tuple[Tuple[Tuple[int, ...], ...], ...]
Int4D = Tuple[Tuple[Tuple[Tuple[int, ...], ...], ...], ...]


class Mp3Header(NamedTuple):
    sync: int
    id: ID
    layer: Layer
    protection: int
    bitrate: int
    frequency: int
    padding: int
    private: int
    mode: Mode
    mode_extension: int
    copyright: int
    home_original: int
    emphasis: int


class Mp3SideInformation(NamedTuple):
    """Layer III side information, indexed as ``[granule][channel]``.

    MPEG-1 has two granules and includes ``scfsi`` and ``preflag``. MPEG-2
    and MPEG-2.5 have one granule and omit both fields. Fields which do not
    exist for a particular block type are represented by ``None``.
    """

    main_data_begin: int
    private_bits: int
    scfsi: Optional[IntRows]
    par2_3_length: IntRows
    big_values: IntRows
    global_gain: IntRows
    scalefac_compress: IntRows
    windows_switching_flag: IntRows
    block_type: OptionalIntRows
    mixed_blockflag: OptionalIntRows
    table_select: OptionalIntTupleRows
    subblock_gain: OptionalIntTupleRows
    region0_count: OptionalIntRows
    region1_count: OptionalIntRows
    preflag: OptionalIntRows
    scalfac_scale: IntRows
    count1table_select: IntRows


class MpegAudioLayer1FrameData(NamedTuple):
    allocation: IntRows
    scalefactors: OptionalIntRows
    sample_codes: IntCube
    bound: int
    data_bits: int
    ancillary_bits: int


class MpegAudioLayer2FrameData(NamedTuple):
    allocation: IntRows
    scfsi: OptionalIntRows
    scalefactors: ScalefactorRows
    quantizer: IntRows
    sample_code_bits: IntRows
    quantizer_steps: IntRows
    sample_codes: Int4D
    sblimit: int
    bound: int
    data_bits: int
    ancillary_bits: int


class Id3v2Header(NamedTuple):
    major_version: int
    revision_number: int
    a: int
    b: int
    c: int
    d: int
    e: int
    f: int
    g: int
    h: int
    size: int


class APEv2Header(NamedTuple):
    version: int
    size: int
    item_count: int
    flags: bytes
    reserved: int


class Id3v1Fields(NamedTuple):
    song_title: bytes
    artist: bytes
    album: bytes
    year: bytes
    comment: bytes
    genre: int


class Id3v11Fields(NamedTuple):
    song_title: bytes
    artist: bytes
    album: bytes
    year: bytes
    comment: bytes
    track: int
    genre: int


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

_BITRATE_TABLES = {
    ID.MPEG_1: {
        Layer.LAYER_I: (0, 32, 64, 96, 128, 160, 192, 224, 256, 288, 320, 352, 384, 416, 448),
        Layer.LAYER_II: (0, 32, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320, 384),
        Layer.LAYER_III: (0, 32, 40, 48, 56, 64, 80, 96, 112, 128, 160, 192, 224, 256, 320),
    },
    ID.MPEG_2: {
        Layer.LAYER_I: (0, 32, 48, 56, 64, 80, 96, 112, 128, 144, 160, 176, 192, 224, 256),
        Layer.LAYER_II: (0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160),
        Layer.LAYER_III: (0, 8, 16, 24, 32, 40, 48, 56, 64, 80, 96, 112, 128, 144, 160),
    },
}

BITRATE_MAP = {
    index: {
        version: {layer: bitrates[index] for layer, bitrates in layers.items()}
        for version, layers in _BITRATE_TABLES.items()
    }
    for index in range(1, 15)
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


def get_frame_length(header: Mp3Header, free_format_size: Optional[int] = None) -> int:
    if header.bitrate == 0:
        if free_format_size is None:
            raise ParseError("Free-format frame length is not encoded in the header")
        return free_format_size

    try:
        samplerate = SAMPLE_RATE_MAP[header.frequency][header.id]
        layer = Layer(header.layer)
    except (KeyError, ValueError):
        raise Reserved(f"Found reserved value frequency={header.frequency} or id={header.id}")

    version = ID.MPEG_2 if header.id == ID.MPEG_25 else header.id
    try:
        bitrate = BITRATE_MAP[header.bitrate][version][layer]
    except (KeyError, ValueError):
        raise ParseError(f"Invalid values bitrate={header.bitrate} id={header.id} layer={header.layer}")

    if layer == Layer.LAYER_I:
        return (int(12 * bitrate * 1000 / samplerate) + header.padding) * 4
    elif version == ID.MPEG_2 and layer == Layer.LAYER_III:
        coefficient = 72
        padding = header.padding
    else:
        coefficient = 144
        padding = header.padding

    return int(coefficient * bitrate * 1000 / samplerate) + padding


def validate_header(header: Mp3Header) -> None:
    if header.sync != 2047:
        raise ParseError(f"Invalid MPEG audio sync {header.sync}")
    if header.id == ID.RESERVED:
        raise Reserved("Found reserved MPEG version")
    if header.layer == Layer.RESERVED:
        raise Reserved("Found reserved MPEG layer")
    if header.mode != Mode.JOINT_STEREO and header.mode_extension:
        raise ParseError("Mode extension is only valid for joint stereo")
    if header.bitrate:
        get_frame_length(header)


MPEG_AUDIO_HEADER_FMT = "u11 u2 u2 b1 u4 u2 u1 b1 u2 u2 b1 b1 u2"


def _read_frame_header(fr: BufferedBinaryIoT) -> Tuple[int, bytes, Mp3Header]:
    pos = fr.tell()
    raw_header, header = read_bits_to_namedtuple(
        fr, MPEG_AUDIO_HEADER_FMT, Mp3Header, {1: ID, 2: Layer, 8: Mode}
    )

    if header.sync != 2047:
        raise OutOfSync(pos, header)

    try:
        validate_header(header)
    except ParseError as e:
        raise InvalidFrame(f"Unsupported frame at pos {pos}: {e}") from e

    return pos, raw_header, header


def _free_format_headers_match(first: Mp3Header, candidate: Mp3Header) -> bool:
    return bool(
        candidate.sync == 2047
        and candidate.bitrate == 0
        and candidate.id == first.id
        and candidate.layer == first.layer
        and candidate.frequency == first.frequency
        and candidate.mode == first.mode
        and candidate.mode_extension == first.mode_extension
    )


def _get_free_format_frame_length(fr: BufferedBinaryIoT, pos: int, header: Mp3Header) -> int:
    """Infer a free-format frame size from the next matching frame header."""

    body_start = fr.tell()
    fr.seek(body_start, os.SEEK_SET)

    try:
        while True:
            candidate_pos = fr.tell()
            raw_candidate = fr.read(4)
            if len(raw_candidate) < 4:
                fr.seek(0, os.SEEK_END)
                return fr.tell() - pos

            candidate = Mp3Header._make(bitstruct.unpack(MPEG_AUDIO_HEADER_FMT, raw_candidate))
            if _free_format_headers_match(header, candidate):
                return candidate_pos - pos

            fr.seek(candidate_pos + 1, os.SEEK_SET)
    finally:
        fr.seek(body_start, os.SEEK_SET)


def _get_frame_length_for_frame(fr: BufferedBinaryIoT, pos: int, header: Mp3Header) -> int:
    if header.bitrate:
        return get_frame_length(header)
    return _get_free_format_frame_length(fr, pos, header)


def _validate_frame_end(fr: BufferedBinaryIoT, pos: int, frame_length: int) -> None:
    fr.seek(pos + frame_length - 1, os.SEEK_SET)
    read_or_raise(fr, 1)


def _read_bits(data: bytes, offset: int, size: int) -> Tuple[int, int]:
    if offset < 0 or size < 0 or offset + size > len(data) * 8:
        raise EOFError("Not enough frame bits")

    value = 0
    for index in range(offset, offset + size):
        value = value << 1 | ((data[index // 8] >> (7 - index % 8)) & 1)
    return value, offset + size


def read_sideinfo(fr: BufferedBinaryIoT, header: Mp3Header) -> Tuple[bytes, Mp3SideInformation]:
    mpeg1 = header.id == ID.MPEG_1
    channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
    granules = 2 if mpeg1 else 1

    # ISO/IEC 11172-3, 2.4.2.7 and ISO/IEC 13818-3, 2.4.2.7.
    main_data_begin_bits = 9 if mpeg1 else 8
    private_bits_width = (5 if channels == 1 else 3) if mpeg1 else (1 if channels == 1 else 2)
    scalefac_compress_width = 4 if mpeg1 else 9
    sideinfo_bits = (
        9 + private_bits_width + channels * 4 + 2 * channels * 59 if mpeg1 else 8 + private_bits_width + channels * 63
    )
    data = read_or_raise(fr, sideinfo_bits // 8)
    offset = 0

    main_data_begin, offset = _read_bits(data, offset, main_data_begin_bits)
    private_bits, offset = _read_bits(data, offset, private_bits_width)

    if mpeg1:
        scfsi_rows = []
        for _channel in range(channels):
            row = []
            for _ in range(4):
                value, offset = _read_bits(data, offset, 1)
                row.append(value)
            scfsi_rows.append(tuple(row))
        scfsi = tuple(scfsi_rows)
    else:
        scfsi = None

    par2_3_length: List[Tuple[int, ...]] = []
    big_values: List[Tuple[int, ...]] = []
    global_gain: List[Tuple[int, ...]] = []
    scalefac_compress: List[Tuple[int, ...]] = []
    windows_switching_flag: List[Tuple[int, ...]] = []
    block_type: List[Tuple[Optional[int], ...]] = []
    mixed_blockflag: List[Tuple[Optional[int], ...]] = []
    table_select: List[Tuple[Optional[Tuple[int, ...]], ...]] = []
    subblock_gain: List[Tuple[Optional[Tuple[int, ...]], ...]] = []
    region0_count: List[Tuple[Optional[int], ...]] = []
    region1_count: List[Tuple[Optional[int], ...]] = []
    preflag: List[Tuple[Optional[int], ...]] = []
    scalfac_scale: List[Tuple[int, ...]] = []
    count1table_select: List[Tuple[int, ...]] = []

    for _granule in range(granules):
        par2_3_length_row: List[int] = []
        big_values_row: List[int] = []
        global_gain_row: List[int] = []
        scalefac_compress_row: List[int] = []
        windows_switching_flag_row: List[int] = []
        block_type_row: List[Optional[int]] = []
        mixed_blockflag_row: List[Optional[int]] = []
        table_select_row: List[Optional[Tuple[int, ...]]] = []
        subblock_gain_row: List[Optional[Tuple[int, ...]]] = []
        region0_count_row: List[Optional[int]] = []
        region1_count_row: List[Optional[int]] = []
        preflag_row: List[Optional[int]] = []
        scalfac_scale_row: List[int] = []
        count1table_select_row: List[int] = []

        for _channel in range(channels):
            value, offset = _read_bits(data, offset, 12)
            par2_3_length_row.append(value)
            value, offset = _read_bits(data, offset, 9)
            big_values_row.append(value)
            value, offset = _read_bits(data, offset, 8)
            global_gain_row.append(value)
            value, offset = _read_bits(data, offset, scalefac_compress_width)
            scalefac_compress_row.append(value)
            value, offset = _read_bits(data, offset, 1)
            windows_switching_flag_row.append(value)

            if value:
                value, offset = _read_bits(data, offset, 2)
                block_type_row.append(value)
                value, offset = _read_bits(data, offset, 1)
                mixed_blockflag_row.append(value)
                table_values = []
                for _ in range(2):
                    value, offset = _read_bits(data, offset, 5)
                    table_values.append(value)
                table_select_row.append(tuple(table_values))
                gain_values = []
                for _ in range(3):
                    value, offset = _read_bits(data, offset, 3)
                    gain_values.append(value)
                subblock_gain_row.append(tuple(gain_values))
                region0_count_row.append(None)
                region1_count_row.append(None)
            else:
                block_type_row.append(None)
                mixed_blockflag_row.append(None)
                table_values = []
                for _ in range(3):
                    value, offset = _read_bits(data, offset, 5)
                    table_values.append(value)
                table_select_row.append(tuple(table_values))
                subblock_gain_row.append(None)
                value, offset = _read_bits(data, offset, 4)
                region0_count_row.append(value)
                value, offset = _read_bits(data, offset, 3)
                region1_count_row.append(value)

            if mpeg1:
                value, offset = _read_bits(data, offset, 1)
                preflag_row.append(value)
            else:
                preflag_row.append(None)
            value, offset = _read_bits(data, offset, 1)
            scalfac_scale_row.append(value)
            value, offset = _read_bits(data, offset, 1)
            count1table_select_row.append(value)

        par2_3_length.append(tuple(par2_3_length_row))
        big_values.append(tuple(big_values_row))
        global_gain.append(tuple(global_gain_row))
        scalefac_compress.append(tuple(scalefac_compress_row))
        windows_switching_flag.append(tuple(windows_switching_flag_row))
        block_type.append(tuple(block_type_row))
        mixed_blockflag.append(tuple(mixed_blockflag_row))
        table_select.append(tuple(table_select_row))
        subblock_gain.append(tuple(subblock_gain_row))
        region0_count.append(tuple(region0_count_row))
        region1_count.append(tuple(region1_count_row))
        preflag.append(tuple(preflag_row))
        scalfac_scale.append(tuple(scalfac_scale_row))
        count1table_select.append(tuple(count1table_select_row))

    assert offset == sideinfo_bits
    side_information = Mp3SideInformation(
        main_data_begin,
        private_bits,
        scfsi,
        tuple(par2_3_length),
        tuple(big_values),
        tuple(global_gain),
        tuple(scalefac_compress),
        tuple(windows_switching_flag),
        tuple(block_type),
        tuple(mixed_blockflag),
        tuple(table_select),
        tuple(subblock_gain),
        tuple(region0_count),
        tuple(region1_count),
        tuple(preflag),
        tuple(scalfac_scale),
        tuple(count1table_select),
    )

    return data, side_information


class IntegrityCheckFailed(Exception):
    pass


class MpegAudioLayer2SideInformation(NamedTuple):
    allocation: IntRows
    scfsi: OptionalIntRows
    scalefactors: ScalefactorRows


FrameSideInformation = Optional[
    Union[MpegAudioLayer1FrameData, MpegAudioLayer2FrameData, MpegAudioLayer2SideInformation, Mp3SideInformation]
]


class MpegAudioFrame(NamedTuple):
    pos: int
    header: Mp3Header
    crc: Optional[int]
    sideinfo: FrameSideInformation
_MPEG_AUDIO_CRC16 = CRC(16, 0x8005, 0xFFFF, False, False, 0)

_LAYER2_ALLOCATION_BITS = (
    (4,) * 11 + (3,) * 12 + (2,) * 4,
    (4,) * 11 + (3,) * 12 + (2,) * 7,
    (4,) * 2 + (3,) * 6,
    (4,) * 2 + (3,) * 10,
)
_LAYER2_SBLIMIT = (27, 30, 8, 12)
_MPEG2_LAYER2_ALLOCATION_BITS = (4,) * 4 + (3,) * 7 + (2,) * 19
_LAYER2_QUANTIZER_STEPS = (3, 5, 7, 9, 15, 31, 63, 127, 255, 511, 1023, 2047, 4095, 8191, 16383, 32767, 65535)
_LAYER2_QUANTIZER_BITS = (-5, -7, 3, -10, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16)
# These are the quantizer indices used by _LAYER2_QUANTIZER_BITS.  The
# allocation tables use a few special codes (17, 18, 19) which represent
# grouped 3-, 5-, and 9-level quantizers respectively.
_LAYER2_QUANTIZER_MAP_1 = (
    ((0, 0, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16),) * 3
    + ((0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16),) * 8
    + ((0, 0, 1, 2, 3, 4, 5, 16),) * 12
    + ((0, 0, 1, 16),) * 7
)
_LAYER2_QUANTIZER_MAP_3 = ((0, 0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15),) * 2 + (
    (0, 0, 1, 3, 4, 5, 6, 7),
) * 10
_LAYER2_QUANTIZER_MAP_4 = (
    ((0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14),) * 4
    + ((0, 0, 1, 3, 4, 5, 6, 7),) * 7
    + ((0, 0, 1, 3),) * 19
)
_LAYER2_QUANTIZER_MAPS = (
    _LAYER2_QUANTIZER_MAP_1[:27],
    _LAYER2_QUANTIZER_MAP_1[:30],
    _LAYER2_QUANTIZER_MAP_3[:8],
    _LAYER2_QUANTIZER_MAP_3[:12],
    _LAYER2_QUANTIZER_MAP_4,
)
_LAYER2_TABLES = (
    (
        (0, 2, 2, 2, 2, 2, 2, 0, 0, 0, 1, 1, 1, 1, 1, 0),
        (0, 2, 2, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0),
    ),
    (
        (0, 2, 2, 2, 2, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0),
        (0, 2, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
    ),
    (
        (0, 3, 3, 3, 3, 3, 3, 0, 0, 0, 1, 1, 1, 1, 1, 0),
        (0, 3, 3, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1, 0),
    ),
)


def _layer1_layout(header: Mp3Header) -> Tuple[int, int]:
    channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
    bound = 32
    if header.mode == Mode.JOINT_STEREO:
        bound = min(4 + 4 * header.mode_extension, 32)
    return bound, channels


def _mpeg1_layer2_table(header: Mp3Header) -> int:
    if header.id != ID.MPEG_1:
        raise ValueError("MPEG-1 Layer II table requested for another MPEG version")
    try:
        bitrate = BITRATE_MAP[header.bitrate][ID.MPEG_1][Layer.LAYER_II]
        sample_rate = SAMPLE_RATE_MAP[header.frequency][ID.MPEG_1]
    except (KeyError, ValueError):
        raise ParseError(
            f"Invalid MPEG-1 Layer II values bitrate={header.bitrate} frequency={header.frequency} mode={header.mode}"
        )

    channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
    channel_bitrate = bitrate / channels
    if (sample_rate == 48000 and channel_bitrate >= 56) or 56 <= channel_bitrate <= 80:
        return 0
    if sample_rate != 48000 and channel_bitrate >= 96:
        return 1
    if sample_rate != 32000 and channel_bitrate <= 48:
        return 2
    return 3


def _layer2_layout(
    header: Mp3Header,
) -> Tuple[Tuple[int, ...], Tuple[Tuple[int, ...], ...], int, int, int]:
    if header.id == ID.MPEG_1:
        table = _mpeg1_layer2_table(header)
    else:
        table = 4

    allocation_bits = _MPEG2_LAYER2_ALLOCATION_BITS if table == 4 else _LAYER2_ALLOCATION_BITS[table]
    quantizer_map = _LAYER2_QUANTIZER_MAPS[table]
    sblimit = len(allocation_bits)
    bound = sblimit
    if header.mode == Mode.JOINT_STEREO:
        bound = min(4 + 4 * header.mode_extension, sblimit)
    channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
    return allocation_bits, quantizer_map, sblimit, bound, channels


def _parse_layer1(data: bytes, header: Mp3Header) -> Tuple[MpegAudioLayer1FrameData, bytes, int]:
    bound, channels = _layer1_layout(header)
    offset = 0
    allocation = [[0] * 32 for _ in range(channels)]

    for subband in range(bound):
        for channel in range(channels):
            allocation[channel][subband], offset = _read_bits(data, offset, 4)
            if allocation[channel][subband] == 15:
                raise InvalidFrame("Reserved MPEG Layer I allocation code")
    for subband in range(bound, 32):
        value, offset = _read_bits(data, offset, 4)
        if value == 15:
            raise InvalidFrame("Reserved MPEG Layer I allocation code")
        allocation[0][subband] = value
        if channels == 2:
            allocation[1][subband] = value

    protected_bits = offset
    scalefactors: List[List[Optional[int]]] = [[None] * 32 for _ in range(channels)]
    for subband in range(bound):
        for channel in range(channels):
            if allocation[channel][subband]:
                scalefactors[channel][subband], offset = _read_bits(data, offset, 6)
    for subband in range(bound, 32):
        if allocation[0][subband]:
            for channel in range(channels):
                scalefactors[channel][subband], offset = _read_bits(data, offset, 6)

    sample_codes = [[[0] * 32 for _ in range(12)] for _ in range(channels)]
    for sample in range(12):
        for subband in range(bound):
            for channel in range(channels):
                bits = allocation[channel][subband]
                if bits:
                    sample_codes[channel][sample][subband], offset = _read_bits(data, offset, bits + 1)
        for subband in range(bound, 32):
            bits = allocation[0][subband]
            if bits:
                value, offset = _read_bits(data, offset, bits + 1)
                sample_codes[0][sample][subband] = value
                if channels == 2:
                    sample_codes[1][sample][subband] = value

    data_bits = offset
    ancillary_bits = len(data) * 8 - data_bits
    parsed = MpegAudioLayer1FrameData(
        tuple(tuple(values) for values in allocation),
        tuple(tuple(values) for values in scalefactors),
        tuple(tuple(tuple(values) for values in channel) for channel in sample_codes),
        bound,
        data_bits,
        ancillary_bits,
    )
    protected_data = data[: (protected_bits + 7) // 8]
    return parsed, protected_data, protected_bits


def _parse_layer2(data: bytes, header: Mp3Header) -> Tuple[MpegAudioLayer2FrameData, bytes, int]:
    allocation_bits, quantizer_map, sblimit, bound, channels = _layer2_layout(header)
    offset = 0
    allocation = [[0] * sblimit for _ in range(channels)]
    quantizer = [[0] * sblimit for _ in range(channels)]
    sample_code_bits = [[0] * sblimit for _ in range(channels)]
    quantizer_steps = [[0] * sblimit for _ in range(channels)]

    for subband, width in enumerate(allocation_bits):
        count = channels if subband < bound else 1
        values = []
        for _ in range(count):
            value, offset = _read_bits(data, offset, width)
            values.append(value)
        if count == 1 and channels == 2:
            values *= 2
        for channel, value in enumerate(values):
            allocation[channel][subband] = value
            if value:
                qindex = quantizer_map[subband][value]
                quantizer[channel][subband] = qindex
                sample_code_bits[channel][subband] = abs(_LAYER2_QUANTIZER_BITS[qindex])
                quantizer_steps[channel][subband] = _LAYER2_QUANTIZER_STEPS[qindex]

    protected_bits = offset
    scfsi: List[List[Optional[int]]] = [[None] * sblimit for _ in range(channels)]
    for subband in range(sblimit):
        for channel in range(channels):
            if allocation[channel][subband]:
                scfsi[channel][subband], offset = _read_bits(data, offset, 2)
    protected_bits = offset

    scalefactors: List[List[Optional[Tuple[int, int, int]]]] = [[None] * sblimit for _ in range(channels)]
    for subband in range(sblimit):
        for channel in range(channels):
            if not allocation[channel][subband]:
                continue
            selection = scfsi[channel][subband]
            assert selection is not None
            first, offset = _read_bits(data, offset, 6)
            if selection == 0:
                second, offset = _read_bits(data, offset, 6)
                third, offset = _read_bits(data, offset, 6)
            elif selection == 1:
                third, offset = _read_bits(data, offset, 6)
                second = first
            elif selection == 2:
                second = third = first
            else:
                second, offset = _read_bits(data, offset, 6)
                third = second
            scalefactors[channel][subband] = (first, second, third)

    sample_codes: List[List[List[Tuple[int, ...]]]] = [
        [[() for _ in range(sblimit)] for _ in range(12)] for _ in range(channels)
    ]
    codes: Tuple[int, ...]
    for scalefactor_index in range(3):
        for group_index in range(4):
            sample_group = scalefactor_index * 4 + group_index
            for subband in range(bound):
                for channel in range(channels):
                    qindex = quantizer[channel][subband]
                    if not allocation[channel][subband]:
                        continue
                    bits = _LAYER2_QUANTIZER_BITS[qindex]
                    if bits < 0:
                        value, offset = _read_bits(data, offset, -bits)
                        codes = (value,)
                    else:
                        values = []
                        for _ in range(3):
                            value, offset = _read_bits(data, offset, bits)
                            values.append(value)
                        codes = tuple(values)
                    sample_codes[channel][sample_group][subband] = codes
            for subband in range(bound, sblimit):
                qindex = quantizer[0][subband]
                if not allocation[0][subband]:
                    continue
                bits = _LAYER2_QUANTIZER_BITS[qindex]
                if bits < 0:
                    value, offset = _read_bits(data, offset, -bits)
                    codes = (value,)
                else:
                    values = []
                    for _ in range(3):
                        value, offset = _read_bits(data, offset, bits)
                        values.append(value)
                    codes = tuple(values)
                sample_codes[0][sample_group][subband] = codes
                sample_codes[1][sample_group][subband] = codes

    data_bits = offset
    ancillary_bits = len(data) * 8 - data_bits
    parsed = MpegAudioLayer2FrameData(
        tuple(tuple(values) for values in allocation),
        tuple(tuple(values) for values in scfsi),
        tuple(tuple(values) for values in scalefactors),
        tuple(tuple(values) for values in quantizer),
        tuple(tuple(values) for values in sample_code_bits),
        tuple(tuple(values) for values in quantizer_steps),
        tuple(tuple(tuple(values) for values in channel) for channel in sample_codes),
        sblimit,
        bound,
        data_bits,
        ancillary_bits,
    )
    protected_data = data[: (protected_bits + 7) // 8]
    return parsed, protected_data, protected_bits


def _crc16_bits(data: bytes, size: int) -> int:
    full_bytes, remainder = divmod(size, 8)
    if not remainder:
        return int(_MPEG_AUDIO_CRC16.calc(data[:full_bytes]))

    bits = bitarray(endian="big")
    bits.frombytes(data[: full_bytes + 1])
    return int(_MPEG_AUDIO_CRC16.calc_bits(bits[:size]))


def _mpeg1_layer2_layout(header: Mp3Header) -> Tuple[Tuple[int, ...], int, int, int]:
    if header.id != ID.MPEG_1:
        raise ValueError("MPEG-1 Layer II side information is not available for this frame")

    table = _mpeg1_layer2_table(header)
    allocation_bits = _LAYER2_ALLOCATION_BITS[table]
    sblimit = _LAYER2_SBLIMIT[table]

    bound = sblimit
    if header.mode == Mode.JOINT_STEREO:
        bound = min(4 + 4 * header.mode_extension, sblimit)

    channels = 2 if header.mode != Mode.SINGLE_CHANNEL else 1
    return allocation_bits, sblimit, bound, channels


def _read_mpeg1_layer2_sideinfo(
    fr: BufferedBinaryIoT, header: Mp3Header
) -> Tuple[bytes, int, MpegAudioLayer2SideInformation]:
    allocation_bits, sblimit, bound, channels = _mpeg1_layer2_layout(header)
    max_bits = sum(allocation_bits[:bound]) * channels
    max_bits += sum(allocation_bits[bound:sblimit])
    max_bits += 2 * channels * sblimit
    max_bits += 18 * channels * sblimit
    data = read_or_raise(fr, (max_bits + 7) // 8)

    offset = 0
    allocations: List[List[int]] = [[] for _ in range(channels)]
    for subband, width in enumerate(allocation_bits[:sblimit]):
        count = channels if subband < bound else 1
        values = []
        for _ in range(count):
            value, offset = _read_bits(data, offset, width)
            values.append(value)
        if count == 1 and channels == 2:
            values *= 2
        for channel, value in enumerate(values):
            allocations[channel].append(value)

    protected_bits = offset
    scfsi: List[List[Optional[int]]] = [[None] * sblimit for _ in range(channels)]
    for subband in range(sblimit):
        for channel in range(channels):
            if allocations[channel][subband]:
                scfsi[channel][subband], offset = _read_bits(data, offset, 2)
    protected_bits = offset

    scalefactors: List[List[Optional[Tuple[int, int, int]]]] = [[None] * sblimit for _ in range(channels)]
    for subband in range(sblimit):
        for channel in range(channels):
            if not allocations[channel][subband]:
                continue

            selection = scfsi[channel][subband]
            assert selection is not None
            first, offset = _read_bits(data, offset, 6)
            if selection == 0:
                second, offset = _read_bits(data, offset, 6)
                third, offset = _read_bits(data, offset, 6)
            elif selection == 1:
                third, offset = _read_bits(data, offset, 6)
                second = first
            elif selection == 2:
                second = third = first
            else:
                second, offset = _read_bits(data, offset, 6)
                third = second
            scalefactors[channel][subband] = (first, second, third)

    sideinfo = MpegAudioLayer2SideInformation(
        tuple(tuple(values) for values in allocations),
        tuple(tuple(values) for values in scfsi),
        tuple(tuple(values) for values in scalefactors),
    )
    return data, protected_bits, sideinfo


def _read_layer1_crc_data(fr: BufferedBinaryIoT, header: Mp3Header) -> Tuple[bytes, int]:
    bound = 32
    if header.mode == Mode.JOINT_STEREO:
        bound = 4 + 4 * header.mode_extension

    channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
    protected_bits = 4 * (channels * bound + 32 - bound)
    data = read_or_raise(fr, (protected_bits + 7) // 8)
    return data, protected_bits


def _read_mpeg2_layer2_crc_data(fr: BufferedBinaryIoT, header: Mp3Header) -> Tuple[bytes, int]:
    allocation_bits = _MPEG2_LAYER2_ALLOCATION_BITS
    sblimit = len(allocation_bits)
    bound = sblimit
    if header.mode == Mode.JOINT_STEREO:
        bound = min(4 + 4 * header.mode_extension, sblimit)

    channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
    allocation_size = sum(allocation_bits[:bound]) * channels + sum(allocation_bits[bound:])
    max_bits = allocation_size + 2 * channels * sblimit
    data = read_or_raise(fr, (max_bits + 7) // 8)

    offset = 0
    allocations: List[List[int]] = [[] for _ in range(channels)]
    for subband, width in enumerate(allocation_bits):
        count = channels if subband < bound else 1
        values = []
        for _ in range(count):
            value, offset = _read_bits(data, offset, width)
            values.append(value)
        if count == 1 and channels == 2:
            values *= 2
        for channel, value in enumerate(values):
            allocations[channel].append(value)

    for subband in range(sblimit):
        for channel in range(channels):
            if allocations[channel][subband]:
                _, offset = _read_bits(data, offset, 2)

    return data, offset


def read_frame(
    fr: BufferedBinaryIoT, content: bool = False, verify: bool = False
) -> Optional[Tuple[Optional[bytes], MpegAudioFrame]]:
    if verify and not content:
        raise ValueError("verify requires content")

    raw: List[bytes] = []

    try:
        pos, raw_header, header = _read_frame_header(fr)
        raw.append(raw_header)
    except (Empty, EOFError):  # end of file or incomplete trailing header
        return None

    sideinfo: FrameSideInformation = None

    try:
        if not header.protection:  # False means CRC data available
            raw_crc = read_or_raise(fr, 2)
            raw.append(raw_crc)
            crc: Optional[int] = int.from_bytes(raw_crc, "big")
        else:
            crc = None

        frame_length = _get_frame_length_for_frame(fr, pos, header)
        if content:
            raw_content = read_or_raise(fr, frame_length - sum(map(len, raw)))
            if header.layer == Layer.LAYER_I:
                sideinfo, protected_data, protected_bits = _parse_layer1(raw_content, header)
                if crc is not None and verify:
                    calculated_crc = _crc16_bits(raw_header[2:] + protected_data, 16 + protected_bits)
                    if crc != calculated_crc:
                        raise IntegrityCheckFailed(
                            f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}"
                        )
            elif header.layer == Layer.LAYER_II:
                sideinfo, protected_data, protected_bits = _parse_layer2(raw_content, header)
                if crc is not None and verify:
                    calculated_crc = _crc16_bits(raw_header[2:] + protected_data, 16 + protected_bits)
                    if crc != calculated_crc:
                        raise IntegrityCheckFailed(
                            f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}"
                        )
            else:
                raw_sideinfo, sideinfo = read_sideinfo(BytesIO(raw_content), header)
                if crc is not None and verify:
                    calculated_crc = _crc16_bits(raw_header[2:] + raw_sideinfo, 16 + len(raw_sideinfo) * 8)
                    if crc != calculated_crc:
                        raise IntegrityCheckFailed(
                            f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}"
                        )
            raw_out: Optional[bytes] = b"".join(raw) + raw_content
            return raw_out, MpegAudioFrame(pos, header, crc, sideinfo)

        if header.layer == Layer.LAYER_II:
            if header.id == ID.MPEG_1:
                raw_sideinfo, crc_bits, sideinfo = _read_mpeg1_layer2_sideinfo(fr, header)
                raw.append(raw_sideinfo)
                if crc is not None and verify:
                    calculated_crc = _crc16_bits(raw_header[2:] + raw_sideinfo, 16 + crc_bits)
                    if crc != calculated_crc:
                        raise IntegrityCheckFailed(
                            f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}"
                        )
            elif crc is not None and verify:
                raw_crc_data, crc_bits = _read_mpeg2_layer2_crc_data(fr, header)
                raw.append(raw_crc_data)
                calculated_crc = _crc16_bits(raw_header[2:] + raw_crc_data, 16 + crc_bits)
                if crc != calculated_crc:
                    raise IntegrityCheckFailed(f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}")
        elif header.layer == Layer.LAYER_I:
            if crc is not None and verify:
                raw_crc_data, crc_bits = _read_layer1_crc_data(fr, header)
                raw.append(raw_crc_data)
                calculated_crc = _crc16_bits(raw_header[2:] + raw_crc_data, 16 + crc_bits)
                if crc != calculated_crc:
                    raise IntegrityCheckFailed(f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}")
        else:
            raw_sideinfo, sideinfo = read_sideinfo(fr, header)
            raw.append(raw_sideinfo)

            if crc is not None and verify:
                if header.layer != Layer.LAYER_III:
                    raise ValueError(
                        f"Integrity check is only supported for layer 3 files currently, not {Layer(header.layer).name}"
                    )

                calculated_crc = _crc16_bits(raw_header[2:] + raw_sideinfo, 16 + len(raw_sideinfo) * 8)
                if crc != calculated_crc:
                    raise IntegrityCheckFailed(f"CRC check failed, stored crc {crc} != calculated crc {calculated_crc}")

        # main_data, ancillary data

        raw_out = None
        _validate_frame_end(fr, pos, frame_length)
    except (Empty, EOFError) as e:
        raise InvalidFrame(f"Truncated frame at pos {pos}") from e

    return raw_out, MpegAudioFrame(pos, header, crc, sideinfo)


def find_first_valid_frame(fr: BufferedBinaryIoT) -> Optional[int]:
    """Find a complete MPEG audio frame and leave the stream at its offset."""

    while True:
        pos = fr.tell()
        try:
            _, _, header = _read_frame_header(fr)
        except (Empty, EOFError):
            return None
        except ParseError:
            fr.seek(pos + 1, os.SEEK_SET)
            continue

        try:
            frame_length = _get_frame_length_for_frame(fr, pos, header)
            _validate_frame_end(fr, pos, frame_length)
        except (ParseError, Empty, EOFError):
            fr.seek(pos + 1, os.SEEK_SET)
            continue

        fr.seek(pos, os.SEEK_SET)
        return pos


Id3v2Fields = tuple
Id3v2Tag = Tuple[int, Id3v2Header, Optional[Id3v2Fields]]


def read_id3v2(fr: BufferedBinaryIoT, parse: bool = False) -> Id3v2Tag:
    pos = fr.tell()

    if fr.read(3) != b"ID3":
        raise WrongSignature("No ID3v2")

    fmt = ">u8 u8 b1 b1 b1 b1 b1 b1 b1 b1 r32"
    raw, id3_header = read_bits_to_namedtuple(fr, fmt, Id3v2Header, {10: id3_sync_safe_to_int})

    # If the 0x10 bit of byte 5 is set, let OFFSET = OFFSET + 10 (for the footer).

    if parse:
        raise RuntimeError("Not implemented yet")
        # return pos, id3_header, Id3v2Fields._make()

    else:
        fr.seek(id3_header.size, os.SEEK_CUR)
        return pos, id3_header, None


def read_id3v1(fr: BufferedBinaryIoT, parse: bool = True) -> Tuple[int, Optional[Union[Id3v1Fields, Id3v11Fields]]]:
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
            return pos, Id3v11Fields(song_title, artist, album, year, comment, track, genre)
        else:
            comment = comment.rstrip(zero)
            return pos, Id3v1Fields(song_title, artist, album, year, comment, genre)

    else:
        fr.seek(125, os.SEEK_CUR)
        return pos, None


def read_apev2(fr: BufferedBinaryIoT) -> Tuple[int, APEv2Header]:
    pos = fr.tell()
    tag = read_or_raise(fr, 8)

    if tag != b"APETAGEX":
        fr.seek(pos)
        raise WrongSignature("No APEv2")

    header = APEv2Header._make(funpack(fr, "<LLL4sQ"))
    if header.version not in (1000, 2000):
        raise ParseError(f"Unsupported APEv2 version {header.version}")
    print(header)
    fr.seek(header.size, os.SEEK_CUR)
    return pos, header


def read_lyrics3v1(fr: BufferedBinaryIoT) -> Tuple[int, str]:
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


def read_lyrics3v2(fr: BufferedBinaryIoT) -> Tuple[int, Dict[str, str]]:
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
                fr.seek(-15, os.SEEK_CUR)

            field_id_, size_ = funpack(fr, "3s6s")
            field_id = field_id_.decode("ascii")
            if field_id not in defined_fields:
                logger.warning("Found undefined field: %s", field_id)

            size = int(size_)
            information = read_or_raise(fr, size).decode("iso-8859-1")
            tags[field_id] = information  # fixme: duplicated fields are overwritten. what's the spec here?

    except Exception:
        raise RuntimeError("Invalid Lyrics v2 tag")

    return pos, tags


Tag = Union[
    Id3v2Tag,
    Tuple[int, APEv2Header],
    Tuple[int, str],
    Tuple[int, Dict[str, str]],
    Tuple[int, Optional[Union[Id3v1Fields, Id3v11Fields]]],
]
FramesOrTags = Union[MpegAudioFrame, Tag]


class NoTagsFound(Exception):
    pass


def read_tags(fr: BufferedBinaryIoT) -> Tuple[None, Tag]:
    funcs: List[Callable[[BufferedBinaryIoT], Tag]] = [read_apev2, read_lyrics3v2, read_lyrics3v1, read_id3v1]
    for func in funcs:
        try:
            return None, func(fr)
        except WrongSignature:
            pass

    raise NoTagsFound()


def read_mp3(
    path: PathType, content: bool = False, verify: bool = False
) -> Iterator[Tuple[Optional[bytes], FramesOrTags]]:
    with open(path, "rb") as fr:
        try:
            yield None, read_id3v2(fr)
        except (WrongSignature, RuntimeError):
            fr.seek(0)

        while True:
            try:
                frame = read_frame(fr, content, verify)
                if frame is None:
                    break
                else:
                    yield frame
            except OutOfSync as e:
                fr.seek(e.pos)

                try:
                    yield read_tags(fr)
                except Empty:
                    return
                except NoTagsFound:
                    first_8_bytes = fr.read(8)
                    if e.pos == 0:
                        raise MaybeNotMp3(f"File doesn't start with a valid frame: {first_8_bytes!r}")
                    else:
                        raise InvalidFrame(f"Invalid frame found in file at pos {e.pos}: {first_8_bytes!r}")


def copy_mpeg_audio(in_path: PathType, out_path: PathType) -> None:
    with open(in_path, "rb") as fr, open(out_path, "wb") as fw:
        start = find_first_valid_frame(fr)
        if start is None:
            raise MaybeNotMp3(f"File doesn't contain a valid MPEG audio frame: {os.fspath(in_path)!r}")
        if start != 0:
            logger.warning(
                "Found and skipped %d bytes of invalid data at the beginning of the file %s",
                start,
                os.fspath(in_path),
            )

        try:
            while True:
                result = read_frame(fr, content=True, verify=True)
                if result is None:
                    break

                raw, parsed_frame = result
                assert isinstance(raw, bytes), type(raw)
                assert isinstance(parsed_frame, MpegAudioFrame), type(parsed_frame)
                fw.write(raw)  # buffered
        except (InvalidFrame, OutOfSync):
            print("stopped at invalid frame")


if __name__ == "__main__":
    import sys
    from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
    from pathlib import Path

    from genutility.args import is_dir
    from genutility.filesystem import scandir_error_log_warning, scandir_ext
    from genutility.iter import list_except
    from genutility.rich import Progress
    from rich.progress import Progress as RichProgress
    from rich.progress import TaskProgressColumn, TextColumn, TimeElapsedColumn

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", type=is_dir)
    parser.add_argument("--action", choices=("read", "copy"), required=True)
    parser.add_argument("--copy-suffix", default=".copy", help="suffix to add when copying files")
    args = parser.parse_args()

    total_count = 0
    errors_count = 0

    progress = RichProgress(
        TextColumn("{task.completed}{task.description}"),
        TaskProgressColumn(show_speed=True),
        TimeElapsedColumn(),
        refresh_per_second=1,
    )
    with progress:
        p = Progress(progress)
        for entry in p.track(
            scandir_ext(args.path, {".mp3", ".mp2"}, errorfunc=scandir_error_log_warning), description=" files scanned"
        ):
            total_count += 1

            if args.action == "read":
                exc, res = list_except(p.track(read_mp3(entry, True, True), description=" frames processed"))
                if exc:
                    for i in res[:1] + res[-2:]:
                        print(*i, file=sys.stderr)
                    logging.exception("Enumerating frames of <%s> failed", os.fspath(entry), exc_info=exc)
                    errors_count += 1
                    print("-----")
            elif args.action == "copy":
                path = Path(entry)
                suffix = f"{args.copy_suffix}{path.suffix}"
                if path.name.endswith(suffix):
                    print("skipping copy file")
                    continue

                outpath = path.with_suffix(suffix)
                if outpath.exists():
                    print("skipping already copied file")
                    continue

                try:
                    copy_mpeg_audio(path, outpath)
                except Exception as e:
                    logging.exception("Copying frames of <%s> failed", os.fspath(path), exc_info=e)
                    errors_count += 1

    print(f"{errors_count}/{total_count} files failed to parse")
