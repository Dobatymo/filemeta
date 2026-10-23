"""MPEG framing with separately validated audio and metadata content.

Unbounded or truncated frames and tag envelopes raise immediately.
Unrecognized spans are preserved as junk or zero padding objects.
Audio and tag content is validated explicitly through immutable objects.
Not every tag content schema is covered.
Audio decoding is outside this parser's scope.
"""

import logging
import os
import re
import struct
import zlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, replace
from enum import IntEnum
from io import BytesIO
from tempfile import NamedTemporaryFile
from typing import (
    Callable,
    ClassVar,
    Dict,
    Generic,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import bitstruct
import importlib_resources
from anycrc import CRC
from bitarray import bitarray
from genutility.exceptions import ParseError
from genutility.file import BufferedBinaryIoT, Empty, PathType, read_or_raise
from genutility.json import read_json

logger = logging.getLogger(__name__)

T_nt = TypeVar("T_nt", bound=NamedTuple)
T_metadata = TypeVar("T_metadata")

# general exceptions


class WrongSignature(ParseError):
    pass


# helpers


def read_until(fr: BufferedBinaryIoT, delimiter: bytes) -> bytes:
    ret: List[int] = []
    delim = list(delimiter)
    delim_len = len(delimiter)

    while ret[-delim_len:] != delim:
        ret.append(read_or_raise(fr, 1)[0])

    return bytes(ret[:-delim_len])


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
    if len(sync_safe) != 4 or any(value & 0x80 for value in sync_safe):
        raise ParseError("Invalid ID3 synchsafe integer")
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
    if header.frequency not in SAMPLE_RATE_MAP:
        raise Reserved("Found reserved sampling frequency")
    if header.emphasis == 2:
        raise Reserved("Found reserved emphasis")
    if header.id == ID.MPEG_1 and header.layer == Layer.LAYER_II:
        if (header.bitrate in (1, 2, 3, 5) and header.mode != Mode.SINGLE_CHANNEL) or (
            header.bitrate >= 11 and header.mode == Mode.SINGLE_CHANNEL
        ):
            raise InvalidFrame("Forbidden MPEG-1 Layer II bitrate/mode combination")
    if header.bitrate:
        get_frame_length(header)


MPEG_AUDIO_HEADER_FMT = "u11 u2 u2 b1 u4 u2 u1 b1 u2 u2 b1 b1 u2"


def _read_frame_header(fr: BufferedBinaryIoT) -> Tuple[int, bytes, Mp3Header]:
    pos = fr.tell()
    raw_header, header = read_bits_to_namedtuple(fr, MPEG_AUDIO_HEADER_FMT, Mp3Header, {1: ID, 2: Layer, 8: Mode})

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
        and candidate.emphasis != 2
    )


def _get_free_format_frame_length(fr: BufferedBinaryIoT, pos: int, header: Mp3Header) -> int:
    """Infer a slot-aligned size confirmed by another frame or the end tags."""

    body_start = fr.tell()
    fr.seek(body_start, os.SEEK_SET)

    try:
        while True:
            candidate_pos = fr.tell()
            raw_candidate = fr.read(4)
            if len(raw_candidate) < 4:
                # The last frame must retain the stream's fixed size, allowing for padding.
                for previous_pos in range(pos - 4, -1, -1):
                    fr.seek(previous_pos)
                    previous = Mp3Header._make(bitstruct.unpack(MPEG_AUDIO_HEADER_FMT, fr.read(4)))
                    if _free_format_headers_match(header, previous):
                        slot = 4 if header.layer == Layer.LAYER_I else 1
                        length = pos - previous_pos + slot * (header.padding - previous.padding)
                        if length % slot == 0 and _free_format_boundary(fr, pos + length, header):
                            return length
                raise InvalidFrame("Cannot infer a free-format frame length without another frame")

            candidate = Mp3Header._make(bitstruct.unpack(MPEG_AUDIO_HEADER_FMT, raw_candidate))
            if _free_format_headers_match(header, candidate):
                length = candidate_pos - pos
                slot = 4 if header.layer == Layer.LAYER_I else 1
                following = candidate_pos + length + slot * (candidate.padding - header.padding)
                if length % slot == 0 and _free_format_boundary(fr, following, header):
                    return length

            fr.seek(candidate_pos + 1, os.SEEK_SET)
    finally:
        fr.seek(body_start, os.SEEK_SET)


def _free_format_boundary(fr: BufferedBinaryIoT, pos: int, header: Mp3Header) -> bool:
    fr.seek(0, os.SEEK_END)
    end = fr.tell()
    if pos > end:
        return False
    fr.seek(pos)
    raw = fr.read(4)
    if len(raw) == 4:
        candidate = Mp3Header._make(bitstruct.unpack(MPEG_AUDIO_HEADER_FMT, raw))
        if _free_format_headers_match(header, candidate):
            return True
    fr.seek(pos)
    try:
        while fr.tell() < end:
            read_tags(fr)
        return fr.tell() == end
    except (ParseError, NoTagsFound, Empty, EOFError, ValueError):
        return False


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


def _validate_layer3_sideinfo(info: Mp3SideInformation) -> None:
    granules = len(info.big_values)
    for channel in range(len(info.big_values[0])):
        short = any(info.block_type[gr][channel] == 2 for gr in range(granules))
        if short and info.scfsi is not None and any(info.scfsi[channel]):
            raise InvalidFrame("Layer III short blocks cannot reuse scalefactors")
        for gr in range(granules):
            if info.big_values[gr][channel] > 288:
                raise InvalidFrame("Layer III big_values exceeds 288 pairs")
            if info.block_type[gr][channel] == 0:
                raise InvalidFrame("Reserved Layer III switched block type")
            tables = info.table_select[gr][channel]
            assert tables is not None
            if any(table in (4, 14) for table in tables):
                raise InvalidFrame("Unused Layer III Huffman table")
            r0, r1 = info.region0_count[gr][channel], info.region1_count[gr][channel]
            if r0 is not None and r1 is not None and r0 + r1 > 20:
                raise InvalidFrame("Layer III regions exceed the scalefactor bands")


class IntegrityCheckFailed(Exception):
    pass


class MpegAudioLayer2SideInformation(NamedTuple):
    allocation: IntRows
    scfsi: OptionalIntRows
    scalefactors: ScalefactorRows


FrameSideInformation = Optional[
    Union[MpegAudioLayer1FrameData, MpegAudioLayer2FrameData, MpegAudioLayer2SideInformation, Mp3SideInformation]
]
AudioFrameData = Union[MpegAudioLayer1FrameData, MpegAudioLayer2FrameData, Mp3SideInformation]


@dataclass(frozen=True)
class MpegAudioFrame:
    """A bounded frame, optionally retaining content for later validation.

    With content=True, iter_mp3/read_mp3 include reservoir context for Huffman checks.
    Standalone read_frame results must use validate_frame() for Layer III.
    """

    pos: int
    header: Mp3Header
    crc: Optional[int]
    size: int
    data: Optional[bytes] = field(repr=False)
    _reservoir: Optional[bytes] = field(default=None, repr=False)
    _overlap: bool = field(default=False, repr=False)

    @property
    def end(self) -> int:
        return self.pos + self.size

    def raw(self) -> bytes:
        """Return the original frame bytes, without copied reservoir context."""
        if self.data is None:
            raise ValueError("Audio bytes require reading with content=True")
        return self.data

    def _parse(self, stream: bool) -> AudioFrameData:
        if self.data is None:
            raise ValueError("Audio content validation requires reading with content=True")
        body = self.data[4 + (2 if self.crc is not None else 0) :]
        parsed: AudioFrameData
        if self.header.layer == Layer.LAYER_I:
            parsed, protected, protected_bits = _parse_layer1(body, self.header)
        elif self.header.layer == Layer.LAYER_II:
            parsed, protected, protected_bits = _parse_layer2(body, self.header)
        else:
            protected, parsed = read_sideinfo(BytesIO(body), self.header)
            protected_bits = len(protected) * 8
            _validate_layer3_sideinfo(parsed)
        if self.crc is not None:
            calculated = _crc16_bits(self.data[2:4] + protected, 16 + protected_bits)
            if self.crc != calculated:
                raise IntegrityCheckFailed(f"CRC check failed, stored crc {self.crc} != calculated crc {calculated}")
        if isinstance(parsed, Mp3SideInformation) and stream:
            if self._reservoir is None:
                raise ValueError("Layer III stream validation requires iter_mp3/read_mp3; use validate_frame() instead")
            if parsed.main_data_begin > len(self._reservoir):
                raise InvalidFrame("Layer III main_data_begin refers to unavailable reservoir data")
            if self._overlap:
                raise InvalidFrame("Layer III main data overlaps previously used data")
            _validate_layer3_main(self._reservoir + body[len(protected) :], self.header, parsed)
        return parsed

    def _checked_parse(self, stream: bool) -> AudioFrameData:
        try:
            return self._parse(stream)
        except (Empty, EOFError) as e:
            raise InvalidFrame(f"Truncated coded audio data at pos {self.pos}") from e

    def parse(self) -> AudioFrameData:
        """Validate content including stream references and return parsed fields."""
        return self._checked_parse(True)

    def validate(self) -> None:
        """Validate content; Layer III requires context from iter_mp3/read_mp3."""
        self.parse()

    def validate_frame(self) -> None:
        """Check frame-local fields and CRC, without reservoir/Huffman checks."""
        self._checked_parse(False)

    @property
    def sideinfo(self) -> AudioFrameData:
        """Inspect fields without CRC or stream validation; Layer III reads only sideinfo."""
        body = self.raw()[4 + (2 if self.crc is not None else 0) :]
        try:
            if self.header.layer == Layer.LAYER_I:
                return _parse_layer1(body, self.header)[0]
            if self.header.layer == Layer.LAYER_II:
                return _parse_layer2(body, self.header)[0]
            return read_sideinfo(BytesIO(body), self.header)[1]
        except (Empty, EOFError) as e:
            raise InvalidFrame(f"Truncated coded audio data at pos {self.pos}") from e


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
    if header.bitrate == 0:
        return 0 if header.frequency == 1 else 1
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

    if any(value == 63 for row in scalefactors for value in row):
        raise InvalidFrame("Undefined Layer I scalefactor 63")

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
            if 63 in (first, second, third):
                raise InvalidFrame("Undefined Layer II scalefactor 63")

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


# Numeric codebooks and band widths are from the CC0 minimp3 tables; provenance is in the data file.
_LAYER3_DATA = read_json(importlib_resources.files(__package__).joinpath("data/mp3-huffman.json"))
_LAYER3_BOOKS = {
    int(number): {int("1" + code, 2): value for code, value in book.items()}
    for number, book in _LAYER3_DATA["books"].items()
}


class _Layer3Bits:
    def __init__(self, data: bytes):
        self.data = data
        self.pos = 0
        self.limit = len(data) * 8

    def read(self, size: int) -> int:
        if size < 0 or self.pos + size > self.limit:
            raise InvalidFrame("Layer III coded data exceeds its bit budget")
        if not size:
            return 0
        byte, shift = divmod(self.pos, 8)
        length = (shift + size + 7) // 8
        value = int.from_bytes(self.data[byte : byte + length], "big")
        self.pos += size
        return (value >> (length * 8 - shift - size)) & ((1 << size) - 1)

    def symbol(self, table: int) -> int:
        book = _LAYER3_BOOKS[table]
        code = 1
        while code not in book:
            code = code * 2 + self.read(1)
        return int(book[code])


def _layer3_scalefactor_bits(header: Mp3Header, info: Mp3SideInformation, gr: int, ch: int) -> int:
    short = info.block_type[gr][ch] == 2
    layout = (1 if info.mixed_blockflag[gr][ch] else 2) if short else 0
    partitions = _LAYER3_DATA["partitions"][layout]
    compressed = info.scalefac_compress[gr][ch]
    if header.id == ID.MPEG_1:
        decoded = _LAYER3_DATA["scalefac_decode"][compressed]
        widths = [decoded >> 2, decoded >> 2, decoded & 3, decoded & 3]
        reuse = info.scfsi[ch] if gr and info.scfsi is not None and not short else (0, 0, 0, 0)
        return sum(partitions[i] * widths[i] for i in range(4) if not reuse[i])
    intensity = header.mode == Mode.JOINT_STEREO and header.mode_extension & 1 and ch == 1
    if intensity:
        compressed >>= 1
        radices = ((5, 6, 6, 1), (4, 4, 4, 1), (4, 3, 1, 1))
    else:
        radices = ((5, 5, 4, 4), (5, 5, 4, 1), (4, 3, 1, 1))
    for group, radix in enumerate(radices):
        product = radix[0] * radix[1] * radix[2] * radix[3]
        if compressed < product:
            widths = [0] * 4
            for i in range(3, -1, -1):
                compressed, widths[i] = divmod(compressed, radix[i])
            start = 4 * (group + 1 + (3 if intensity else 0))
            return sum(partitions[start + i] * widths[i] for i in range(4))
        compressed -= product
    raise InvalidFrame("Invalid Layer III scalefactor compression")


def _validate_layer3_main(data: bytes, header: Mp3Header, info: Mp3SideInformation) -> None:
    bits = _Layer3Bits(data)
    sample_index = header.frequency + (6 if header.id == ID.MPEG_1 else 3 if header.id == ID.MPEG_2 else 0)
    sample_index -= bool(sample_index)
    for gr, lengths in enumerate(info.par2_3_length):
        for ch, length in enumerate(lengths):
            end = bits.pos + length
            if end > len(data) * 8:
                raise InvalidFrame("Layer III main-data length exceeds available reservoir and frame data")
            bits.limit = end
            scale_bits = _layer3_scalefactor_bits(header, info, gr, ch)
            if bits.pos + scale_bits > end:
                raise InvalidFrame("Layer III scalefactors exceed part2_3_length")
            bits.pos += scale_bits
            block = info.block_type[gr][ch]
            if block == 2:
                kind = "mixed_bands" if info.mixed_blockflag[gr][ch] else "short_bands"
                widths = _LAYER3_DATA[kind][sample_index]
                first = 8 if info.mixed_blockflag[gr][ch] else 9
                boundaries = [sum(widths[:first]), 576]
            elif block is not None:
                widths = _LAYER3_DATA["long_bands"][sample_index]
                boundaries = [sum(widths[:8]), 576]
            else:
                widths = _LAYER3_DATA["long_bands"][sample_index]
                r0, r1 = info.region0_count[gr][ch], info.region1_count[gr][ch]
                assert r0 is not None and r1 is not None
                boundaries = [sum(widths[: r0 + 1]), sum(widths[: r0 + r1 + 2]), 576]
            tables = info.table_select[gr][ch]
            assert tables is not None
            pairs = info.big_values[gr][ch]
            region = 0
            for pair in range(pairs):
                while region < len(boundaries) - 1 and pair * 2 >= boundaries[region]:
                    region += 1
                table = tables[region]
                book = 16 if 16 <= table < 24 else 24 if table >= 24 else table
                symbol = bits.symbol(book)
                for value in (symbol & 15, symbol >> 4):
                    if value == 15:
                        value += bits.read(_LAYER3_DATA["linbits"][table])
                    if value:
                        bits.read(1)
            # A final partial count1 code/sign group is stuffing, not a mandatory big_values pair.
            for _ in range((576 - pairs * 2) // 4):
                if bits.pos >= end:
                    break
                try:
                    symbol = bits.symbol(32 + info.count1table_select[gr][ch])
                    bits.read(bin(symbol).count("1"))
                except InvalidFrame:
                    break
            bits.pos = end


def read_frame(fr: BufferedBinaryIoT, content: bool = False) -> Optional[MpegAudioFrame]:
    """Read a bounded frame; content validation is explicit via validate_frame()."""
    try:
        pos, raw_header, header = _read_frame_header(fr)
    except Empty:
        return None
    except EOFError as e:
        raise InvalidFrame("Truncated MPEG audio header") from e

    try:
        raw_crc = read_or_raise(fr, 2) if not header.protection else b""
        crc = int.from_bytes(raw_crc, "big") if raw_crc else None
        frame_length = _get_frame_length_for_frame(fr, pos, header)
        body_size = frame_length - 4 - len(raw_crc)
        if body_size <= 0:
            raise InvalidFrame("MPEG frame has no audio data")
        if header.layer == Layer.LAYER_III:
            channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
            sideinfo_size = (17 if channels == 1 else 32) if header.id == ID.MPEG_1 else (9 if channels == 1 else 17)
            if body_size < sideinfo_size:
                raise InvalidFrame(f"Truncated Layer III side information at pos {pos}")
        data = None
        if content:
            body = read_or_raise(fr, body_size)
            data = raw_header + raw_crc + body
        else:
            _validate_frame_end(fr, pos, frame_length)
    except (Empty, EOFError) as e:
        raise InvalidFrame(f"Truncated frame at pos {pos}") from e

    return MpegAudioFrame(pos, header, crc, frame_length, data)


def find_first_valid_frame(fr: BufferedBinaryIoT, *, validate_stream: bool = False) -> Optional[int]:
    """Find a validated candidate, skipping bounded tags; optionally confirm stream context."""
    trusted_start = int(fr.tell())
    ape_start = _get_ape_start(fr)
    while True:
        pos = int(fr.tell())
        prefix = fr.read(11)
        fr.seek(pos)
        if not prefix:
            return None
        if pos == ape_start or prefix.startswith(_TAG_SIGNATURES):
            try:
                read_tags(fr)
            except (ParseError, NoTagsFound, Empty, EOFError):
                fr.seek(pos + 1)
            else:
                trusted_start = int(fr.tell())
            continue
        if len(prefix) < 2 or prefix[0] != 0xFF or prefix[1] & 0xE0 != 0xE0:
            for _ in _read_unrecognized(fr, ape_start):
                pass
            continue
        try:
            frame = read_frame(fr, content=True)
            assert frame is not None
            if validate_stream:
                if frame.header.layer == Layer.LAYER_III:
                    frame = replace(frame, _reservoir=b"")
                frame.validate()
                following = fr.read(11)
                if (
                    pos != trusted_start
                    and following
                    and not following.startswith(_TAG_SIGNATURES)
                    and frame.end != ape_start
                ):
                    fr.seek(pos)
                    candidates = iter_mp3(fr, content=True)
                    next(candidates).validate()
                    second = next(candidates)
                    if not isinstance(second, MpegAudioFrame) or (
                        second.header.id,
                        second.header.layer,
                        second.header.frequency,
                    ) != (frame.header.id, frame.header.layer, frame.header.frequency):
                        raise InvalidFrame("Unconfirmed MPEG audio sequence")
                    second.validate()
            else:
                frame.validate_frame()
        except (ParseError, IntegrityCheckFailed, StopIteration):
            fr.seek(pos + 1)
            continue
        fr.seek(pos)
        return pos


class Id3v2Frame(NamedTuple):
    """An ID3 frame with its original ID and flags.

    ``data`` is the de-unsynchronised payload, including any format prefixes;
    compression/encryption remain intact. ``text`` contains decoded ordinary
    text-frame values, or None for other frame types and encrypted frames.
    """

    frame_id: str
    flags: int
    data: bytes
    text: Optional[Tuple[str, ...]]


Id3v2Fields = Tuple[Id3v2Frame, ...]


class MetadataParseError(InvalidFrame):
    """Invalid metadata content within an already bounded tag."""

    def __init__(self, pos: int, tag: str, message: str):
        self.pos = pos
        self.tag = tag
        super().__init__(f"Invalid {tag} metadata at pos {pos}: {message}")


@dataclass(frozen=True)
class Metadata(Generic[T_metadata], ABC):
    """A bounded tag whose immutable payload is parsed only on request.

    The immutable bytes are independent of the source stream. Results are not
    cached; repeated calls repeat validation and extraction.
    """

    pos: int
    data: bytes
    _prefix: bytes = field(repr=False)
    _suffix: bytes = field(repr=False)
    tag: ClassVar[str]

    @property
    def size(self) -> int:
        return len(self._prefix) + len(self.data) + len(self._suffix)

    @property
    def end(self) -> int:
        return self.pos + self.size

    def raw(self) -> bytes:
        """Return the original complete tag, including its envelope and padding."""
        return self._prefix + self.data + self._suffix

    @abstractmethod
    def _process(self, extract: bool) -> Optional[T_metadata]:
        pass

    def _checked(self, extract: bool) -> Optional[T_metadata]:
        try:
            return self._process(extract)
        except (ParseError, Empty, EOFError, UnicodeDecodeError, ValueError, struct.error) as e:
            raise MetadataParseError(self.pos, self.tag, str(e)) from e

    def parse(self) -> T_metadata:
        """Validate and extract fields, raising MetadataParseError on invalid content."""
        fields = self._checked(True)
        assert fields is not None
        return fields

    def validate(self) -> None:
        """Validate content without retaining extracted frame records."""
        self._checked(False)


@dataclass(frozen=True)
class Id3v2Metadata(Metadata[Id3v2Fields]):
    header: Id3v2Header
    appended: bool = False
    tag: ClassVar[str] = "ID3v2"

    def _process(self, extract: bool) -> Optional[Id3v2Fields]:
        if self.appended and (self.header.major_version != 4 or not self.header.d):
            raise InvalidFrame("An appended ID3v2 tag requires a v2.4 footer")
        return _parse_id3_body(self.data, self.header, extract)


class APEv2Item(NamedTuple):
    key: str
    flags: int
    data: bytes
    text: Optional[Tuple[str, ...]]


@dataclass(frozen=True)
class APEv2Metadata(Metadata[Tuple[APEv2Item, ...]]):
    header: APEv2Header
    appended: bool = False
    tag: ClassVar[str] = "APE"

    def _process(self, extract: bool) -> Optional[Tuple[APEv2Item, ...]]:
        if self.appended and self.header.version == 2000 and int.from_bytes(self.header.flags, "little") & (1 << 30):
            raise InvalidFrame("An appended APE tag requires a footer")
        return _parse_ape_items(self.data, self.header, extract)


@dataclass(frozen=True)
class Lyrics3v1Metadata(Metadata[str]):
    has_id3v1: Optional[bool] = None
    tag: ClassVar[str] = "Lyrics3v1"

    def _process(self, extract: bool) -> Optional[str]:
        if self.has_id3v1 is False:
            raise InvalidFrame("Lyrics3 requires a following ID3v1 tag")
        if len(self.data) > 5100:
            raise InvalidFrame("Oversized Lyrics3 v1 lyrics")
        if b"\xff" in self.data or b"LYRICSBEGIN" in self.data or b"LYRICSEND" in self.data:
            raise InvalidFrame("Forbidden Lyrics3 v1 content")
        _validate_lyrics_lines(self.data)
        return self.data.decode("iso-8859-1") if extract else None


@dataclass(frozen=True)
class Lyrics3v2Metadata(Metadata[Dict[str, str]]):
    has_id3v1: Optional[bool] = None
    tag: ClassVar[str] = "Lyrics3v2"

    def _process(self, extract: bool) -> Optional[Dict[str, str]]:
        if self.has_id3v1 is False:
            raise InvalidFrame("Lyrics3 requires a following ID3v1 tag")
        return _parse_lyrics3v2(self.data, extract)


@dataclass(frozen=True)
class Id3v1Metadata(Metadata[Union[Id3v1Fields, Id3v11Fields]]):
    tag: ClassVar[str] = "ID3v1"

    def _process(self, extract: bool) -> Optional[Union[Id3v1Fields, Id3v11Fields]]:
        if len(self.data) != 125:
            raise InvalidFrame("Invalid ID3v1 payload length")
        if not extract:
            return None
        title, artist, album, year, comment, genre = struct.unpack("30s30s30s4s30sB", self.data)
        title, artist, album = (value.rstrip(b"\0") for value in (title, artist, album))
        if comment[-1] != 0 and comment[-2] == 0:
            return Id3v11Fields(title, artist, album, year, comment[:-2].rstrip(b"\0"), comment[-1], genre)
        return Id3v1Fields(title, artist, album, year, comment.rstrip(b"\0"), genre)


def _check_id3_compression(data: bytes, expected: int, collect: bool = False) -> Optional[bytes]:
    decoder = zlib.decompressobj()
    count = 0
    chunks: Optional[List[bytes]] = [] if collect else None
    try:
        while data:
            decoded = decoder.decompress(data, 65536)
            count += len(decoded)
            if count > expected:
                raise InvalidFrame("ID3 decompressed length exceeds its declaration")
            if chunks is not None:
                chunks.append(decoded)
            data = decoder.unconsumed_tail
        if not decoder.eof or decoder.unused_data or count != expected:
            raise InvalidFrame("Invalid ID3 compressed frame or decompressed size")
    except zlib.error as e:
        raise InvalidFrame("Invalid ID3 compressed frame") from e
    return b"".join(chunks) if chunks is not None else None


def _decode_id3_text(data: bytes, version: int) -> Tuple[str, ...]:
    if not data or data[0] > (1 if version < 4 else 3):
        raise InvalidFrame("Missing or invalid ID3 text encoding")
    encoding, data = data[0], data[1:]
    unit = 2 if encoding in (1, 2) else 1
    terminator = bytes(unit)
    parts = []
    start = 0
    for offset in range(0, len(data) - unit + 1, unit):
        if data[offset : offset + unit] == terminator:
            parts.append(data[start:offset])
            start = offset + unit
            # ID3v2.2/2.3 explicitly ignore bytes after the first text terminator.
            if version < 4:
                break
    else:
        if start < len(data) or not parts:
            parts.append(data[start:])
    codec = {0: "iso-8859-1", 2: "utf-16-be", 3: "utf-8"}.get(encoding)
    values = []
    try:
        for part in parts:
            if encoding == 1:
                bom = part[:2]
                if bom in (b"\xff\xfe", b"\xfe\xff"):
                    order = "utf-16-le" if bom == b"\xff\xfe" else "utf-16-be"
                    if codec is not None and codec != order:
                        raise InvalidFrame("ID3 UTF-16 strings change byte order within a frame")
                    codec = order
                    part = part[2:]
                elif part and codec is None:
                    raise InvalidFrame("ID3 UTF-16 text is missing its byte order mark")
            value = part.decode(codec or "utf-16-le")
            if "\r" in value or "\n" in value:
                raise InvalidFrame("Newlines are forbidden in ordinary ID3 text frames")
            values.append(value)
    except UnicodeDecodeError as e:
        raise InvalidFrame("Invalid ID3 text encoding") from e
    return tuple(values)


def _parse_id3_body(body: bytes, header: Id3v2Header, parse: bool) -> Optional[Id3v2Fields]:
    version = header.major_version
    masks = {2: 0xC0, 3: 0xE0, 4: 0xF0}
    flags = sum(int(value) << (7 - bit) for bit, value in enumerate(header[2:10]))
    if version not in masks or header.revision_number == 255:
        raise ParseError("Unsupported ID3v2 version")
    if flags & ~masks[version]:
        raise ParseError("Reserved ID3v2 flags")
    if version == 2 and header.b:
        raise ParseError("ID3v2.2 has no defined compression scheme to validate")
    if version < 4 and header.a:
        body = body.replace(b"\xff\x00", b"\xff")
    offset = 0
    crc = None
    declared_padding = None
    if version >= 3 and header.b:
        if len(body) < 4:
            raise InvalidFrame("Truncated ID3 extended header")
        if version == 3:
            size = int.from_bytes(body[:4], "big")
            if size not in (6, 10) or len(body) < size + 4:
                raise InvalidFrame("Invalid ID3v2.3 extended header size")
            flags = int.from_bytes(body[4:6], "big")
            if flags not in (0, 0x8000) or size != (10 if flags else 6):
                raise InvalidFrame("Invalid ID3v2.3 extended flags")
            declared_padding = int.from_bytes(body[6:10], "big")
            if flags:
                crc = int.from_bytes(body[10:14], "big")
            offset = size + 4
        else:
            size = id3_sync_safe_to_int(body[:4])
            if size < 6 or size > len(body) or body[4] != 1 or body[5] & ~0x70:
                raise InvalidFrame("Invalid ID3v2.4 extended header")
            offset = 6
            for flag, length in ((0x40, 0), (0x20, 5), (0x10, 1)):
                if body[5] & flag:
                    if offset >= size or body[offset] != length or offset + 1 + length > size:
                        raise InvalidFrame("Invalid ID3 extended flag data")
                    offset += 1
                    if flag == 0x20:
                        data = body[offset : offset + 5]
                        if data[0] > 15 or any(value & 0x80 for value in data):
                            raise InvalidFrame("Invalid ID3 extended CRC")
                        crc = 0
                        for value in data:
                            crc = (crc << 7) | value
                    offset += length
            if offset != size:
                raise InvalidFrame("Invalid ID3 extended header length")
    frames_start = offset
    frames = 0
    fields = []
    ident_size, frame_header = (3, 6) if version == 2 else (4, 10)
    while offset < len(body):
        if body[offset] == 0:
            if any(body[offset:]) or header.d and version == 4:
                raise InvalidFrame("Invalid ID3 padding")
            break
        if offset + frame_header > len(body):
            raise InvalidFrame("Truncated ID3 frame header")
        ident = body[offset : offset + ident_size]
        if not all(65 <= value <= 90 or 48 <= value <= 57 for value in ident):
            raise InvalidFrame("Invalid ID3 frame identifier")
        size_raw = body[offset + ident_size : offset + ident_size + (3 if version == 2 else 4)]
        size = id3_sync_safe_to_int(size_raw) if version == 4 else int.from_bytes(size_raw, "big")
        end = offset + frame_header + size
        if not size or end > len(body):
            raise InvalidFrame("ID3 frame size exceeds the tag or is empty")
        payload = body[offset + frame_header : end]
        flags = 0
        status = 0
        if version >= 3:
            status, flags = body[offset + 8 : offset + 10]
            status_mask, format_mask = (0xE0, 0xE0) if version == 3 else (0x70, 0x4F)
            if status & ~status_mask or flags & ~format_mask:
                raise InvalidFrame("Reserved ID3 frame flags")
        compressed = bool(flags & (0x80 if version == 3 else 8)) if version >= 3 else False
        encrypted = bool(flags & (0x40 if version == 3 else 4)) if version >= 3 else False
        if version == 4 and (header.a or flags & 2):
            payload = payload.replace(b"\xff\x00", b"\xff")
        extra = 0
        expected = None
        if version == 3:
            if compressed:
                if len(payload) < 4:
                    raise InvalidFrame("Missing ID3 decompressed size")
                expected = int.from_bytes(payload[:4], "big")
                extra += 4
            extra += bool(flags & 0x40) + bool(flags & 0x20)
        elif version == 4:
            extra += bool(flags & 0x40) + bool(flags & 4)
            if compressed and not flags & 1:
                raise InvalidFrame("Compressed ID3v2.4 frame requires a data length indicator")
            if flags & 1:
                if len(payload) < extra + 4:
                    raise InvalidFrame("Missing ID3 data length indicator")
                expected = id3_sync_safe_to_int(payload[extra : extra + 4])
                extra += 4
        if extra > len(payload):
            raise InvalidFrame("Truncated ID3 frame format fields")
        text_frame = ident.startswith(b"T") and ident not in (b"TXX", b"TXXX")
        decoded = payload[extra:]
        if not encrypted and expected is not None:
            if compressed:
                uncompressed = _check_id3_compression(decoded, expected, collect=text_frame)
                if uncompressed is not None:
                    decoded = uncompressed
            elif len(payload) - extra != expected:
                raise InvalidFrame("ID3 data length indicator mismatch")
        text = _decode_id3_text(decoded, version) if text_frame and not encrypted else None
        if parse:
            fields.append(Id3v2Frame(ident.decode("ascii"), status << 8 | flags, payload, text))
        offset = end
        frames += 1
    if not frames:
        raise InvalidFrame("ID3 tag must contain at least one frame")
    if declared_padding is not None and declared_padding != len(body) - offset:
        raise InvalidFrame("ID3 extended header padding length mismatch")
    crc_end = offset if version == 3 else len(body)
    if crc is not None and zlib.crc32(body[frames_start:crc_end]) != crc:
        raise InvalidFrame("ID3 extended CRC mismatch")
    return tuple(fields) if parse else None


def read_id3v2(fr: BufferedBinaryIoT) -> Id3v2Metadata:
    """Read an ID3v2 envelope without parsing its encoded body."""
    pos = fr.tell()

    if fr.read(3) != b"ID3":
        fr.seek(pos)
        raise WrongSignature("No ID3v2")

    fmt = ">u8 u8 b1 b1 b1 b1 b1 b1 b1 b1 r32"
    try:
        raw, id3_header = read_bits_to_namedtuple(fr, fmt, Id3v2Header, {10: id3_sync_safe_to_int})
    except (Empty, EOFError) as e:
        raise InvalidFrame("Truncated ID3v2 header") from e
    if id3_header.major_version not in (2, 3, 4):
        raise ParseError("Unsupported ID3v2 version")

    footer = b""
    try:
        body = read_or_raise(fr, id3_header.size) if id3_header.size else b""
        if id3_header.major_version == 4 and id3_header.d:
            footer = read_or_raise(fr, 10)
            if footer != b"3DI" + raw:
                raise InvalidFrame("Invalid ID3v2 footer")
    except (Empty, EOFError) as e:
        raise InvalidFrame("Truncated ID3v2 tag") from e
    return Id3v2Metadata(pos=pos, header=id3_header, data=body, _prefix=b"ID3" + raw, _suffix=footer)


def read_id3v1(fr: BufferedBinaryIoT) -> Id3v1Metadata:
    pos = fr.tell()
    if fr.read(3) != b"TAG":
        fr.seek(pos)
        raise WrongSignature("No ID3v1")
    try:
        data = read_or_raise(fr, 125)
    except (Empty, EOFError) as e:
        raise InvalidFrame("Truncated ID3v1 tag") from e
    return Id3v1Metadata(pos=pos, data=data, _prefix=b"TAG", _suffix=b"")


def _get_tag_end(fr: BufferedBinaryIoT) -> int:
    pos = fr.tell()
    try:
        fr.seek(0, os.SEEK_END)
        end = fr.tell()
        if end >= 128:
            fr.seek(end - 128)
            if fr.read(3) == b"TAG":
                end -= 128
        return end
    finally:
        fr.seek(pos)


def _read_ape_footer(fr: BufferedBinaryIoT) -> Optional[Tuple[int, APEv2Header]]:
    pos = fr.tell()
    try:
        end = _get_tag_end(fr)
        if end - pos >= 32:
            fr.seek(end - 32)
            if fr.read(8) == b"APETAGEX":
                return end - 32, APEv2Header._make(funpack(fr, "<LLL4sQ"))
        return None
    finally:
        fr.seek(pos)


def _parse_ape_items(data: bytes, header: APEv2Header, extract: bool) -> Optional[Tuple[APEv2Item, ...]]:
    flags = int.from_bytes(header.flags, "little") if header.version == 2000 else 0
    if header.reserved or flags & 0x1FFFFFF8 or flags & 6 == 6:
        raise InvalidFrame("Invalid APE tag header fields")
    fr = BytesIO(data)
    items_end = len(data)
    keys = set()
    items = []
    for _ in range(header.item_count):
        if items_end - fr.tell() < 8:
            raise InvalidFrame("APE item count exceeds the tag")
        size, item_flags = funpack(fr, "<LL")
        key = bytearray()
        while fr.tell() < items_end:
            key_byte = read_or_raise(fr, 1)[0]
            if key_byte == 0:
                break
            if key_byte < 32 or key_byte > 126 or len(key) == 255:
                raise InvalidFrame("Invalid APE item key")
            key.append(key_byte)
        else:
            raise InvalidFrame("Missing APE item key terminator")
        normalized = bytes(key).lower()
        if len(key) < 2 or normalized in keys or normalized in (b"id3", b"tag", b"oggs", b"mp+"):
            raise InvalidFrame("Invalid or duplicate APE item key")
        keys.add(normalized)
        if size > items_end - fr.tell():
            raise InvalidFrame("APE item value exceeds the tag")
        value = read_or_raise(fr, size) if size else b""
        text = None
        if header.version == 2000:
            if item_flags & ~7 or item_flags & 6 == 6:
                raise InvalidFrame("Reserved APE item flags")
            if item_flags & 6 != 2:
                decoded = value.decode("utf-8")
                if extract:
                    text = tuple(decoded.split("\0"))
        if extract:
            items.append(APEv2Item(bytes(key).decode("ascii"), item_flags, value, text))
    if fr.tell() != items_end:
        raise InvalidFrame("APE item sizes do not account for the tag")
    return tuple(items) if extract else None


def read_apev2(fr: BufferedBinaryIoT) -> APEv2Metadata:
    pos = fr.tell()
    footer_pos = None
    if fr.read(8) != b"APETAGEX":
        footer = _read_ape_footer(fr)
        if footer is None:
            fr.seek(pos)
            raise WrongSignature("No APEv2")
        footer_pos, header = footer
    else:
        try:
            header = APEv2Header._make(funpack(fr, "<LLL4sQ"))
        except (Empty, EOFError) as e:
            raise InvalidFrame("Truncated APE tag") from e
    if header.version not in (1000, 2000):
        raise ParseError(f"Unsupported APEv2 version {header.version}")
    flags = int.from_bytes(header.flags, "little") if header.version == 2000 else 0
    if header.size < 32:
        raise InvalidFrame("Invalid APE tag header")
    is_header = bool(flags & (1 << 29))
    envelope = b"APETAGEX" + struct.pack("<LLL4sQ", *header)
    prefix, suffix = (envelope, b"") if is_header else (b"", envelope)
    if footer_pos is not None and (is_header or footer_pos + 32 - header.size != pos):
        fr.seek(pos)
        raise WrongSignature("No APEv2")
    if is_header:
        if not flags & (1 << 31):
            raise InvalidFrame("APE header contradicts its contains-header flag")
        end = pos + 32 + header.size
        items_start = pos + 32
        if flags & (1 << 30):
            end -= 32
            items_end = end
        else:
            items_end = end - 32
            fr.seek(items_end)
            try:
                raw_footer = read_or_raise(fr, 32)
            except (Empty, EOFError) as e:
                raise InvalidFrame("Truncated APE footer") from e
            expected = b"APETAGEX" + struct.pack(
                "<LLLLQ",
                header.version,
                header.size,
                header.item_count,
                flags & ~(1 << 29),
                header.reserved,
            )
            if raw_footer != expected:
                raise InvalidFrame("Invalid APE footer")
            suffix = raw_footer
    else:
        if flags & (1 << 30):
            raise InvalidFrame("APE footer contradicts its no-footer flag")
        items_end = footer_pos if footer_pos is not None else pos
        items_start = items_end + 32 - header.size
        end = items_end + 32
        if items_start < 0:
            raise InvalidFrame("APE items extend before the input")
        if flags & (1 << 31):
            if footer_pos is not None or items_start < 32:
                raise InvalidFrame("APE declares a missing header")
            fr.seek(items_start - 32)
            expected = b"APETAGEX" + struct.pack(
                "<LLLLQ",
                header.version,
                header.size,
                header.item_count,
                flags | (1 << 29),
                header.reserved,
            )
            if fr.read(32) != expected:
                raise InvalidFrame("APE declares a missing or inconsistent header")
            prefix = expected
    try:
        _validate_frame_end(fr, pos, end - pos)
        fr.seek(items_start)
        data = read_or_raise(fr, items_end - items_start) if items_end > items_start else b""
    except (Empty, EOFError) as e:
        raise InvalidFrame("Truncated APE tag") from e
    fr.seek(end)
    tag_start = pos if is_header else items_start - (32 if flags & (1 << 31) else 0)
    return APEv2Metadata(pos=tag_start, header=header, data=data, _prefix=prefix, _suffix=suffix)


def _validate_lyrics_lines(data: bytes) -> None:
    lines = data.replace(b"\r\n", b"")
    if b"\r" in lines or b"\n" in lines:
        raise InvalidFrame("Lyrics3 line endings must be CRLF")


def read_lyrics3v1(fr: BufferedBinaryIoT) -> Lyrics3v1Metadata:
    pos = fr.tell()
    if fr.read(11) != b"LYRICSBEGIN":
        fr.seek(pos)
        raise WrongSignature("No Lyrics3 v1")
    end = _get_tag_end(fr)
    if end - pos < 20:
        raise InvalidFrame("Truncated Lyrics3 v1 tag")
    fr.seek(end - 9)
    if fr.read(9) != b"LYRICSEND":
        raise InvalidFrame("Missing Lyrics3 v1 ending marker")
    fr.seek(pos + 11)
    size = end - 9 - fr.tell()
    data = read_or_raise(fr, size) if size else b""
    fr.seek(end)
    has_id3v1 = fr.read(3) == b"TAG"
    fr.seek(end)
    return Lyrics3v1Metadata(pos=pos, data=data, has_id3v1=has_id3v1, _prefix=b"LYRICSBEGIN", _suffix=b"LYRICSEND")


def read_lyrics3v2(fr: BufferedBinaryIoT) -> Lyrics3v2Metadata:
    pos = fr.tell()
    if fr.read(11) != b"LYRICSBEGIN":
        fr.seek(pos)
        raise WrongSignature("No Lyrics3 v2")
    end = _get_tag_end(fr)
    if end - pos < 26:
        fr.seek(pos)
        raise WrongSignature("No Lyrics3 v2")
    fr.seek(end - 15)
    size, signature = funpack(fr, "6s9s")
    if signature != b"LYRICS200":
        fr.seek(pos)
        raise WrongSignature("No Lyrics3 v2")
    if not size.isdigit() or int(size) != end - 15 - pos:
        raise InvalidFrame("Invalid Lyrics3 v2 tag size")
    fr.seek(pos + 11)
    length = end - 15 - fr.tell()
    data = read_or_raise(fr, length) if length else b""
    fr.seek(end)
    has_id3v1 = fr.read(3) == b"TAG"
    fr.seek(end)
    return Lyrics3v2Metadata(pos=pos, data=data, has_id3v1=has_id3v1, _prefix=b"LYRICSBEGIN", _suffix=size + signature)


def _parse_lyrics3v2(data: bytes, extract: bool) -> Optional[Dict[str, str]]:
    fr = BytesIO(data)
    end = len(data)
    tags: Dict[str, str] = {}
    defined_fields = {"IND", "LYR", "INF", "AUT", "EAL", "EAR", "ETT", "IMG"}
    while fr.tell() < end:
        if end - fr.tell() < 8:
            raise InvalidFrame("Truncated Lyrics3 v2 field")
        field_id_raw, size_raw = funpack(fr, "3s5s")
        field_id = field_id_raw.decode("ascii")
        if field_id not in defined_fields:
            logger.warning("Found undefined field: %s", field_id)
        if not size_raw.isdigit():
            raise InvalidFrame("Invalid Lyrics3 v2 field size")
        size = int(size_raw)
        if size > end - fr.tell():
            raise InvalidFrame("Truncated Lyrics3 v2 field")
        raw = read_or_raise(fr, size) if size else b""
        if b"\0" in raw or b"\xff" in raw:
            raise InvalidFrame("Forbidden Lyrics3 v2 text byte")
        _validate_lyrics_lines(raw)
        if field_id == "IND" and (tags or size != 3 or any(value not in (48, 49) for value in raw)):
            raise InvalidFrame("Lyrics3 IND must be first and contain three binary indicators")
        if field_id in ("AUT", "EAL", "EAR", "ETT") and size > 250:
            raise InvalidFrame("Lyrics3 field exceeds its maximum size")
        tags[field_id] = raw.decode("iso-8859-1") if extract or field_id == "IND" else ""
    if not tags:
        raise InvalidFrame("Lyrics3 v2 must contain at least one field")
    if "IND" in tags and (tags["IND"][0] == "1") != ("LYR" in tags):
        raise InvalidFrame("Lyrics3 IND contradicts the presence of lyrics")
    return tags if extract else None


Tag = Union[Id3v2Metadata, APEv2Metadata, Lyrics3v1Metadata, Lyrics3v2Metadata, Id3v1Metadata]


@dataclass(frozen=True)
class JunkFrame:
    """Unrecognized bytes preserved at their original offset."""

    pos: int
    data: bytes = field(repr=False)

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def end(self) -> int:
        return self.pos + self.size

    def raw(self) -> bytes:
        return self.data

    def validate(self) -> None:
        raise InvalidFrame(f"Unrecognized bytes at pos {self.pos}")


@dataclass(frozen=True)
class PaddingFrame:
    """A standalone run of zero bytes; its placement is not a conformance claim."""

    pos: int
    data: bytes = field(repr=False)

    @property
    def size(self) -> int:
        return len(self.data)

    @property
    def end(self) -> int:
        return self.pos + self.size

    def raw(self) -> bytes:
        return self.data

    def validate(self) -> None:
        if any(self.data):
            raise InvalidFrame(f"Nonzero padding at pos {self.pos}")


FramesOrTags = Union[MpegAudioFrame, Tag, JunkFrame, PaddingFrame]


class NoTagsFound(Exception):
    pass


def read_tags(fr: BufferedBinaryIoT) -> Tag:
    try:
        metadata = read_id3v2(fr)
    except WrongSignature:
        pass
    else:
        return replace(metadata, appended=True)

    funcs: List[Callable[[BufferedBinaryIoT], Tag]] = [
        read_apev2,
        read_lyrics3v2,
        read_lyrics3v1,
        read_id3v1,
    ]
    for func in funcs:
        try:
            result = func(fr)
        except WrongSignature:
            continue
        return result
    raise NoTagsFound()


_TAG_SIGNATURES = (b"ID3", b"TAG", b"APETAGEX", b"LYRICSBEGIN")
_FRAME_START = re.compile(rb"\xff[\xe0-\xff]|ID3|TAG|APETAGEX|LYRICSBEGIN")
_BYTE_RUNS = re.compile(rb"\x00+|[^\x00]+")


def _read_unrecognized(fr: BufferedBinaryIoT, ape_start: Optional[int]) -> Iterator[Union[JunkFrame, PaddingFrame]]:
    pos = fr.tell()
    # Ten lookahead bytes keep the longest signature intact across chunks.
    block = fr.read(65536 + 10)
    end = min(65536, len(block))
    match = _FRAME_START.search(block)
    if match is not None:
        end = min(end, match.start())
    if ape_start is not None and ape_start > pos:
        end = min(end, ape_start - pos)
    assert end > 0
    for run in _BYTE_RUNS.finditer(block, 0, end):
        data = run.group()
        fr.seek(pos + run.end())
        if data[0] == 0:
            yield PaddingFrame(pos + run.start(), data)
        else:
            yield JunkFrame(pos + run.start(), data)


def _get_ape_start(fr: BufferedBinaryIoT) -> Optional[int]:
    footer = _read_ape_footer(fr)
    if footer is not None:
        pos, header = footer
        if header.version == 1000 or not int.from_bytes(header.flags, "little") & (1 << 31):
            return pos + 32 - header.size
    return None


def iter_mp3(fr: BufferedBinaryIoT, content: bool = False) -> Iterator[FramesOrTags]:
    """Read from the current position to EOF, leaving the caller's seekable stream open."""
    if not fr.seekable():
        raise ValueError("iter_mp3 requires a seekable stream")
    if not isinstance(fr.read(0), bytes):
        raise TypeError("iter_mp3 requires a binary stream")
    ape_start = _get_ape_start(fr)
    reservoir = b""
    payload_offset = used_bits = 0
    previous_format = None
    free_base = None
    audio_seen = False
    while True:
        pos = fr.tell()
        prefix = fr.read(11)
        fr.seek(pos)
        if not prefix:
            break
        # A headerless APE item's size can itself look like an MPEG frame header.
        if pos == ape_start or prefix.startswith(_TAG_SIGNATURES):
            metadata = read_tags(fr)
            if metadata.pos != pos:
                raise InvalidFrame("Tag overlaps previously read bytes")
            if isinstance(metadata, (Id3v2Metadata, APEv2Metadata)):
                metadata = replace(metadata, appended=audio_seen)
            yield metadata
            del metadata
            continue
        if len(prefix) < 2 or prefix[0] != 0xFF or prefix[1] & 0xE0 != 0xE0:
            yield from _read_unrecognized(fr, ape_start)
            # An unrecognized gap cannot establish continuous reservoir context.
            reservoir = b""
            payload_offset = used_bits = 0
            previous_format = free_base = None
            continue
        frame = read_frame(fr, content=content)
        assert frame is not None
        frame_length = frame.size
        header = frame.header
        current_format = (header.id, header.layer, header.frequency)
        if current_format != previous_format:
            reservoir = b""
            payload_offset = used_bits = 0
            free_base = None
        previous_format = current_format
        audio_seen = True
        if not header.bitrate:
            slot = 4 if header.layer == Layer.LAYER_I else 1
            base = frame_length - slot * header.padding
            if free_base is not None and base != free_base:
                raise InvalidFrame("Free-format base frame length changed")
            free_base = base
        else:
            free_base = None
        if content and header.layer == Layer.LAYER_III:
            raw = frame.raw()
            channels = 1 if header.mode == Mode.SINGLE_CHANNEL else 2
            size = (17 if channels == 1 else 32) if header.id == ID.MPEG_1 else (9 if channels == 1 else 17)
            body = raw[4 + (2 if frame.crc is not None else 0) :]
            _, info = read_sideinfo(BytesIO(body), header)
            payload = body[size:]
            back = info.main_data_begin
            start = (payload_offset - back) * 8
            frame = replace(frame, _reservoir=reservoir[-back:] if back else b"", _overlap=start < used_bits)
            used_bits = start + sum(sum(row) for row in info.par2_3_length)
            payload_offset += len(payload)
            reservoir = (reservoir + payload)[-511:]
        yield frame


def read_mp3(path: PathType, content: bool = False) -> Iterator[FramesOrTags]:
    """Open a path and read its audio, tags and unrecognized spans in file order."""
    with open(path, "rb") as fr:
        yield from iter_mp3(fr, content=content)


def validate_mp3(path: PathType) -> None:
    """Validate every part and require one audio block without standalone padding."""
    audio_seen = trailing_metadata = False
    for part in read_mp3(path, content=True):
        if isinstance(part, PaddingFrame):
            raise InvalidFrame(f"Standalone padding at pos {part.pos}")
        if isinstance(part, MpegAudioFrame):
            if trailing_metadata:
                raise InvalidFrame(f"Audio follows trailing metadata at pos {part.pos}")
            audio_seen = True
        elif audio_seen:
            trailing_metadata = True
        part.validate()
    if not audio_seen:
        raise MaybeNotMp3("File contains no MPEG audio frames")


@dataclass(frozen=True)
class CopyResult:
    frames: int
    bytes_copied: int
    start: int
    end: int
    skipped_trailing: int
    stop_reason: str
    validation_scope: str = "local"
    error: Optional[str] = None

    @property
    def skipped_leading(self) -> int:
        return self.start

    @property
    def stop_offset(self) -> int:
        return self.end


def copy_mpeg_audio(in_path: PathType, out_path: PathType, *, validate_stream: bool = False) -> CopyResult:
    """Atomically recover the first audio block; optionally require stream validation.

    Known parse/content errors commit the recovered prefix and are reported in
    CopyResult. No audio or unexpected I/O errors leave the destination unchanged.
    """
    if os.path.exists(out_path) and os.path.samefile(in_path, out_path):
        raise ValueError("Input and output must be different files")
    temporary: Optional[str] = None
    with open(in_path, "rb") as fr:
        fr.seek(0, os.SEEK_END)
        source_size = int(fr.tell())
        fr.seek(0)
        start = find_first_valid_frame(fr, validate_stream=validate_stream)
        if start is None:
            raise MaybeNotMp3(f"File doesn't contain a valid MPEG audio frame: {os.fspath(in_path)!r}")
        if start:
            logger.warning("Found and skipped %d bytes of invalid data at the beginning of the file %s", start, in_path)
        frames = bytes_copied = 0
        end = start
        stop_reason = "eof"
        error = None
        destination = os.path.abspath(out_path)
        try:
            with NamedTemporaryFile(
                mode="wb", dir=os.path.dirname(destination), prefix=".filemeta-", delete=False
            ) as fw:
                temporary = fw.name
                try:
                    for item in iter_mp3(fr, content=True):
                        if not isinstance(item, MpegAudioFrame):
                            stop_reason = "non_audio"
                            break
                        if validate_stream:
                            item.validate()
                        else:
                            item.validate_frame()
                        fw.write(item.raw())
                        frames += 1
                        bytes_copied += item.size
                        end = item.end
                except (ParseError, IntegrityCheckFailed) as exc:
                    stop_reason, error = "invalid_frame", str(exc)
                if not frames:
                    raise MaybeNotMp3("No valid audio frames could be copied")
            assert temporary is not None
            os.replace(temporary, destination)
            temporary = None
        finally:
            if temporary is not None:
                os.unlink(temporary)
    return CopyResult(
        frames,
        bytes_copied,
        start,
        end,
        source_size - end,
        stop_reason,
        validation_scope="stream" if validate_stream else "local",
        error=error,
    )


def _check_mp3_roundtrip(path: PathType, validate: bool = False) -> None:
    """Compare concatenated raw spans to the source without writing a copy."""
    with open(path, "rb") as fr:
        for item in read_mp3(path, content=True):
            pos = fr.tell()
            raw = item.raw()
            expected = fr.read(len(raw))
            if raw != expected:
                offset = next((i for i, (a, b) in enumerate(zip(raw, expected)) if a != b), len(expected))
                raise InvalidFrame(f"Roundtrip byte mismatch at pos {pos + offset}")
            if validate:
                item.validate()
        pos = fr.tell()
        if fr.read(1):
            raise InvalidFrame(f"Roundtrip omitted source bytes at pos {pos}")


if __name__ == "__main__":
    from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
    from pathlib import Path

    from genutility.args import is_dir
    from genutility.datetime import now
    from genutility.filesystem import scandir_error_log_warning, scandir_ext
    from genutility.iter import list_except
    from genutility.rich import Progress
    from rich.logging import RichHandler
    from rich.progress import Progress as RichProgress
    from rich.progress import TaskProgressColumn, TextColumn, TimeElapsedColumn

    DEFAULT_OUT = Path(f"mp3-{now().isoformat().replace(':', '-')}.log")

    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("path", type=is_dir)
    parser.add_argument("--action", choices=("read", "copy", "roundtrip"), required=True)
    parser.add_argument(
        "--validate", action="store_true", help="Validate content in roundtrip mode or stream context in copy mode"
    )
    parser.add_argument("--copy-suffix", default=".copy", help="suffix to add when copying files")
    parser.add_argument("-r", "--recursive", action="store_true", help="Scan subdirectories")
    parser.add_argument("-v", "--verbose", action="store_true", help="Log debug information")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="Log file output path")
    args = parser.parse_args()
    if args.validate and args.action not in ("roundtrip", "copy"):
        parser.error("--validate requires --action roundtrip or copy")

    FORMAT = "%(message)s"
    handlers = [logging.FileHandler(args.out, encoding="utf-8", delay=True), RichHandler(show_path=False)]

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG, format=FORMAT, datefmt="[%X]", handlers=handlers)
    else:
        logging.basicConfig(level=logging.INFO, format=FORMAT, datefmt="[%X]", handlers=handlers)

    def validated_frames(path: PathType) -> Iterator[FramesOrTags]:
        for item in read_mp3(path, True):
            item.validate()
            yield item

    total_count = 0
    errors_count = 0

    MPEG_AUDIO_EXT = {".mp3", ".mp2", ".m3a", ".m2a", ".mp2a", ".mpa", ".mpga"}

    progress = RichProgress(
        TextColumn("{task.completed}{task.description}"),
        TaskProgressColumn(show_speed=True),
        TimeElapsedColumn(),
        refresh_per_second=1,
    )
    with progress:
        p = Progress(progress)
        for entry in p.track(
            scandir_ext(args.path, MPEG_AUDIO_EXT, rec=args.recursive, errorfunc=scandir_error_log_warning),
            description=" files scanned",
        ):
            if args.action != "copy":
                total_count += 1

            if args.action == "read":
                exc, res = list_except(
                    p.track(validated_frames(entry), description=" frames processed", transient=True)
                )
                if exc:
                    out = []
                    for i in res[:1] + res[-2:]:
                        out.append(str(i))
                    logging.info("%s", "\n".join(out))
                    logging.exception("Enumerating frames of <%s> failed", os.fspath(entry), exc_info=exc)
                    logging.info("-----")
                    errors_count += 1
            elif args.action == "roundtrip":
                try:
                    _check_mp3_roundtrip(entry, validate=args.validate)
                except Exception as e:
                    logging.exception("Roundtrip of <%s> failed", os.fspath(entry), exc_info=e)
                    logging.info("-----")
                    errors_count += 1
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

                total_count += 1
                try:
                    result = copy_mpeg_audio(path, outpath, validate_stream=args.validate)
                    logging.info(
                        "Copied %d frames (%d bytes) from <%s>; skipped %d leading and %d trailing bytes; stopped at %d (%s, %s validation)",
                        result.frames,
                        result.bytes_copied,
                        path,
                        result.skipped_leading,
                        result.skipped_trailing,
                        result.stop_offset,
                        result.stop_reason,
                        result.validation_scope,
                    )
                    if result.error is not None:
                        logging.warning("Copy stopped at an invalid frame: %s", result.error)
                        logging.info("-----")
                        errors_count += 1
                except Exception as e:
                    logging.exception("Copying frames of <%s> failed", os.fspath(path), exc_info=e)
                    logging.info("-----")
                    errors_count += 1

    print(f"{errors_count}/{total_count} files failed to parse")
