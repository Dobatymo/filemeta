import struct
import unittest
import zlib
from io import BytesIO
from unittest.mock import patch

import bitstruct
from genutility.exceptions import ParseError

from filemeta import mp3


def replace_bits(data, offset, width, value):
    shift = len(data) * 8 - offset - width
    mask = ((1 << width) - 1) << shift
    return ((int.from_bytes(data, "big") & ~mask) | (value << shift)).to_bytes(len(data), "big")


def frame(size=None, body=None, crc_bits=None, **changes):
    header = mp3.Mp3Header(
        2047, mp3.ID.MPEG_1, mp3.Layer.LAYER_III, crc_bits is None, 9, 0, 0, False, mp3.Mode.STEREO, 0, False, True, 0
    )._replace(**changes)
    raw = bitstruct.pack(mp3.MPEG_AUDIO_HEADER_FMT, *header)
    length = mp3.get_frame_length(header) if size is None else size
    data = (body or b"").ljust(length - 4 - (2 if crc_bits is not None else 0), b"\0")
    if crc_bits is not None:
        raw += mp3._crc16_bits(raw[2:] + data, 16 + crc_bits).to_bytes(2, "big")
    return raw + data


def synchsafe(value, size=4):
    return bytes((value >> (7 * i)) & 127 for i in range(size - 1, -1, -1))


def id3_frame(payload=b"\0title", version=4, flags=0, ident=b"TIT2"):
    if version == 2:
        return ident[:3] + len(payload).to_bytes(3, "big") + payload
    size = synchsafe(len(payload)) if version == 4 else len(payload).to_bytes(4, "big")
    return ident + size + flags.to_bytes(2, "big") + payload


def id3(body=None, version=4, flags=0):
    if body is None:
        body = id3_frame(version=version)
    header = b"ID3" + bytes((version, 0, flags)) + synchsafe(len(body))
    return header + body + (b"3DI" + header[3:] if version == 4 and flags & 16 else b"")


def ape(items=b"", count=0, flags=0, with_header=False, version=2000):
    flags |= (1 << 31) if with_header else 0
    footer = b"APETAGEX" + struct.pack("<LLLLQ", version, len(items) + 32, count, flags, 0)
    header = b"APETAGEX" + struct.pack("<LLLLQ", version, len(items) + 32, count, flags | (1 << 29), 0)
    return (header if with_header else b"") + items + footer


def ape_item(key=b"Title", value=b"words", flags=0):
    return struct.pack("<LL", len(value), flags) + key + b"\0" + value


def lyrics(fields):
    body = b"LYRICSBEGIN" + fields
    return body + ("%06d" % len(body)).encode("ascii") + b"LYRICS200"


class StrictMp3Tests(unittest.TestCase):
    @staticmethod
    def parse(data, content):
        with patch("builtins.open", return_value=BytesIO(data)):
            parsed = []
            for item in mp3.read_mp3("test.mp3", content=content):
                if content or not isinstance(item, mp3.MpegAudioFrame):
                    item.validate()
                parsed.append(item)
            return parsed

    def reject(self, data):
        for content in (False, True):
            with self.subTest(content=content), self.assertRaises(ParseError):
                self.parse(data, content)

    def reject_audio(self, data):
        self.parse(data, False)
        with self.assertRaises(ParseError):
            self.parse(data, True)

    def accept(self, data, count=None):
        for content in (False, True):
            with self.subTest(content=content):
                parsed = self.parse(data, content)
                if count is not None:
                    self.assertEqual(count, len(parsed))

    def test_trailing_fragments_and_id3_padding(self):
        for tail in (b"x", b"xx", b"xxx", b"\xff\xfb\x90"):
            self.reject(frame() + tail)
        self.accept(id3() + bytes(16) + frame(), 3)
        self.accept(id3(id3_frame() + bytes(16)) + frame(), 2)
        self.accept(frame(), 1)

    def test_free_format_size_alignment_and_payload_sync(self):
        self.reject(frame(size=420, bitrate=0) + frame(size=421, bitrate=0) * 2)
        self.reject(frame(size=421, bitrate=0, layer=mp3.Layer.LAYER_I) * 3)
        data = bytearray(frame(size=420, bitrate=0) * 3)
        data[200:204] = data[:4]
        self.accept(bytes(data), 3)

    def test_layer3_semantics_with_correct_crcs(self):
        for offset, width, value in ((32, 9, 289), (53, 1, 1), (54, 5, 4), (54, 5, 14)):
            body = replace_bits(bytes(411), offset, width, value)
            self.reject_audio(frame(body=body, crc_bits=256))
        body = replace_bits(bytes(411), 12, 4, 1)
        body = replace_bits(body, 53, 1, 1)
        body = replace_bits(body, 54, 2, 2)
        self.reject_audio(frame(body=body, crc_bits=256))

    def test_layer3_reservoir_and_huffman_budgets(self):
        for offset, width, value in ((0, 9, 1), (20, 12, 4095), (49, 4, 15)):
            body = replace_bits(bytes(411), offset, width, value)
            self.reject_audio(frame(body=body, crc_bits=256))
        body = replace_bits(bytes(411), 32, 9, 1)
        body = replace_bits(body, 54, 5, 1)
        self.reject_audio(frame(body=body, crc_bits=256))
        # Table 1 code 1 is the (0, 0) pair and consumes one bit.
        body = replace_bits(body, 20, 12, 1)
        body = replace_bits(body, 256, 1, 1)
        self.accept(frame(body=body, crc_bits=256), 1)
        # Backward references are valid only when previous main-data bytes exist.
        body = replace_bits(bytes(411), 0, 9, 1)
        self.accept(frame() + frame(body=body, crc_bits=256), 2)
        # Previous data consumes the whole first payload; reusing it overlaps.
        first = replace_bits(bytes(413), 20, 12, 3048)
        self.reject_audio(frame(body=first) + frame(body=body, crc_bits=256))

    def test_layer3_escape_values_and_sign_bits_are_bounded(self):
        for table in (16, 23, 24, 31):
            book = "16" if table < 24 else "24"
            code = next(code for code, symbol in mp3._LAYER3_DATA["books"][book].items() if symbol == 15)
            encoded = code + "0" * mp3._LAYER3_DATA["linbits"][table] + "1"
            body = replace_bits(bytes(413), 32, 9, 1)
            body = replace_bits(body, 54, 5, table)
            body = replace_bits(body, 20, 12, len(encoded))
            body = replace_bits(body, 256, len(encoded), int(encoded, 2))
            self.accept(frame(body=body), 1)
            self.reject_audio(frame(body=replace_bits(body, 20, 12, len(encoded) - 1)))

    def test_layer1_and_layer2_reserved_scalefactors(self):
        for layer, bitrate, allocation_bits, sf_offset, crc_bits in (
            (mp3.Layer.LAYER_I, 9, 256, 256, 256),
            (mp3.Layer.LAYER_II, 8, 176, 178, 178),
        ):
            body = replace_bits(bytes(300), 0, 4, 1)
            if layer == mp3.Layer.LAYER_II:
                body = replace_bits(body, allocation_bits, 2, 2)
            body = replace_bits(body, sf_offset, 6, 63)
            self.reject_audio(frame(body=body, layer=layer, bitrate=bitrate, crc_bits=crc_bits))
        self.reject_audio(frame(layer=mp3.Layer.LAYER_I, bitrate=1, frequency=1))

    def test_id3_frame_bounds_flags_and_padding(self):
        for body in (
            b"junk",
            b"TIT2\0\0\1\0\0\0\0a",
            id3_frame(flags=0x8000),
            id3_frame(flags=0x10),
            id3_frame(payload=b""),
            id3_frame() + b"\0x",
        ):
            self.reject(id3(body) + frame())
        self.reject(id3(b"x", flags=0x40))
        self.reject(frame() + id3(id3_frame() + bytes(4), flags=16))
        self.reject(id3(b"", version=3))
        for version in (2, 3, 4):
            self.accept(id3(version=version) + frame(), 2)
        self.accept(frame() + id3(flags=16), 2)

    def test_id3_compression_unsynchronisation_and_length_indicators(self):
        payload = b"\0a\xff\xe0b\xff\0"
        for version in (3, 4):
            compressed = zlib.compress(payload)
            prefix = len(payload).to_bytes(4, "big") if version == 3 else synchsafe(len(payload))
            flags = 0x80 if version == 3 else 9
            self.accept(id3(id3_frame(prefix + compressed, version, flags), version), 1)
            self.reject(id3(id3_frame(prefix + compressed[:-1], version, flags), version))
            self.reject(id3(id3_frame(bytes(4) + compressed, version, flags), version))
        self.reject(id3(id3_frame(b"data", flags=8)))
        self.reject(id3(id3_frame(synchsafe(99) + payload, flags=1)))
        self.accept(id3(id3_frame(synchsafe(len(payload)) + payload, flags=1)), 1)
        escaped = payload.replace(b"\xff", b"\xff\0")
        raw = id3_frame(payload, 3).replace(b"\xff", b"\xff\0")
        self.accept(id3(raw, 3, 0x80), 1)
        self.accept(id3(id3_frame(escaped, flags=2)), 1)
        # A zero flag-data byte belongs to grouping, not padding.
        self.accept(id3(id3_frame(b"\0" + payload, flags=0x40)), 1)

    def test_id3_extended_header_crcs(self):
        for version in (3, 4):
            frames = id3_frame(version=version)
            padding = bytes(3)
            crc = zlib.crc32(frames + (padding if version == 4 else b""))
            ext = (
                struct.pack(">LHL", 10, 0x8000, len(padding)) + crc.to_bytes(4, "big")
                if version == 3
                else synchsafe(12) + b"\1\x20\5" + synchsafe(crc, 5)
            )
            self.accept(id3(ext + frames + padding, version, 0x40), 1)
            damaged = bytearray(ext + frames + padding)
            damaged[len(ext) + 11] ^= 1
            self.reject(id3(bytes(damaged), version, 0x40))
        self.reject(id3(synchsafe(6) + b"\1\x20" + id3_frame(), flags=0x40))

    def test_ape_items_counts_keys_and_flags(self):
        for tag in (
            ape(count=1),
            ape(ape_item(), 0),
            ape(struct.pack("<LL", 100, 0) + b"Title\0x", 1),
            ape(ape_item(key=b"x"), 1),
            ape(ape_item(key=b"TAG"), 1),
            ape(ape_item(key=b"Title") + ape_item(key=b"title"), 2),
            ape(ape_item(value=b"\xff"), 1),
            ape(ape_item(flags=6), 1),
            ape(flags=1 << 31),
            ape(flags=1 << 30),
        ):
            self.reject(frame() + tag)
        for with_header in (False, True):
            for value, flags in ((b"text\0multiple", 0), (b"\xff\0", 2), (b"cover.jpg", 4)):
                self.accept(frame() + ape(ape_item(value=value, flags=flags), 1, with_header=with_header), 2)
        self.accept(frame() + ape(version=1000), 2)
        # A leading streaming tag may omit the footer; an appended tag must have one.
        header_only = ape(with_header=True, flags=1 << 30)[:-32]
        self.accept(header_only + frame(), 2)
        self.reject(frame() + header_only)
        self.reject(frame() + header_only + b"TAG" + bytes(125))
        self.reject(frame() + ape(with_header=True)[:-1])

    def test_lyrics_constraints_and_id3v1_bounds(self):
        end = b"TAG" + bytes(125)
        for content in (b"a\xff", b"a" * 5101, b"aLYRICSBEGINb", b"a\nb"):
            self.reject(frame() + b"LYRICSBEGIN" + content + b"LYRICSEND" + end)
        for fields in (
            b"",
            b"IND0000211",
            b"IND000031x0",
            b"LYR00005wordsIND00003100",
            b"AUT00251" + b"a" * 251,
            b"LYR00001\0",
            b"LYR00003a\nb",
            b"IND00003100",
        ):
            self.reject(frame() + lyrics(fields) + end)
        self.reject(frame() + lyrics(b"LYR00005words"))
        self.accept(frame() + lyrics(b"IND00003100LYR00005words") + end, 3)
        self.accept(frame() + b"LYRICSBEGINa\r\nbLYRICSEND" + end, 3)
        with self.assertRaises(mp3.InvalidFrame):
            mp3.read_id3v1(BytesIO(b"TAG"))
        stream = BytesIO(end)
        mp3.read_id3v1(stream)
        self.assertEqual(128, stream.tell())


if __name__ == "__main__":
    unittest.main()
