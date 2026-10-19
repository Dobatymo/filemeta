import os
import struct
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory

import bitstruct
from genutility.exceptions import ParseError

from filemeta import mp3


class Mp3RegressionTests(unittest.TestCase):
    @staticmethod
    def frame(size=None, **changes):
        header = mp3.Mp3Header(
            2047,
            mp3.ID.MPEG_1,
            mp3.Layer.LAYER_III,
            True,
            9,
            0,
            0,
            False,
            mp3.Mode.STEREO,
            0,
            False,
            True,
            0,
        )._replace(**changes)
        raw = bitstruct.pack(mp3.MPEG_AUDIO_HEADER_FMT, *header)
        length = mp3.get_frame_length(header) if size is None else size
        return raw + bytes(length - 4)

    @staticmethod
    def id3v24():
        body = b"TIT2\x00\x00\x00\x02\x00\x00\x00a"
        header = b"ID3\x04\x00\x10\x00\x00\x00\x0c"
        return header + body + b"3DI" + header[3:]

    @staticmethod
    def parse(data, content=True):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "audio.mp3"
            path.write_bytes(data)
            return list(mp3.read_mp3(path, content=content, verify=content))

    def test_copy_preserves_source_for_same_file_and_hardlink(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "source.mp3"
            data = self.frame() * 2
            path.write_bytes(data)
            alias = Path(directory) / "alias.mp3"
            os.link(path, alias)
            for destination in (path, alias):
                with self.subTest(destination=destination.name), self.assertRaises(ValueError):
                    mp3.copy_mpeg_audio(path, destination)
                self.assertEqual(data, path.read_bytes())

    def test_id3v2_rejects_missing_body_and_footer(self):
        tag = self.id3v24()
        for prefix in (b"", self.frame()):
            for content in (False, True):
                for damaged in (tag[:3], tag[:10], tag[:-1], tag[:-10], tag[:-10] + b"bad-footer"):
                    with self.subTest(prefix=bool(prefix), content=content, damaged=damaged):
                        with self.assertRaises(mp3.InvalidFrame):
                            self.parse(prefix + damaged, content)
                parsed = self.parse(prefix + tag, content)
                self.assertEqual(2 if prefix else 1, len(parsed))

    def test_id3v2_rejects_reserved_flags_and_invalid_sizes(self):
        for tag in (
            b"ID3\x03\x00\x10\x00\x00\x00\x00",
            b"ID3\x04\x00\x00\x00\x00\x00\x80",
        ):
            with self.subTest(tag=tag), self.assertRaises(ParseError):
                mp3.read_id3v2(BytesIO(tag))
        stream = BytesIO(b"ID3\x03\x00\x00\x00\x00\x00\x00" + self.frame())
        mp3.read_id3v2(stream)
        self.assertEqual(10, stream.tell())

    def test_free_format_padding_tags_and_truncation_for_all_layers(self):
        for layer in (mp3.Layer.LAYER_I, mp3.Layer.LAYER_II, mp3.Layer.LAYER_III):
            slot = 4 if layer == mp3.Layer.LAYER_I else 1
            frames = (
                self.frame(size=420, layer=layer, bitrate=0),
                self.frame(size=420 + slot, layer=layer, bitrate=0, padding=1),
                self.frame(size=420, layer=layer, bitrate=0),
            )
            for content in (False, True):
                with self.subTest(layer=layer, content=content):
                    parsed = self.parse(b"".join(frames) + self.id3v24(), content)
                    self.assertEqual(4, len(parsed))
                    self.assertIsInstance(parsed[-1][1][1], mp3.Id3v2Header)
                    if content:
                        self.assertEqual([420, 420 + slot, 420], [len(raw) for raw, _ in parsed[:-1]])
                    with self.assertRaises(mp3.InvalidFrame):
                        self.parse(b"".join(frames)[:-10], content)
                    stream = BytesIO(b"".join(frames))
                    for expected in frames:
                        parsed_frame = mp3.read_frame(stream, content=content, verify=content)
                        self.assertIsNotNone(parsed_frame)
                        if content:
                            self.assertEqual(expected, parsed_frame[0])
                    self.assertIsNone(mp3.read_frame(stream, content=content, verify=content))

    def test_free_format_layer2_uses_specified_allocation_table(self):
        for frequency, sblimit in ((0, 30), (1, 27), (2, 30)):
            with self.subTest(frequency=frequency):
                frames = self.parse(self.frame(size=420, layer=mp3.Layer.LAYER_II, bitrate=0, frequency=frequency) * 2)
                self.assertEqual(2, len(frames))
                self.assertEqual(sblimit, frames[0][1].sideinfo.sblimit)

    def test_id3v11_track_and_comment(self):
        for comment in (b"", b"a comment"):
            tag = b"TAG" + bytes(94) + comment.ljust(28, b"\0") + b"\0\x07\x00"
            _, fields = mp3.read_id3v1(BytesIO(tag))
            self.assertIsInstance(fields, mp3.Id3v11Fields)
            self.assertEqual(7, fields.track)
            self.assertEqual(comment, fields.comment)
        _, fields = mp3.read_id3v1(BytesIO(b"TAG" + bytes(125)))
        self.assertIsInstance(fields, mp3.Id3v1Fields)

    def test_lyrics3_versions_preserve_payload_and_next_tag(self):
        lyrics = b"complete lyrics longer than nine bytes"
        v1 = b"LYRICSBEGIN" + lyrics + b"LYRICSEND"
        body = b"LYRICSBEGININD00003110LYR00005words"
        v2 = body + ("%06d" % len(body)).encode("ascii") + b"LYRICS200"
        for tag, expected in ((v1, lyrics.decode("ascii")), (v2, {"IND": "110", "LYR": "words"})):
            with self.subTest(tag=tag):
                parsed = self.parse(self.frame() + tag + b"TAG" + bytes(125))
                self.assertEqual(3, len(parsed))
                self.assertEqual(expected, parsed[1][1][1])
                self.assertIsInstance(parsed[-1][1][1], mp3.Id3v1Fields)
        with self.assertRaises(mp3.InvalidFrame):
            mp3.read_lyrics3v1(BytesIO(v1[:-1]))
        with self.assertRaises(mp3.InvalidFrame):
            mp3.read_lyrics3v2(BytesIO(v2.replace(b"LYR00005", b"LYR99999")))

    def test_ape_optional_header_and_following_id3v1(self):
        for item in (b"", struct.pack("<LL", 1, 0) + b"Title\0x"):
            for with_header in (False, True):
                flags = 1 << 31 if with_header else 0
                footer = b"APETAGEX" + struct.pack("<LLLLQ", 2000, len(item) + 32, bool(item), flags, 0)
                header = b""
                if with_header:
                    header = b"APETAGEX" + struct.pack(
                        "<LLLLQ",
                        2000,
                        len(item) + 32,
                        bool(item),
                        flags | (1 << 29),
                        0,
                    )
                tag = header + item + footer
                with self.subTest(item=bool(item), with_header=with_header):
                    stream = BytesIO(tag + b"TAG" + bytes(125))
                    mp3.read_apev2(stream)
                    self.assertEqual(len(tag), stream.tell())
                    self.assertEqual(b"TAG", stream.read(3))
                    parsed = self.parse(self.frame() + tag + b"TAG" + bytes(125))
                    self.assertEqual(3, len(parsed))
                    self.assertIsInstance(parsed[1][1][1], mp3.APEv2Header)

    def test_ape_item_size_cannot_be_mistaken_for_an_audio_header(self):
        for prefix in (bytes.fromhex("fffb0000"), bytes.fromhex("fffb1000")):
            size = int.from_bytes(prefix, "little")
            item = prefix + bytes(4) + b"Cover\0" + bytes(size)
            footer = b"APETAGEX" + struct.pack("<LLLLQ", 2000, len(item) + 32, 1, 0, 0)
            for content in (False, True):
                parsed = self.parse(self.frame() + item + footer, content)
                self.assertEqual(2, len(parsed))
                self.assertIsInstance(parsed[-1][1][1], mp3.APEv2Header)

    def test_appended_id3v24_requires_footer_flag(self):
        tag = self.id3v24()
        without_footer = tag[:5] + b"\0" + tag[6:-10]
        for content in (False, True):
            self.assertEqual(2, len(self.parse(without_footer + self.frame(), content)))
            with self.assertRaisesRegex(mp3.InvalidFrame, "requires a v2.4 footer"):
                self.parse(self.frame() + without_footer, content)

    def test_header_ignores_unused_bits_and_rejects_forbidden_values(self):
        for mode in (mp3.Mode.STEREO, mp3.Mode.DUAL_CHANNEL, mp3.Mode.SINGLE_CHANNEL):
            for extension in (1, 2, 3):
                self.assertEqual(1, len(self.parse(self.frame(mode=mode, mode_extension=extension))))
        for changes in (
            {"emphasis": 2},
            {"bitrate": 0, "frequency": 3},
            {"layer": mp3.Layer.LAYER_II, "bitrate": 1},
            {"layer": mp3.Layer.LAYER_II, "bitrate": 2},
            {"layer": mp3.Layer.LAYER_II, "bitrate": 3},
            {"layer": mp3.Layer.LAYER_II, "bitrate": 5},
            {"layer": mp3.Layer.LAYER_II, "bitrate": 11, "mode": mp3.Mode.SINGLE_CHANNEL},
        ):
            with self.subTest(changes=changes), self.assertRaises(ParseError):
                header = mp3.Mp3Header._make(
                    bitstruct.unpack(mp3.MPEG_AUDIO_HEADER_FMT, self.frame(size=420, **changes)[:4])
                )
                mp3.validate_header(header)
        for bitrate in (4, 6, 7, 8, 9, 10):
            self.assertEqual(1, len(self.parse(self.frame(layer=mp3.Layer.LAYER_II, bitrate=bitrate))))


if __name__ == "__main__":
    unittest.main()
