import struct
import unittest
from io import BytesIO
from unittest.mock import patch

from filemeta import mp3


def ape_item(key=b"Title", value=b"title", flags=0):
    return struct.pack("<LL", len(value), flags) + key + b"\0" + value


def ape(items=b"", count=0, flags=0, version=2000, header=False, reserved=0):
    flags |= (1 << 31) if header else 0
    footer = b"APETAGEX" + struct.pack("<LLLLQ", version, len(items) + 32, count, flags, reserved)
    start = b"APETAGEX" + struct.pack("<LLLLQ", version, len(items) + 32, count, flags | (1 << 29), reserved)
    return (start if header else b"") + items + footer


def lyrics2(fields):
    raw = b"LYRICSBEGIN" + fields
    return raw + ("%06d" % len(raw)).encode("ascii") + b"LYRICS200"


class MetadataTests(unittest.TestCase):
    audio = bytes.fromhex("fffb9000") + bytes(413)
    id3v1 = b"TAG" + bytes(125)
    id3v2 = b"ID3\4\0\0\0\0\0\x0cTIT2\0\0\0\2\0\0\0a"

    @staticmethod
    def read(data):
        with patch("builtins.open", return_value=BytesIO(data)):
            items = list(mp3.read_mp3("test.mp3", content=True))
            for item in items:
                if isinstance(item, mp3.MpegAudioFrame):
                    item.validate()
            return items

    def test_all_metadata_objects_are_immutable_and_source_independent(self):
        cases = (
            (mp3.read_id3v2, self.id3v2, mp3.Id3v2Metadata),
            (mp3.read_id3v1, self.id3v1, mp3.Id3v1Metadata),
            (mp3.read_apev2, ape(ape_item(), 1, header=True), mp3.APEv2Metadata),
            (mp3.read_lyrics3v1, b"LYRICSBEGINwordsLYRICSEND", mp3.Lyrics3v1Metadata),
            (mp3.read_lyrics3v2, lyrics2(b"LYR00005words"), mp3.Lyrics3v2Metadata),
        )
        for reader, data, kind in cases:
            with self.subTest(kind=kind):
                with patch.object(kind, "_process", side_effect=AssertionError("eager parsing")):
                    with BytesIO(
                        b"prefix!"
                        + data
                        + (self.id3v1 if kind in (mp3.Lyrics3v1Metadata, mp3.Lyrics3v2Metadata) else b"")
                    ) as stream:
                        stream.seek(7)
                        metadata = reader(stream)
                        self.assertEqual(7 + len(data), stream.tell())
                self.assertIsInstance(metadata, mp3.Metadata)
                self.assertIsInstance(metadata, kind)
                self.assertEqual(7, metadata.pos)
                self.assertIsInstance(metadata.data, bytes)
                self.assertIsNone(metadata.validate())
                self.assertEqual(metadata.parse(), metadata.parse())
                with self.assertRaises(AttributeError):
                    metadata.data = b"changed"
        self.assertEqual("words", mp3.read_lyrics3v1(BytesIO(cases[3][1] + self.id3v1)).parse())
        self.assertEqual({"LYR": "words"}, mp3.read_lyrics3v2(BytesIO(cases[4][1] + self.id3v1)).parse())
        self.assertEqual(("a",), mp3.read_id3v2(BytesIO(self.id3v2)).parse()[0].text)

    def test_ape_preserves_items_and_decodes_text_lists_and_locators(self):
        items = (
            ape_item(value="one\0\u53f0\u7063".encode()),
            ape_item(key=b"Cover Art (Front)", value=b"cover.png\0\xff\xe0", flags=2),
            ape_item(key=b"External", value=b"cover.png", flags=4),
        )
        for header in (False, True):
            metadata = mp3.read_apev2(BytesIO(ape(b"".join(items), 3, header=header)))
            parsed = metadata.parse()
            self.assertEqual(["Title", "Cover Art (Front)", "External"], [item.key for item in parsed])
            self.assertEqual(("one", "\u53f0\u7063"), parsed[0].text)
            self.assertEqual(b"cover.png\0\xff\xe0", parsed[1].data)
            self.assertIsNone(parsed[1].text)
            self.assertEqual(("cover.png",), parsed[2].text)
            self.assertIsNone(metadata.validate())

    def test_ape_v1_ignores_flags_and_keeps_value_bytes(self):
        raw = ape(ape_item(value=b"\xff", flags=0xFFFFFFFF), 1, flags=0xFFFFFFFF, version=1000)
        parsed = self.read(self.audio + raw)
        metadata = parsed[-1]
        self.assertIsNone(metadata.validate())
        self.assertEqual(b"\xff", metadata.parse()[0].data)
        self.assertIsNone(metadata.parse()[0].text)

    def test_ape_content_errors_are_deferred(self):
        bad = (
            ape(count=1),
            ape(ape_item(), 0),
            ape(struct.pack("<LL", 999, 0) + b"Title\0x", 1),
            ape(ape_item(key=b"x"), 1),
            ape(ape_item(key=b"TAG"), 1),
            ape(ape_item() + ape_item(key=b"title"), 2),
            ape(ape_item(value=b"\xff"), 1),
            ape(ape_item(flags=6), 1),
            ape(reserved=1),
            ape(flags=8),
            ape(header=True, reserved=1),
        )
        for raw in bad:
            with self.subTest(raw=raw):
                parsed = self.read(self.audio + raw + self.id3v1)
                self.assertEqual(3, len(parsed))
                metadata = parsed[1]
                self.assertIsInstance(metadata, mp3.APEv2Metadata)
                for operation in (metadata.parse, metadata.validate):
                    with self.assertRaises(mp3.MetadataParseError) as caught:
                        operation()
                    self.assertEqual("APE", caught.exception.tag)
                    self.assertEqual(len(self.audio), caught.exception.pos)
                    self.assertIsNotNone(caught.exception.__cause__)

    def test_lyrics_content_errors_are_deferred(self):
        bad_v1 = (b"a\xff", b"aLYRICSBEGINb", b"aLYRICSENDb", b"a\nb", b"a" * 5101)
        bad_v2 = (
            b"",
            b"IND0000211",
            b"IND000031x0",
            b"AUT00251" + b"a" * 251,
            b"LYR00001\0",
            b"LYR00003a\nb",
            b"LYR99999words",
            b"IND00003100",
        )
        for raw in [b"LYRICSBEGIN" + data + b"LYRICSEND" for data in bad_v1] + [lyrics2(data) for data in bad_v2]:
            with self.subTest(raw=raw[:30]):
                parsed = self.read(self.audio + raw + self.id3v1)
                self.assertEqual(3, len(parsed))
                metadata = parsed[1]
                self.assertIsInstance(metadata, (mp3.Lyrics3v1Metadata, mp3.Lyrics3v2Metadata))
                for operation in (metadata.parse, metadata.validate):
                    with self.assertRaises(mp3.MetadataParseError) as caught:
                        operation()
                    self.assertEqual(len(self.audio), caught.exception.pos)

    def test_lyrics_and_id3_boundaries_remain_fatal(self):
        bad = (b"LYRICSBEGINwordsLYRICSEN", lyrics2(b"LYR00005words")[:-1])
        # Alter the declared total size without altering the payload.
        wrong_size = bytearray(lyrics2(b"LYR00005words"))
        wrong_size[-15:-9] = b"000999"
        for raw in bad + (bytes(wrong_size),):
            with self.subTest(raw=raw), self.assertRaises(mp3.InvalidFrame):
                self.read(self.audio + raw + self.id3v1)
        for raw in (b"LYRICSBEGINwordsLYRICSEND", lyrics2(b"LYR00005words")):
            parsed = self.read(self.audio + raw)
            self.assertEqual(2, len(parsed))
            with self.assertRaises(mp3.MetadataParseError):
                parsed[-1].validate()
        for data in (b"TAG", self.id3v1[:-1]):
            with self.assertRaises(mp3.InvalidFrame):
                mp3.read_id3v1(BytesIO(data))

    def test_ape_boundaries_remain_fatal(self):
        bad = (
            ape(header=True)[:-1],
            ape(flags=1 << 31),
            ape(flags=1 << 30),
            b"APETAGEX" + struct.pack("<LLLLQ", 2000, 31, 0, 0, 0),
        )
        for raw in bad:
            with self.subTest(raw=raw), self.assertRaises(mp3.InvalidFrame):
                self.read(self.audio + raw)

    def test_safe_metadata_placement_errors_are_deferred(self):
        without_footer = ape(header=True, flags=1 << 30)[:-32]
        for raw in (self.id3v2, without_footer):
            parsed = self.read(self.audio + raw)
            self.assertEqual(2, len(parsed))
            metadata = parsed[-1]
            self.assertTrue(metadata.appended)
            with self.assertRaises(mp3.MetadataParseError):
                metadata.validate()

    def test_free_format_audio_survives_invalid_ape_contents(self):
        free = bytes.fromhex("fffb0000") + bytes(416)
        parsed = self.read(free * 2 + ape(ape_item(value=b"\xff"), 1) + self.id3v1)
        self.assertEqual([420, 420], [len(item.raw()) for item in parsed if isinstance(item, mp3.MpegAudioFrame)])
        self.assertIsInstance(parsed[-2], mp3.APEv2Metadata)
        with self.assertRaises(mp3.MetadataParseError):
            parsed[-2].validate()

    def test_id3v1_and_v11_use_the_same_lazy_interface(self):
        comment = b"comment"
        raw = b"TAG" + bytes(94) + comment.ljust(28, b"\0") + b"\0\7\0"
        value = mp3.read_id3v1(BytesIO(raw)).parse()
        self.assertIsInstance(value, mp3.Id3v11Fields)
        self.assertEqual((comment, 7), (value.comment, value.track))
        value = mp3.read_id3v1(BytesIO(self.id3v1)).parse()
        self.assertIsInstance(value, mp3.Id3v1Fields)


if __name__ == "__main__":
    unittest.main()
