import struct
import unittest
import zlib
from io import BytesIO
from unittest.mock import patch

from filemeta import mp3


def synchsafe(value):
    return bytes((value >> shift) & 127 for shift in (21, 14, 7, 0))


def frame(payload, ident=b"TIT2", version=4, flags=0):
    if version == 2:
        return ident + len(payload).to_bytes(3, "big") + payload
    size = synchsafe(len(payload)) if version == 4 else len(payload).to_bytes(4, "big")
    return ident + size + flags.to_bytes(2, "big") + payload


def tag(body, version=4, flags=0):
    header = b"ID3" + bytes((version, 0, flags)) + synchsafe(len(body))
    return header + body + (b"3DI" + header[3:] if version == 4 and flags & 16 else b"")


class Id3ExtractionTests(unittest.TestCase):
    def extract(self, raw):
        fields = mp3.read_id3v2(BytesIO(raw)).parse()
        self.assertIsInstance(fields, tuple)
        return fields

    def reject(self, raw):
        for operation in ("parse", "validate"):
            with self.subTest(operation=operation), self.assertRaises(mp3.InvalidFrame):
                metadata = mp3.read_id3v2(BytesIO(raw))
                getattr(metadata, operation)()

    def test_title_artist_album_for_every_version(self):
        for version, ids in (
            (2, (b"TT2", b"TP1", b"TAL")),
            (3, (b"TIT2", b"TPE1", b"TALB")),
            (4, (b"TIT2", b"TPE1", b"TALB")),
        ):
            body = b"".join(
                frame(b"\0" + value, ident, version) for ident, value in zip(ids, (b"Title", b"AC/DC", b"Album"))
            )
            raw = tag(body, version)
            fields = self.extract(raw)
            self.assertEqual([ident.decode("ascii") for ident in ids], [item.frame_id for item in fields])
            self.assertEqual([("Title",), ("AC/DC",), ("Album",)], [item.text for item in fields])
            self.assertEqual([b"\0Title", b"\0AC/DC", b"\0Album"], [item.data for item in fields])
            metadata = mp3.read_id3v2(BytesIO(raw))
            self.assertIsInstance(metadata, mp3.Id3v2Metadata)
            self.assertEqual(body, metadata.data)
            self.assertEqual(fields, metadata.parse())
            self.assertIsNone(metadata.validate())

    def test_supported_text_encodings_and_utf16_alignment(self):
        cases = (
            (0, "iso-8859-1", b"", "caf\u00e9"),
            (1, "utf-16-le", b"\xff\xfe", "\u53f0\u7063"),
            (1, "utf-16-be", b"\xfe\xff", "\u53f0\u7063"),
            (2, "utf-16-be", b"", "\u53f0\u7063"),
            (3, "utf-8", b"", "\u53f0\u7063"),
        )
        for encoding, codec, bom, value in cases:
            for version in (2, 3, 4):
                if version < 4 and encoding > 1:
                    continue
                ident = b"TT2" if version == 2 else b"TIT2"
                raw = tag(frame(bytes((encoding,)) + bom + value.encode(codec), ident, version), version)
                self.assertEqual((value,), self.extract(raw)[0].text)
        # Looking for 00 00 at arbitrary byte offsets would truncate this string.
        payload = b"\1\xff\xfeA\0\0\1B\0\0\0"
        self.assertEqual(("A\u0100B",), self.extract(tag(frame(payload)))[0].text)

    def test_v24_multivalue_text_and_byte_order(self):
        for payload in (
            b"\3" + "Alice\0\u53f0\u7063".encode(),
            b"\1\xff\xfe" + "Alice\0\u53f0\u7063".encode("utf-16-le"),
            b"\1\xff\xfe" + "Alice\0".encode("utf-16-le") + b"\xff\xfe" + "\u53f0\u7063".encode("utf-16-le"),
            b"\2" + "Alice\0\u53f0\u7063".encode("utf-16-be"),
        ):
            self.assertEqual(("Alice", "\u53f0\u7063"), self.extract(tag(frame(payload, b"TPE1")))[0].text)
        self.assertEqual(("one", "", "three"), self.extract(tag(frame(b"\0one\0\0three\0")))[0].text)
        self.reject(tag(frame(b"\1\xff\xfeA\0\0\0\xfe\xff\0B")))

    def test_legacy_text_ignores_suffix_after_terminator(self):
        for version in (2, 3):
            ident = b"TT2" if version == 2 else b"TIT2"
            for payload in (b"\0visible\0ignored", b"\1\xff\xfe" + "visible\0".encode("utf-16-le") + b"\xff"):
                self.assertEqual(("visible",), self.extract(tag(frame(payload, ident, version), version))[0].text)

    def test_empty_text_and_invalid_encoded_values(self):
        for payload in (b"\0", b"\3", b"\1", b"\1\0\0", b"\1\xff\xfe\0\0"):
            self.assertEqual(("",), self.extract(tag(frame(payload)))[0].text)
        for payload in (
            b"\4text",
            b"\3\xff",
            b"\3\xc0\x80",
            b"\1A\0",
            b"\1\xff\xfeA",
            b"\1\xff\xfe\0\xd8",
            b"\2\0",
            b"\0line\nbreak",
            b"\0line\rbreak",
        ):
            self.reject(tag(frame(payload)))
        for version in (2, 3):
            ident = b"TT2" if version == 2 else b"TIT2"
            self.reject(tag(frame(b"\3ascii", ident, version), version))

    def test_raw_binary_unknown_user_defined_and_encrypted_frames(self):
        records = (
            (b"XABC", b"\xff\0\xff", 0),
            (b"XABC", b"other", 0),
            (b"APIC", b"\0image/png\0\3\0\x89PNG", 0),
            (b"TXXX", b"\3description\0value", 0),
            (b"TIT2", b"\x80\xffciphertext", 0x1004),
        )
        fields = self.extract(tag(b"".join(frame(data, ident, flags=flags) for ident, data, flags in records)))
        self.assertEqual([(ident.decode("ascii"), flags, data, None) for ident, data, flags in records], list(fields))

    def test_format_prefixes_and_unsynchronisation_preserve_payload(self):
        for version in (3, 4):
            original = b"\0a\xff\xe0b"
            normal = frame(original, version=version)
            if version == 3:
                raw = tag(normal.replace(b"\xff", b"\xff\0"), version, 0x80)
            else:
                raw = tag(frame(original.replace(b"\xff", b"\xff\0"), flags=2))
            value = self.extract(raw)[0]
            self.assertEqual(original, value.data)
            self.assertEqual(("a\u00ff\u00e0b",), value.text)
            grouped = b"\x80\0title"
            flags = 0x8020 if version == 3 else 0x1040
            value = self.extract(tag(frame(grouped, version=version, flags=flags), version))[0]
            self.assertEqual((flags, grouped, ("title",)), (value.flags, value.data, value.text))
        value = self.extract(tag(frame(synchsafe(6) + b"\0title", flags=1)))[0]
        self.assertEqual(("title",), value.text)

    def test_compressed_text_and_decompressed_size_checks(self):
        for version in (3, 4):
            for payload in (b"\0title", b"\3" + b"a" * 131072 if version == 4 else b"\0" + b"a" * 131072):
                prefix = len(payload).to_bytes(4, "big") if version == 3 else synchsafe(len(payload))
                data = prefix + zlib.compress(payload)
                flags = 0x80 if version == 3 else 9
                value = self.extract(tag(frame(data, version=version, flags=flags), version))[0]
                self.assertEqual((payload[1:].decode("ascii"),), value.text)
                self.assertEqual(data, value.data)
                self.reject(tag(frame(data[:-1], version=version, flags=flags), version))
                self.reject(tag(frame(bytes(4) + data[4:], version=version, flags=flags), version))

    def test_footer_position_bounds_and_crc_still_checked(self):
        raw = tag(frame(b"\0title"), flags=16)
        stream = BytesIO(b"prefix" + raw + b"audio")
        stream.seek(6)
        metadata = mp3.read_id3v2(stream)
        position, fields = metadata.pos, metadata.parse()
        self.assertEqual(6, position)
        self.assertEqual(("title",), fields[0].text)
        self.assertEqual(b"audio", stream.read())
        for broken in (raw[:-1], raw[:10], tag(b"TIT2\0\0\1\0\0\0x")):
            self.reject(broken)
        data = frame(b"\0title", version=3)
        ext = struct.pack(">LHL", 10, 0x8000, 0) + zlib.crc32(data).to_bytes(4, "big")
        self.assertEqual(("title",), self.extract(tag(ext + data, 3, 0x40))[0].text)
        self.reject(tag(ext + data[:-1] + b"X", 3, 0x40))

    def test_lazy_content_is_not_processed_until_requested(self):
        payload = b"\0title"
        encoded = synchsafe(len(payload)) + zlib.compress(payload)
        raw = tag(frame(encoded, flags=9))
        with patch.object(mp3, "_parse_id3_body", side_effect=AssertionError("eager body parsing")):
            with BytesIO(raw) as stream:
                metadata = mp3.read_id3v2(stream)
                self.assertEqual(len(raw), stream.tell())
        self.assertEqual(frame(encoded, flags=9), metadata.data)
        self.assertEqual(("title",), metadata.parse()[0].text)
        with self.assertRaises(AttributeError):
            metadata.data = b"changed"
        self.assertEqual(metadata.parse(), metadata.parse())

    def test_invalid_contents_do_not_interrupt_audio_iteration(self):
        audio = bytes.fromhex("fffb9000") + bytes(413)
        invalid = (
            tag(frame(b"\3\xff")),
            tag(b"junk"),
            tag(b""),
            tag(b"x", flags=0x40),
            tag(frame(synchsafe(10) + b"bad deflate data", flags=9)),
            tag(synchsafe(12) + b"\1\x20\5" + bytes(5) + frame(b"\0title"), flags=0x40),
            tag(frame(b"\0title"), flags=1),
            tag(frame(b"\0title"), version=3, flags=0x10),
            tag(frame(b"\0title"), flags=16)[:-10] + b"bad-footer",
        )
        # A mismatching outer footer remains a structural error, not a content error.
        for raw in invalid[:-1]:
            for content in (False, True):
                with patch("builtins.open", return_value=BytesIO(raw + audio)):
                    parsed = list(mp3.read_mp3("test.mp3", content=content))
                    for item in parsed:
                        if content and isinstance(item, mp3.MpegAudioFrame):
                            item.validate()
                self.assertEqual(2, len(parsed))
                metadata = parsed[0]
                self.assertIsInstance(metadata, mp3.Id3v2Metadata)
                for operation in (metadata.parse, metadata.validate):
                    with self.assertRaises(mp3.MetadataParseError) as caught:
                        operation()
                    self.assertEqual(0, caught.exception.pos)
                    self.assertEqual("ID3v2", caught.exception.tag)
                    self.assertIsNotNone(caught.exception.__cause__)
                with patch("builtins.open", return_value=BytesIO(raw + audio)):
                    with self.assertRaises(mp3.MetadataParseError):
                        for item in mp3.read_mp3("test.mp3", content=content):
                            if content and isinstance(item, mp3.MpegAudioFrame):
                                item.validate()
                            if isinstance(item, mp3.Metadata):
                                item.validate()
        self.reject(invalid[-1])

    def test_invalid_appended_content_and_free_format_boundaries(self):
        audio = bytes.fromhex("fffb9000") + bytes(413)
        raw = audio + tag(frame(b"\3\xff"), flags=16)
        with patch("builtins.open", return_value=BytesIO(raw)):
            parsed = list(mp3.read_mp3("test.mp3", content=True))
            for item in parsed:
                if isinstance(item, mp3.MpegAudioFrame):
                    item.validate()
        self.assertEqual(2, len(parsed))
        with self.assertRaises(mp3.MetadataParseError) as caught:
            parsed[-1].parse()
        self.assertEqual(len(audio), caught.exception.pos)
        free = bytes.fromhex("fffb0000") + bytes(416)
        raw = free * 2 + tag(b"junk", flags=16)
        with patch("builtins.open", return_value=BytesIO(raw)):
            parsed = list(mp3.read_mp3("test.mp3", content=True))
            for item in parsed:
                if isinstance(item, mp3.MpegAudioFrame):
                    item.validate()
        self.assertEqual([420, 420], [len(item.raw()) for item in parsed if isinstance(item, mp3.MpegAudioFrame)])
        with self.assertRaises(mp3.MetadataParseError):
            parsed[-1].validate()

    def test_full_file_extraction_for_leading_and_appended_tags(self):
        audio = bytes.fromhex("fffb9000") + bytes(413)
        raw = tag(frame(b"\0Title")) + audio + tag(frame(b"\0Album", b"TALB"), flags=16)
        with patch("builtins.open", return_value=BytesIO(raw)):
            parsed = list(mp3.read_mp3("test.mp3", content=True))
            for item in parsed:
                if isinstance(item, mp3.MpegAudioFrame):
                    item.validate()
        self.assertEqual(3, len(parsed))
        self.assertIsInstance(parsed[0], mp3.Id3v2Metadata)
        self.assertIsInstance(parsed[2], mp3.Id3v2Metadata)
        self.assertEqual(("Title",), parsed[0].parse()[0].text)
        self.assertEqual(("Album",), parsed[2].parse()[0].text)
        stream = BytesIO(tag(frame(b"\0Album", b"TALB"), flags=16))
        self.assertEqual(("Album",), mp3.read_tags(stream).parse()[0].text)


if __name__ == "__main__":
    unittest.main()
