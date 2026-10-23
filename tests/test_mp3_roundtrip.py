import unittest
from dataclasses import FrozenInstanceError
from io import BytesIO
from unittest.mock import patch

from filemeta import mp3
from test_mp3_strict import ape, ape_item, frame, id3, id3_frame, lyrics, replace_bits


class RawFrameTests(unittest.TestCase):
    @staticmethod
    def read(data, content=True):
        with patch("builtins.open", return_value=BytesIO(data)):
            return list(mp3.read_mp3("audio.mp3", content=content))

    def check_roundtrip(self, data):
        items = self.read(data)
        pos = 0
        for item in items:
            self.assertEqual(pos, item.pos)
            pos += len(item.raw())
        self.assertEqual(len(data), pos)
        self.assertEqual(data, b"".join(item.raw() for item in items))
        return items

    def test_all_metadata_envelopes_and_padding_roundtrip(self):
        end = b"TAG" + bytes(125)
        cases = [b"", b"x", bytes(3), b"junk" + bytes(3) + frame() + bytes(7) + b"tail"]
        for version in (2, 3, 4):
            cases.append(id3(id3_frame(version=version) + bytes(9), version=version) + frame())
        cases.extend(
            (
                frame() + id3(flags=16),
                frame() + end,
                frame() + b"LYRICSBEGINwordsLYRICSEND" + end,
                frame() + lyrics(b"IND00003100LYR00005words") + end,
                ape(with_header=True, flags=1 << 30)[:-32] + frame(),
            )
        )
        for version in (1000, 2000):
            for with_header in (False,) if version == 1000 else (False, True):
                cases.append(frame() + ape(ape_item(), 1, with_header=with_header, version=version) + end)
        # APEv1 ignores these flag bits, but raw() must preserve them exactly.
        cases.append(frame() + ape(ape_item(), 1, flags=0xFFFFFFFF, version=1000))
        for data in cases:
            with self.subTest(size=len(data)):
                self.check_roundtrip(data)

    def test_junk_and_padding_are_explicit_and_validation_is_separate(self):
        items = self.check_roundtrip(b"junk" + bytes(3) + frame() + b"tail")
        self.assertEqual([mp3.JunkFrame, mp3.PaddingFrame, mp3.MpegAudioFrame, mp3.JunkFrame], list(map(type, items)))
        with self.assertRaises(mp3.InvalidFrame):
            items[0].validate()
        items[1].validate()
        items[2].validate()
        with self.assertRaises(FrozenInstanceError):
            items[0].data = b"changed"
        with self.assertRaises(mp3.InvalidFrame):
            mp3.PaddingFrame(0, b"nonzero").validate()

    def test_signatures_crossing_scan_chunks_are_preserved(self):
        tags = (id3() + frame(), ape(ape_item(), 1), lyrics(b"LYR00005words") + b"TAG" + bytes(125), frame())
        for size in (65535, 65536, 65537):
            for tag in tags:
                with self.subTest(size=size, signature=tag[:11]):
                    items = self.check_roundtrip(b"x" * size + tag)
                    self.assertTrue(any(isinstance(item, (mp3.Metadata, mp3.MpegAudioFrame)) for item in items))

    def test_large_unrecognized_spans_are_bounded(self):
        for byte, kind in ((b"x", mp3.JunkFrame), (b"\0", mp3.PaddingFrame)):
            items = self.check_roundtrip(byte * 200000)
            self.assertTrue(all(type(item) is kind for item in items))
            self.assertTrue(all(0 < len(item.raw()) <= 65536 for item in items))

    def test_signatures_inside_known_payloads_do_not_split_objects(self):
        payload = b"ID3TAGAPETAGEXLYRICSBEGIN\xff\xfb" + bytes(7)
        data = id3(id3_frame(payload)) + frame(body=bytes(32) + payload)
        items = self.check_roundtrip(data)
        self.assertEqual([mp3.Id3v2Metadata, mp3.MpegAudioFrame], list(map(type, items)))

    def test_raw_does_not_duplicate_the_reservoir(self):
        body = replace_bits(bytes(413), 0, 9, 1)
        data = frame(body=bytes(412) + b"\x80") + frame(body=body)
        items = self.check_roundtrip(data)
        self.assertEqual(b"\x80", items[1]._reservoir)
        for item in reversed(items):
            item.validate()

    def test_content_false_retains_only_non_audio_bytes(self):
        tag = id3()
        items = self.read(b"junk" + bytes(2) + tag + frame(), content=False)
        self.assertEqual(b"junk", items[0].raw())
        self.assertEqual(bytes(2), items[1].raw())
        self.assertEqual(tag, items[2].raw())
        with self.assertRaisesRegex(ValueError, "content=True"):
            items[3].raw()

    def test_recognized_truncated_envelopes_still_fail(self):
        for damaged in (frame()[:-1], b"\xff\xfb\x90", id3()[:-1], b"APETAGEX", b"LYRICSBEGIN"):
            with self.subTest(data=damaged[:11]), self.assertRaises(mp3.InvalidFrame):
                self.read(b"junk" + damaged)

    def test_gaps_reset_reservoir_and_preserve_appended_tag_context(self):
        body = replace_bits(bytes(413), 0, 9, 1)
        items = self.check_roundtrip(frame() + b"junk" + frame(body=body))
        with self.assertRaisesRegex(mp3.InvalidFrame, "unavailable reservoir"):
            items[-1].validate()
        items = self.check_roundtrip(frame() + bytes(2) + id3())
        with self.assertRaisesRegex(mp3.MetadataParseError, "footer"):
            items[-1].validate()

    def test_free_format_frames_roundtrip(self):
        items = self.check_roundtrip(frame(size=420, bitrate=0) * 3)
        self.assertEqual(3, len(items))


if __name__ == "__main__":
    unittest.main()
