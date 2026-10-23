import unittest
from contextlib import ExitStack
from dataclasses import FrozenInstanceError
from io import BytesIO
from unittest.mock import patch

from filemeta import mp3
from test_mp3_strict import frame, replace_bits


class LazyAudioTests(unittest.TestCase):
    @staticmethod
    def read(data, content=False):
        stream = BytesIO(data)
        with patch("builtins.open", return_value=stream):
            results = list(mp3.read_mp3("audio.mp3", content=content))
        assert stream.closed
        return results

    def test_content_processing_is_deferred_in_both_output_modes(self):
        data = frame(layer=mp3.Layer.LAYER_I) + frame(layer=mp3.Layer.LAYER_II) + frame(crc_bits=256)
        for content in (False, True):
            with self.subTest(content=content), ExitStack() as stack:
                for name in (
                    "_parse_layer1",
                    "_parse_layer2",
                    "_crc16_bits",
                    "_validate_layer3_sideinfo",
                    "_validate_layer3_main",
                ):
                    stack.enter_context(patch.object(mp3, name, side_effect=AssertionError("eager content processing")))
                parsed = self.read(data, content)
            self.assertEqual(3, len(parsed))
            for item in parsed:
                if content:
                    self.assertEqual(item.data, item.raw())
                    self.assertIsNone(item.validate())
                    self.assertEqual(item.parse(), item.sideinfo)
                else:
                    self.assertIsNone(item.data)
                    self.assertIsNone(item._reservoir)
                    for operation in (item.raw, item.parse, item.validate, lambda item=item: item.sideinfo):
                        with self.assertRaisesRegex(ValueError, "content=True"):
                            operation()

    def test_huffman_errors_do_not_interrupt_later_frames(self):
        body = replace_bits(bytes(413), 32, 9, 1)
        body = replace_bits(body, 54, 5, 1)
        for content in (False, True):
            with self.subTest(content=content):
                parsed = self.read(frame(body=body) + frame(), content)
                self.assertEqual(2, len(parsed))
                if not content:
                    continue
                parsed[1].validate()
                for operation in (parsed[0].parse, parsed[0].validate):
                    with self.assertRaisesRegex(mp3.InvalidFrame, "bit budget"):
                        operation()

    def test_crc_errors_are_deferred(self):
        damaged = bytearray(frame(crc_bits=256))
        damaged[4] ^= 1
        for content in (False, True):
            with self.subTest(content=content):
                parsed = self.read(bytes(damaged) + frame(), content)
                self.assertEqual(2, len(parsed))
                if not content:
                    continue
                for operation in (parsed[0].parse, parsed[0].validate):
                    with self.assertRaises(mp3.IntegrityCheckFailed):
                        operation()

    def test_layer1_and_layer2_content_errors_are_deferred(self):
        for layer, offset in ((mp3.Layer.LAYER_I, 256), (mp3.Layer.LAYER_II, 178)):
            body = replace_bits(bytes(300), 0, 4, 1)
            if layer == mp3.Layer.LAYER_II:
                body = replace_bits(body, 176, 2, 2)
            body = replace_bits(body, offset, 6, 63)
            data = frame(body=body, layer=layer, bitrate=8 if layer == mp3.Layer.LAYER_II else 9)
            for content in (False, True):
                with self.subTest(layer=layer, content=content):
                    item = self.read(data + frame(), content)[0]
                    if not content:
                        continue
                    with self.assertRaisesRegex(mp3.InvalidFrame, "scalefactor 63"):
                        item.validate()

    def test_reservoir_snapshots_survive_source_close_and_validation_order(self):
        body = replace_bits(bytes(413), 0, 9, 1)
        body = replace_bits(body, 20, 12, 1)
        body = replace_bits(body, 32, 9, 1)
        body = replace_bits(body, 54, 5, 1)
        prefix = frame(body=bytes(412) + b"\x80")
        parsed = self.read(prefix + frame(body=body) + frame(), content=True)
        for item in reversed(parsed):
            item.validate()
        for item in parsed:
            item.validate()
        item = parsed[1]
        self.assertEqual(b"\x80", item._reservoir)
        self.assertIsInstance(item.data, bytes)
        with self.assertRaises(FrozenInstanceError):
            item.data = b"changed"

    def test_missing_reservoir_and_overlap_are_deferred(self):
        back = replace_bits(bytes(413), 0, 9, 1)
        first = replace_bits(bytes(413), 20, 12, 3048)
        cases = ((frame(body=back), 0, "unavailable reservoir"), (frame(body=first) + frame(body=back), 1, "overlaps"))
        for data, index, message in cases:
            for content in (False, True):
                with self.subTest(message=message, content=content):
                    item = self.read(data, content)[index]
                    if not content:
                        continue
                    with self.assertRaisesRegex(mp3.InvalidFrame, message):
                        item.validate()

    def test_layer3_sideinfo_semantics_are_deferred(self):
        body = replace_bits(bytes(413), 32, 9, 289)
        item = self.read(frame(body=body) + frame(), content=True)[0]
        with self.assertRaisesRegex(mp3.InvalidFrame, "big_values"):
            item.validate()

    def test_standalone_frames_keep_local_validation_lazy(self):
        for layer, expected in (
            (mp3.Layer.LAYER_I, mp3.MpegAudioLayer1FrameData),
            (mp3.Layer.LAYER_II, mp3.MpegAudioLayer2FrameData),
            (mp3.Layer.LAYER_III, mp3.Mp3SideInformation),
        ):
            with self.subTest(layer=layer), BytesIO(frame(layer=layer)) as stream:
                item = mp3.read_frame(stream, content=True)
            self.assertIsInstance(item.sideinfo, expected)
            self.assertIsNone(item.validate_frame())
            if layer == mp3.Layer.LAYER_III:
                with self.assertRaisesRegex(ValueError, "stream validation"):
                    item.validate()
            else:
                self.assertIsInstance(item.parse(), expected)
                self.assertIsNone(item.validate())
        damaged = bytearray(frame(crc_bits=256))
        damaged[4] ^= 1
        with BytesIO(damaged) as stream:
            item = mp3.read_frame(stream, content=True)
        with self.assertRaises(mp3.IntegrityCheckFailed):
            item.validate()

    def test_structural_truncation_remains_eager(self):
        for data in (frame()[:-1], frame() + b"\xff\xfb\x90"):
            for content in (False, True):
                with self.subTest(size=len(data), content=content), self.assertRaises(mp3.InvalidFrame):
                    self.read(data, content)

    def test_header_only_mode_skips_payload_reads(self):
        class TrackingStream(BytesIO):
            def __init__(self, data):
                super().__init__(data)
                self.reads = []

            def read(self, size=-1):
                pos = self.tell()
                data = super().read(size)
                self.reads.append((pos, len(data)))
                return data

        for layer in mp3.Layer:
            if layer == mp3.Layer.RESERVED:
                continue
            for crc_bits in (None, 0):
                with self.subTest(layer=layer, crc=crc_bits):
                    data = frame(layer=layer, crc_bits=crc_bits)
                    stream = TrackingStream(data)
                    item = mp3.read_frame(stream)
                    expected = [(0, 4)] + ([(4, 2)] if crc_bits is not None else []) + [(len(data) - 1, 1)]
                    self.assertEqual(expected, stream.reads)
                    self.assertEqual(len(data), stream.tell())
                    self.assertIsNone(item.data)
                    stream.close()
                    with self.assertRaisesRegex(ValueError, "content=True"):
                        item.validate()

        data = frame() * 100
        stream = TrackingStream(data)
        with patch("builtins.open", return_value=stream), patch.object(
            mp3, "read_sideinfo", side_effect=AssertionError("eager sideinfo read")
        ):
            parsed = list(mp3.read_mp3("audio.mp3"))
        self.assertEqual(100, len(parsed))
        self.assertTrue(all(item.data is None for item in parsed))
        self.assertLess(sum(size for _, size in stream.reads), len(data) // 10)

    def test_readers_leave_validation_explicit(self):
        data = frame(crc_bits=256)
        with patch.object(mp3.MpegAudioFrame, "validate", side_effect=AssertionError("eager validation")), patch.object(
            mp3.MpegAudioFrame, "validate_frame", side_effect=AssertionError("eager local validation")
        ):
            with BytesIO(data) as stream:
                self.assertIsNotNone(mp3.read_frame(stream, content=True))
            with BytesIO(data) as stream:
                self.assertEqual(1, len(list(mp3.iter_mp3(stream, content=True))))
            self.assertEqual(1, len(self.read(data, content=True)))


if __name__ == "__main__":
    unittest.main()
