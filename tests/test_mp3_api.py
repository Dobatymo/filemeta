import unittest
from contextlib import ExitStack
from dataclasses import FrozenInstanceError
from io import BytesIO, StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from filemeta import mp3
from test_mp3_strict import ape, ape_item, frame, id3, id3_frame, replace_bits


class StreamAndValidationTests(unittest.TestCase):
    def test_stream_current_position_ownership_and_later_validation(self):
        source = b"prefix" + id3() + frame()
        with BytesIO(source) as stream:
            stream.seek(6)
            items = list(mp3.iter_mp3(stream, content=True))
            self.assertFalse(stream.closed)
            self.assertEqual(len(source), stream.tell())
        self.assertEqual([6, 6 + len(id3())], [item.pos for item in items])
        self.assertEqual(source[6:], b"".join(item.raw() for item in items))
        for item in items:
            item.validate()

    def test_early_stop_and_parse_errors_do_not_close_stream(self):
        with BytesIO(frame() * 2) as stream:
            iterator = mp3.iter_mp3(stream, content=True)
            next(iterator)
            iterator.close()
            self.assertFalse(stream.closed)
            self.assertEqual(len(frame()), stream.tell())
        with BytesIO(frame()[:-1]) as stream:
            with self.assertRaises(mp3.InvalidFrame):
                list(mp3.iter_mp3(stream))
            self.assertFalse(stream.closed)

    def test_stream_requirements_are_explicit(self):
        class ForwardOnly(BytesIO):
            def seekable(self):
                return False

        with ForwardOnly(frame()) as stream, self.assertRaisesRegex(ValueError, "seekable"):
            list(mp3.iter_mp3(stream))
        with StringIO("text") as stream, self.assertRaisesRegex(TypeError, "binary"):
            list(mp3.iter_mp3(stream))

    def test_extents_are_available_without_audio_content(self):
        source = b"junk" + bytes(3) + id3() + frame(padding=1) + frame() + ape(ape_item(), 1) + b"TAG" + bytes(125)
        for content in (False, True):
            with self.subTest(content=content), BytesIO(source) as stream:
                items = list(mp3.iter_mp3(stream, content=content))
                pos = 0
                for item in items:
                    self.assertEqual(pos, item.pos)
                    self.assertGreater(item.size, 0)
                    self.assertEqual(item.pos + item.size, item.end)
                    if content or not isinstance(item, mp3.MpegAudioFrame):
                        self.assertEqual(source[item.pos : item.end], item.raw())
                    else:
                        self.assertIsNone(item.data)
                    pos = item.end
                self.assertEqual(len(source), pos)
        with BytesIO(frame(size=420, bitrate=0) * 3) as stream:
            self.assertEqual([420] * 3, [item.size for item in mp3.iter_mp3(stream)])

    def test_layer3_sideinfo_inspection_does_not_validate(self):
        body = replace_bits(bytes(411), 32, 9, 289)
        damaged = bytearray(frame(body=body, crc_bits=256))
        damaged[4] ^= 1
        with BytesIO(damaged) as stream:
            item = mp3.read_frame(stream, content=True)
        with ExitStack() as stack:
            for name in ("_crc16_bits", "_validate_layer3_sideinfo", "_validate_layer3_main"):
                stack.enter_context(patch.object(mp3, name, side_effect=AssertionError("validation during inspection")))
            self.assertEqual(289, item.sideinfo.big_values[0][0])

    def test_local_and_stream_validation_have_distinct_guarantees(self):
        body = replace_bits(bytes(413), 32, 9, 1)
        body = replace_bits(body, 54, 5, 1)
        data = frame(body=body)
        with BytesIO(data) as stream:
            local = mp3.read_frame(stream, content=True)
        local.validate_frame()
        for operation in (local.parse, local.validate):
            with self.assertRaisesRegex(ValueError, "stream validation"):
                operation()
        with BytesIO(data) as stream:
            contextual = next(mp3.iter_mp3(stream, content=True))
        contextual.validate_frame()
        with self.assertRaisesRegex(mp3.InvalidFrame, "bit budget"):
            contextual.validate()

    @staticmethod
    def validate_file(data):
        with patch("builtins.open", return_value=BytesIO(data)):
            return mp3.validate_mp3("audio.mp3")

    def test_file_validation_adds_layout_rules_to_content_checks(self):
        self.assertIsNone(self.validate_file(id3() + frame() * 2 + ape(ape_item(), 1) + b"TAG" + bytes(125)))
        for data in (b"", id3(), b"TAG" + bytes(125)):
            with self.subTest(data=data), self.assertRaisesRegex(mp3.MaybeNotMp3, "no MPEG audio"):
                self.validate_file(data)
        for data in (bytes(3) + frame(), frame() + bytes(3), frame() + bytes(3) + frame()):
            with self.subTest(data=data[:4]), self.assertRaisesRegex(mp3.InvalidFrame, "Standalone padding"):
                self.validate_file(data)
        with self.assertRaisesRegex(mp3.InvalidFrame, "Unrecognized bytes"):
            self.validate_file(b"junk" + frame())
        with self.assertRaisesRegex(mp3.InvalidFrame, "follows trailing metadata"):
            self.validate_file(frame() + id3(flags=16) + frame())
        with self.assertRaises(mp3.MetadataParseError):
            self.validate_file(id3(id3_frame(b"\3\xff")) + frame())
        body = replace_bits(bytes(413), 32, 9, 1)
        body = replace_bits(body, 54, 5, 1)
        with self.assertRaisesRegex(mp3.InvalidFrame, "bit budget"):
            self.validate_file(frame(body=body))


class CopyRecoveryTests(unittest.TestCase):
    @staticmethod
    def copy(folder, data, **kwargs):
        source, destination = folder / "source.mp3", folder / "copy.mp3"
        source.write_bytes(data)
        result = mp3.copy_mpeg_audio(source, destination, **kwargs)
        assert source.read_bytes() == data
        return result, destination.read_bytes()

    def test_copy_reports_exact_bytes_and_skipped_spans(self):
        audio = frame() + frame(padding=1) + frame()
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), b"leading" + audio + b"trailing")
        self.assertEqual(audio, copied)
        self.assertEqual(
            (3, len(audio), 7, 7 + len(audio)), (result.frames, result.bytes_copied, result.start, result.end)
        )
        self.assertEqual((7, 8, result.end), (result.skipped_leading, result.skipped_trailing, result.stop_offset))
        self.assertEqual("non_audio", result.stop_reason)
        self.assertIsNone(result.error)
        self.assertEqual("local", result.validation_scope)
        with self.assertRaises(FrozenInstanceError):
            result.frames = 0

    def test_leading_tags_do_not_expose_embedded_false_frames(self):
        invalid = frame(body=replace_bits(bytes(413), 32, 9, 289))
        audio = frame() * 2
        for embedded in (invalid[:36], frame()):
            leading = id3(id3_frame(b"vendor\0" + embedded + b"tail", ident=b"PRIV"))
            leading += ape(ape_item(value=b"cover\0" + frame(), flags=2), 1, with_header=True)
            with self.subTest(embedded=len(embedded)), TemporaryDirectory() as directory:
                result, copied = self.copy(Path(directory), leading + audio)
            self.assertEqual(audio, copied)
            self.assertEqual(len(leading), result.start)
            self.assertEqual("eof", result.stop_reason)

    def test_invalid_candidates_are_skipped_before_audio(self):
        invalid = frame(body=replace_bits(bytes(413), 32, 9, 289))
        audio = frame() * 2
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), b"junk" + invalid + audio)
        self.assertEqual(audio, copied)
        self.assertEqual(4 + len(invalid), result.start)

    def test_strict_recovery_confirms_sequence_after_junk(self):
        prefix = b"junk" + frame() + b"tail"
        audio = frame() * 2
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), prefix + audio, validate_stream=True)
        self.assertEqual(audio, copied)
        self.assertEqual(len(prefix), result.start)
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), id3() + frame() + b"tail", validate_stream=True)
        self.assertEqual(frame(), copied)
        self.assertEqual("non_audio", result.stop_reason)

    def test_no_audio_preserves_existing_or_absent_destination(self):
        for data in (b"not audio", id3(id3_frame(b"vendor\0" + frame(), ident=b"PRIV"))):
            for existing in (False, True):
                with self.subTest(existing=existing), TemporaryDirectory() as directory:
                    folder = Path(directory)
                    source, destination = folder / "source.mp3", folder / "copy.mp3"
                    source.write_bytes(data)
                    if existing:
                        destination.write_bytes(b"keep existing output")
                    with self.assertRaises(mp3.MaybeNotMp3):
                        mp3.copy_mpeg_audio(source, destination)
                    self.assertEqual(data, source.read_bytes())
                    if existing:
                        self.assertEqual(b"keep existing output", destination.read_bytes())
                    else:
                        self.assertFalse(destination.exists())
                    self.assertFalse(list(folder.glob(".filemeta-*")))

    def test_replace_failure_preserves_destination_and_cleans_temporary(self):
        with TemporaryDirectory() as directory:
            folder = Path(directory)
            source, destination = folder / "source.mp3", folder / "copy.mp3"
            source.write_bytes(frame())
            destination.write_bytes(b"keep")
            with patch.object(mp3.os, "replace", side_effect=OSError("replace failed")):
                with self.assertRaisesRegex(OSError, "replace failed"):
                    mp3.copy_mpeg_audio(source, destination)
            self.assertEqual(b"keep", destination.read_bytes())
            self.assertFalse(list(folder.glob(".filemeta-*")))

    def test_unexpected_read_failure_does_not_commit_partial_output(self):
        iterator = mp3.iter_mp3

        def interrupted(stream, **kwargs):
            yield next(iterator(stream, **kwargs))
            raise OSError("read failed")

        with TemporaryDirectory() as directory:
            folder = Path(directory)
            source, destination = folder / "source.mp3", folder / "copy.mp3"
            source.write_bytes(frame() * 2)
            destination.write_bytes(b"keep")
            with patch.object(mp3, "iter_mp3", side_effect=interrupted):
                with self.assertRaisesRegex(OSError, "read failed"):
                    mp3.copy_mpeg_audio(source, destination)
            self.assertEqual(b"keep", destination.read_bytes())
            self.assertFalse(list(folder.glob(".filemeta-*")))

    def test_known_truncation_reports_recovered_prefix(self):
        audio = frame() * 2
        tail = frame()[:-1]
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), audio + tail)
        self.assertEqual(audio, copied)
        self.assertEqual((2, len(tail), "invalid_frame"), (result.frames, result.skipped_trailing, result.stop_reason))
        self.assertIn("Truncated frame", result.error)

    def test_copy_stops_at_gaps_and_checks_reservoir(self):
        unavailable = frame(body=replace_bits(bytes(413), 0, 9, 511))
        for suffix, reason in ((b"junk" + frame(), "non_audio"), (unavailable + frame(), "invalid_frame")):
            with self.subTest(reason=reason), TemporaryDirectory() as directory:
                result, copied = self.copy(Path(directory), frame() + suffix, validate_stream=True)
            self.assertEqual(frame(), copied)
            self.assertEqual(reason, result.stop_reason)
        unavailable = frame(body=replace_bits(bytes(413), 0, 9, 1))
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), unavailable + frame())
        self.assertEqual(unavailable + frame(), copied)
        self.assertEqual("local", result.validation_scope)
        with TemporaryDirectory() as directory:
            result, copied = self.copy(Path(directory), unavailable + frame(), validate_stream=True)
        self.assertEqual(frame(), copied)
        self.assertEqual("stream", result.validation_scope)
        self.assertEqual(len(unavailable), result.skipped_leading)


if __name__ == "__main__":
    unittest.main()
