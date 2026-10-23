import importlib.util
import subprocess
import sys
import unittest
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from filemeta import mp3
from test_mp3_strict import frame, id3, id3_frame, replace_bits


class RoundtripCheckTests(unittest.TestCase):
    @staticmethod
    def check(original, spans, validate=False):
        stream = BytesIO(original)
        items = [mp3.JunkFrame(0, data) for data in spans]
        with patch("builtins.open", return_value=stream) as opened, patch.object(
            mp3, "read_mp3", return_value=iter(items)
        ):
            mp3._check_mp3_roundtrip("audio.mp3", validate=validate)
        opened.assert_called_once_with("audio.mp3", "rb")
        assert stream.closed

    def test_exact_bytes_pass_without_content_validation(self):
        self.check(b"abcde", [b"ab", b"cde"])
        self.check(b"", [])

    def test_optional_validation_runs(self):
        with self.assertRaisesRegex(mp3.InvalidFrame, "Unrecognized bytes"):
            self.check(b"junk", [b"junk"], validate=True)

    def test_changed_or_reordered_bytes_report_absolute_offset(self):
        for original, spans, offset in ((b"abcde", [b"ab", b"cXe"], 3), (b"abc", [b"b", b"ac"], 0)):
            with self.subTest(spans=spans), self.assertRaisesRegex(mp3.InvalidFrame, f"byte mismatch at pos {offset}"):
                self.check(original, spans)

    def test_extra_output_bytes_are_detected(self):
        with self.assertRaisesRegex(mp3.InvalidFrame, "byte mismatch at pos 3"):
            self.check(b"abc", [b"abcd"])

    def test_missing_output_bytes_are_detected_at_eof(self):
        with self.assertRaisesRegex(mp3.InvalidFrame, "omitted source bytes at pos 2"):
            self.check(b"abc", [b"ab"])


@unittest.skipUnless(importlib.util.find_spec("rich") is not None, "CLI requires the optional rich dependency")
class RoundtripCliTests(unittest.TestCase):
    @staticmethod
    def run_cli(folder, logfile, *extra):
        return subprocess.run(  # noqa: S603 - current Python, fixed module and test-owned arguments
            [sys.executable, "-B", "-m", "filemeta.mp3", str(folder), "--out", str(logfile), *extra],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )

    def test_roundtrip_validation_flag_and_per_file_error_logging(self):
        bad_body = replace_bits(bytes(413), 32, 9, 1)
        bad_body = replace_bits(bad_body, 54, 5, 1)
        cases = {
            "valid.mp3": id3() + frame(),
            "padding.mp3": bytes(3) + frame() + bytes(2),
            "junk.mp3": b"junk" + frame(),
            "bad-metadata.mp3": id3(id3_frame(ident=b"bad!")) + frame(),
            "bad-audio.mp3": frame(body=bad_body),
            "truncated.mp3": frame()[:-1],
        }
        with TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / "input"
            folder.mkdir()
            for name, data in cases.items():
                (folder / name).write_bytes(data)
            for validate, failed in ((False, 1), (True, 4)):
                with self.subTest(validate=validate):
                    logfile = root / f"roundtrip-{validate}.log"
                    args = ("--action", "roundtrip") + (("--validate",) if validate else ())
                    result = self.run_cli(folder, logfile, *args)
                    self.assertEqual(0, result.returncode, result.stderr)
                    self.assertIn(f"{failed}/6 files failed to parse", result.stdout)
                    log = logfile.read_text(encoding="utf-8")
                    self.assertEqual(failed, log.count("-----"))
                    self.assertIn("Roundtrip of <", log)
                    self.assertIn("truncated.mp3", log)
                    self.assertIn("Truncated frame", log)
                    for name in ("junk.mp3", "bad-metadata.mp3", "bad-audio.mp3"):
                        self.assertEqual(validate, name in log)
            self.assertEqual(cases, {path.name: path.read_bytes() for path in folder.iterdir()})

    def test_copy_reports_validation_scope_and_counts(self):
        unavailable = frame(body=replace_bits(bytes(413), 0, 9, 1))
        for strict in (False, True):
            with self.subTest(strict=strict), TemporaryDirectory() as directory:
                root = Path(directory)
                folder = root / "input"
                folder.mkdir()
                (folder / "audio.mp3").write_bytes(unavailable + frame())
                logfile = root / "copy.log"
                args = ("--action", "copy") + (("--validate",) if strict else ())
                result = self.run_cli(folder, logfile, *args)
                self.assertEqual(0, result.returncode, result.stderr)
                self.assertIn("0/1 files failed to parse", result.stdout)
                expected = frame() if strict else unavailable + frame()
                self.assertEqual(expected, (folder / "audio.copy.mp3").read_bytes())
                self.assertIn(
                    "stream validation" if strict else "local validation", logfile.read_text(encoding="utf-8")
                )

    def test_copy_reports_partial_stream_validation_failure(self):
        unavailable = frame(body=replace_bits(bytes(413), 0, 9, 511))
        with TemporaryDirectory() as directory:
            root = Path(directory)
            folder = root / "input"
            folder.mkdir()
            (folder / "audio.mp3").write_bytes(frame() + unavailable)
            logfile = root / "copy.log"
            result = self.run_cli(folder, logfile, "--action", "copy", "--validate")
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertIn("1/1 files failed to parse", result.stdout)
            self.assertEqual(frame(), (folder / "audio.copy.mp3").read_bytes())
            log = logfile.read_text(encoding="utf-8")
            self.assertIn("unavailable reservoir", log)
            self.assertIn("invalid_frame", log)
            self.assertIn("-----", log)

    def test_read_validation_is_already_unconditional(self):
        with TemporaryDirectory() as directory:
            folder = Path(directory)
            result = self.run_cli(folder, folder / "errors.log", "--action", "read", "--validate")
            self.assertEqual(2, result.returncode)
            self.assertIn("--validate requires --action roundtrip or copy", result.stderr)


if __name__ == "__main__":
    unittest.main()
