from unittest.mock import patch

import piexif
from genutility.test import MyTestCase, parametrize

from filemeta.exif import exif_table, recstr


class Test(MyTestCase):
    @parametrize(
        ({}, "{}"),
        (["asd", (1, 2)], "[asd, (1, 2)]"),
    )
    def test_recstr(self, obj, truth):
        result = recstr(obj)
        self.assertEqual(truth, result)

    def test_exif_table_labels_byte_enums(self):
        exif = {
            "0th": {},
            "Exif": {piexif.ExifIFD.FileSource: b"\x03"},
            "GPS": {piexif.GPSIFD.GPSAltitudeRef: 1},
            "1st": {},
        }
        with patch("filemeta.exif.piexif_load", return_value=exif):
            labels = {name: value_label for _, name, _, _, value_label in exif_table("unused.jpg")}

        self.assertEqual("Digital still camera", labels["FileSource"])
        self.assertEqual("Negative ellipsoidal height", labels["GPSAltitudeRef"])


if __name__ == "__main__":
    import unittest

    unittest.main()
