from genutility.test import MyTestCase, parametrize

from filemeta.exif import recstr


class Test(MyTestCase):
    @parametrize(
        ({}, "{}"),
        (["asd", (1, 2)], "[asd, (1, 2)]"),
    )
    def test_recstr(self, obj, truth):
        result = recstr(obj)
        self.assertEqual(truth, result)


if __name__ == "__main__":
    import unittest

    unittest.main()
