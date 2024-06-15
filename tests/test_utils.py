from genutility.test import MyTestCase

from filemeta.utils import getattrnotnone


class MyClass:
    def __init__(self):
        self.a = None
        self.b = 1


class Test(MyTestCase):
    def test_getattrnotnone(self):
        m = MyClass()
        with self.assertRaises(AttributeError):
            getattrnotnone(m, "a")

        result = getattrnotnone(m, "b")
        truth = 1
        self.assertEqual(truth, result)


if __name__ == "__main__":
    import unittest

    unittest.main()
