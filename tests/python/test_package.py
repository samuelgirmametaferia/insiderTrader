import unittest

import insidertrader


class PackageTest(unittest.TestCase):
    def test_version_is_explicit(self) -> None:
        self.assertEqual(insidertrader.__version__, "0.1.0")


if __name__ == "__main__":
    unittest.main()

