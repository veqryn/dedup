import unittest
import tempfile

from dedup import dedup


class TestDedup(unittest.TestCase):
    def test_dedup(self):
        with tempfile.NamedTemporaryFile(mode="w+", prefix="dedup.", suffix=".log") as out_file, \
                open("../testdata/testdata.log", "r") as in_file:
            dedup(out_file, in_file)
            # Seek back to beginning of file, read all content
            out_file.seek(0, 0)
            content = out_file.readlines()
            # Make sure there aren't any duplicates
            s = set()
            for line in content:
                s.add(line)
            self.assertEqual(len(s), len(content))
            # Assert line count is equal to 100
            self.assertEqual(len(content), 100)


if __name__ == '__main__':
    unittest.main()
