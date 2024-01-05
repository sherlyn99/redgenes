import sqlite3
import unittest
from pathlib import Path
from redgenes.gene_annotations import get_patch_list


class TestBase(unittest.TestCase):
    def setUp(self):
        # conn = sqlite3.connect(":memory:")
        pass

    def tearDown(self):
        pass

    def test_get_patch_list(self):
        # Create a temporary directory with sample patch files
        patch_dir = Path("tmp_patch_dir")
        patch_dir.mkdir()

        # Case 1: FileNotFoundError when patch_dir does not exist
        self.assertRaises(FileNotFoundError, get_patch_list, "tmp_patch123_dir")

        # Case 2: returns [] when patch_dir is empty
        res2 = get_patch_list("tmp_patch_dir")

        # Case 3: returns a list of patch file paths when patch_dir is not empty
        (patch_dir / "thisisnotapatch.txt").touch()
        for i in range(1, 6):
            if i == 3:
                continue
            (patch_dir / f"00{i}.sql").touch()

        res3 = get_patch_list(patch_dir)
        exp3 = [
            Path("tmp_patch_dir/001.sql"),
            Path("tmp_patch_dir/002.sql"),
            Path("tmp_patch_dir/004.sql"),
            Path("tmp_patch_dir/005.sql"),
        ]

        # Clean the temporary directory
        (patch_dir / "thisisnotapatch.txt").unlink()
        for file_path in patch_dir.glob("*.sql"):
            file_path.unlink()
        patch_dir.rmdir()

        self.assertEqual(res2, [])
        self.assertEqual(res3, exp3)


if __name__ == "__main__":
    unittest.main()
