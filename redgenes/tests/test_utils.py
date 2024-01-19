import subprocess
import pandas as pd
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch, mock_open
from redgenes.utils import (
    read_gff_file,
    extract_gff_info,
    process_gff_info,
    run_command_and_check_outputs,
    copy_and_unzip,
)


# Mock class to simulate the structure of GFF3 file data
class MockRecord:
    def __init__(self, metadata, bounds, fuzzy):
        self.bounds = bounds
        self.fuzzy = fuzzy
        self.metadata = metadata


class MockIntervals:
    def __init__(self, intervals):
        self._intervals = intervals


# Mock generator function to simulate reading a GFF3 file
def mock_read_gff_file(path, format):
    mock_data = [
        [
            "contig1",
            MockIntervals([MockRecord({"key": "value1"}, [(1, 2)], [(False, False)])]),
        ],
        [
            "contig2",
            MockIntervals([MockRecord({"key": "value2"}, [(3, 4)], [(False, False)])]),
        ],
    ]
    return mock_data


class TestUtils(TestCase):
    @patch("redgenes.utils.read", side_effect=mock_read_gff_file)
    def test_read_gff_file(self, mock_read):
        gff_path = "dummy_path"
        gen = read_gff_file(gff_path)
        self.assertTrue(
            isinstance(gen, type(mock_read_gff_file(gff_path, format="gff3")))
        )

    def test_extract_gff_info(self):
        gen = mock_read_gff_file("dummy_path", format="gff3")
        df = extract_gff_info(gen)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(df.shape[0], 2)  # Two records in the mocked data
        self.assertTrue("contig_id" in df.columns)
        self.assertTrue("start" in df.columns)
        self.assertTrue("end" in df.columns)

    def test_process_gff_info(self):
        df = pd.DataFrame({"col1": ["1", "2"], "col2": ["3", "4"]})
        cols_to_front = ["col2"]
        dtype_map = {"col1": int, "col2": str}

        result = process_gff_info(df, cols_to_front, dtype_map)
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(list(result.columns), ["col2", "col1"])  # Check column order
        self.assertEqual(result.dtypes["col1"], "int64")  # Check dtype of col1

    @patch("redgenes.utils.subprocess.run")
    def test_run_command_success(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["echo", "hello"], returncode=0
        )

        try:
            run_command_and_check_outputs(["echo", "hello"], RuntimeError)
        except Exception as e:
            self.fail(
                f"run_command_and_check_outputs raised an exception unexpectedly: {e}"
            )

    @patch("redgenes.utils.subprocess.run")
    def test_run_command_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, ["false"])

        with self.assertRaises(RuntimeError):
            run_command_and_check_outputs(["false"], RuntimeError)

    @patch("redgenes.utils.Path.exists", return_value=True)
    @patch("redgenes.utils.subprocess.run")
    def test_run_command_with_file_check(self, mock_run, mock_exists):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["touch", "testfile"], returncode=0
        )
        files_to_check = ["testfile"]

        try:
            run_command_and_check_outputs(
                ["touch", "testfile"], RuntimeError, files=files_to_check
            )
        except Exception as e:
            self.fail(
                f"run_command_and_check_outputs raised an exception unexpectedly when checking files: {e}"
            )

    @patch("redgenes.utils.shutil.copyfileobj")
    @patch("redgenes.utils.gzip.open", new_callable=mock_open)
    @patch("redgenes.utils.open", new_callable=mock_open)
    @patch("redgenes.utils.Path.exists")
    @patch("redgenes.utils.Path.mkdir")
    @patch("redgenes.utils.shutil.rmtree")
    def test_copy_and_unzip(
        self,
        mock_rmtree,
        mock_mkdir,
        mock_exists,
        mock_open_file,
        mock_gzip_open,
        mock_copyfileobj,
    ):
        zip_path = "test_genome.fna.gz"
        tmp_dir = "/tmp"
        source_filename_unzipped = "test_genome.fna"

        # Test case when zip file does not exist
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            with copy_and_unzip(zip_path, tmp_dir):
                pass

        # Test case for successful unzip
        mock_exists.return_value = True
        with copy_and_unzip(zip_path, tmp_dir) as unzipped_file:
            mock_mkdir.assert_called_once()
            mock_open_file.assert_called()
            mock_gzip_open.assert_called()
            mock_copyfileobj.assert_called()
            self.assertEqual(
                unzipped_file,
                Path(tmp_dir) / "test_genome" / source_filename_unzipped,
            )

        # Ensure rmtree is called
        mock_rmtree.assert_called_once_with(Path(tmp_dir) / "test_genome")


if __name__ == "__main__":
    main()
