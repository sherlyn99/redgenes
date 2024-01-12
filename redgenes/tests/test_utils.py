import subprocess
from pathlib import Path
from unittest import TestCase, main
from unittest.mock import patch, mock_open
from redgenes.exceptions import InvalidFna
from redgenes.utils import (
    generate_insert_stmt,
    read_gff_file,
    extract_qualifier,
    process_qualifiers,
    run_unzip_fna,
    run_zip_fna,
    copy_and_unzip,
    run_command,
)


class TestUtils(TestCase):
    def test_generate_insert_stmt(self):
        table_name = "test_table"
        column_names = ["col1", "col2", "col3"]
        expected = "insert into test_table (col1, col2, col3) values (?, ?, ?);"
        result = generate_insert_stmt(table_name, column_names)
        self.assertEqual(result, expected)

    def test_read_gff_file_file_not_found(self):
        with self.assertRaises(FileNotFoundError):
            read_gff_file("nonexistent.gff")

    def test_extract_qualifier(self):
        # Test case when the qualifier key exists and is a list
        qualifiers_dict = {"key1": ["value1", "value2"], "key2": "value3"}
        result = extract_qualifier(qualifiers_dict, "key1", str)
        assert (
            result == "value1"
        ), "Failed to extract first value from a list of qualifiers"

        # Test case when the qualifier key exists and is not a list
        result = extract_qualifier(qualifiers_dict, "key2", str)
        assert (
            result == "value3"
        ), "Failed to extract value when qualifier is not a list"

        # Test case when the qualifier key does not exist
        result = extract_qualifier(qualifiers_dict, "key3", int)
        assert result is None, "Failed to return None for non-existent key"

        # Test case when the qualifier value is 'None' (string)
        qualifiers_dict = {"key1": "None"}
        result = extract_qualifier(qualifiers_dict, "key1", str)
        assert result is None, "Failed to return None for 'None' string value"

        # Test case for converting type
        qualifiers_dict = {"key1": "123"}
        result = extract_qualifier(qualifiers_dict, "key1", int)
        assert result == 123, "Failed to convert string to integer"

    def test_process_qualifiers(self):
        # Setup
        qualifiers_dict = {
            "key1": "value1",
            "key2": ["value2a", "value2b"],
            "key3": "123",
            "key4": "None",
        }
        dtype_map = {
            "key1": str,
            "key2": str,
            "key3": int,
            "key4": str,
            "key5": float,  # key5 does not exist in qualifiers_dict
        }

        # Expected result should handle the conversion and default values correctly
        expected_result = ["value1", "value2a", 123, None, None]

        # Call the function
        result = process_qualifiers(qualifiers_dict, dtype_map)

        # Assertions
        assert (
            result == expected_result
        ), f"Result {result} does not match expected {expected_result}"

    @patch("redgenes.utils.Path.exists")
    def test_run_unzip_fna_file_not_found(self, mock_exists):
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            run_unzip_fna("nonexistent.fna.gz")

    @patch("redgenes.utils.Path.exists")
    @patch("redgenes.utils.run_command")
    def test_run_unzip_fna_success(self, mock_run_command, mock_exists):
        mock_exists.side_effect = [
            True,
            False,
        ]  # First call (for .gz file), second call (for unzipped file)
        run_unzip_fna("test.fna.gz")
        mock_run_command.assert_called_with(
            ["gzip", "-d", "test.fna.gz"], InvalidFna, "error"
        )

    @patch("redgenes.utils.Path.exists")
    @patch("redgenes.utils.run_command")
    def test_run_zip_fna(self, mock_run_command, mock_exists):
        # Setup
        filepath = "test_file.fna"
        zipped_path = "test_file.fna.gz"

        # Test case when unzipped file does not exist
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            run_zip_fna(filepath)

        # Test case when zipped file already exists
        mock_exists.side_effect = [
            True,
            True,
        ]  # First call for unzipped, second for zipped
        with self.assertRaises(ValueError):
            run_zip_fna(filepath)

        # Test case for successful zipping
        mock_exists.side_effect = [
            True,
            False,
        ]  # First call for unzipped, second for zipped not existing
        run_zip_fna(filepath)
        mock_run_command.assert_called_with(["gzip", filepath], InvalidFna, "error")

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

        # # Test case when zip file does not exist
        # mock_exists.return_value = False
        # with self.assertRaises(FileNotFoundError):
        #     with copy_and_unzip(zip_path, tmp_dir):
        #         pass

        # Test case for successful unzip
        mock_exists.return_value = True
        with copy_and_unzip(zip_path, tmp_dir) as unzipped_file:
            mock_mkdir.assert_called_once()
            mock_open_file.assert_called()
            mock_gzip_open.assert_called()
            mock_copyfileobj.assert_called()
            self.assertEqual(
                unzipped_file, Path(tmp_dir) / "test_genome" / source_filename_unzipped
            )

        # Ensure rmtree is called
        mock_rmtree.assert_called_once_with(Path(tmp_dir) / "test_genome")

    @patch("redgenes.utils.subprocess.run")
    def test_run_command_success(self, mock_run):
        mock_run.return_value = subprocess.CompletedProcess(
            args=["ls", "-la"], returncode=0
        )

        try:
            run_command(["ls", "-la"], RuntimeError, "list error")
        except Exception as e:
            self.fail(f"run_command raised an exception unexpectedly: {e}")

    @patch("redgenes.utils.subprocess.run")
    def test_run_command_failure(self, mock_run):
        mock_run.side_effect = subprocess.CalledProcessError(1, ["ls", "-la"])

        with self.assertRaises(RuntimeError) as context:
            run_command(["ls", "-la"], RuntimeError, "list error")
        self.assertIn("There is an list error:", str(context.exception))


if __name__ == "__main__":
    main()
