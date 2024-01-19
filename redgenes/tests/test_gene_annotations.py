from unittest import TestCase, main
from unittest.mock import patch, MagicMock
import pandas as pd
from redgenes.gene_annotations import (
    extract_prodigal_results,
    extract_kofamscan_results,
    extract_barrnap_results,
)


class TestExtractionFunctions(TestCase):
    @patch("redgenes.gene_annotations.extract_gff_info")
    @patch("redgenes.gene_annotations.read_gff_file")
    @patch("redgenes.gene_annotations.process_gff_info")
    def test_extract_prodigal_results(
        self, mock_process, mock_read_gff, mock_extract_gff
    ):
        # Setup
        mock_read_gff.return_value = "mock_gen"
        mock_extract_gff.return_value = pd.DataFrame({"col1": [1, 2, 3]})
        mock_process.return_value = pd.DataFrame({"processed_col1": [1, 2, 3]})

        # Test
        result = extract_prodigal_results("dummy_path")
        self.assertIsInstance(result, pd.DataFrame)

    @patch("pandas.read_csv")
    def test_extract_kofamscan_results(self, mock_read_csv):
        # Setup
        mock_read_csv.return_value = pd.DataFrame(
            {
                "tab": [1, 2, 3],
                "gene_name": [1, 2, 3],
                "ko": [1, 2, 3],
                "threshold": [1, 2, 3],
                "score": [1, 2, 3],
                "e_value": [1, 2, 3],
                "ko_definition": [1, 2, 3],
            }
        )

        # Test
        result = extract_kofamscan_results("dummy_path")
        self.assertIsInstance(result, pd.DataFrame)

    @patch("redgenes.gene_annotations.extract_gff_info")
    @patch("redgenes.gene_annotations.read_gff_file")
    @patch("redgenes.gene_annotations.process_gff_info")
    def test_extract_barrnap_results(
        self, mock_process, mock_read_gff, mock_extract_gff
    ):
        # Setup
        mock_read_gff.return_value = "mock_gen"
        mock_extract_gff.return_value = pd.DataFrame(
            {"Name": ["16S_rRNA", "other"], "col1": [1, 2]}
        )
        mock_process.return_value = pd.DataFrame({"processed_col1": [1]})

        # Test
        result = extract_barrnap_results("dummy_path")
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(result.shape[0], 1)  # Only one record with '16S_rRNA'


if __name__ == "__main__":
    main()
