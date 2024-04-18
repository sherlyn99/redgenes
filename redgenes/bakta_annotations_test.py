import pytest
from unittest.mock import MagicMock, patch
import pandas as pd
from collections import defaultdict
from pathlib import Path
from bakta_annotations import process_dbxref, insert_dbxref_info, extract_bakta_results, fetch_entity_id, insert_bakta_results

# Mocking the SQL transaction object
TRN = MagicMock()

# Fixtures
@pytest.fixture
def setup_database():
    # Setup database connection, if needed
    TRN.reset_mock()
    yield
    # Teardown database connection, if needed
    TRN.reset_mock()

# Test process_dbxref function
def test_process_dbxref_valid_input():
    input_str = "kegg:K12345, refseq:XP_123456"
    expected_output = defaultdict(list, {'kegg': ['K12345'], 'refseq': ['XP_123456']})
    assert process_dbxref(input_str) == expected_output

def test_process_dbxref_invalid_input():
    input_str = "keggK12345, refseqXP_123456"  # No colon
    assert process_dbxref(input_str) == defaultdict(list)

def test_process_dbxref_empty_input():
    assert process_dbxref("") == defaultdict(list)
    assert process_dbxref(None) == defaultdict(list)

# Test insert_dbxref_info function
@patch('bakta_annotations.TRN', new_callable=MagicMock)
def test_insert_dbxref_info_valid_data(mock_trn):
    bakta_accession = "ACC123"
    dbxref_data = defaultdict(list, {'kegg': ['K12345'], 'refseq': ['XP_123456']})
    insert_dbxref_info(bakta_accession, dbxref_data)
    mock_trn.add.assert_called()
    mock_trn.execute.assert_called_once()

# Test extract_bakta_results function
def test_extract_bakta_results_valid_file(tmpdir):
    # Create a temporary CSV file
    df = pd.DataFrame({
        "contig_ID": ["contig1", "contig2"],
        "type": ["gene", "gap"],
        "start": [1, 100],
        "stop": [900, 200],
        "strand": ["+", "-"],
        "locus_tag": ["LT1", "LT2"],
        "gene": ["gene1", "gene2"],
        "product": ["enzyme1", "enzyme2"],
        "dbxrefs": ["kegg:K12345", "refseq:XP_123456"]
    })
    tsv_file = tmpdir.join("test.tsv")
    df.to_csv(tsv_file, sep='\t', index=False)
    result_df = extract_bakta_results(str(tsv_file))
    assert len(result_df) == 1  # Only one row should be returned (gap rows excluded)

@patch('bakta_annotations.TRN', new_callable=MagicMock)
def test_fetch_entity_id_found(mock_trn):
    mock_trn.execute_fetchflatten.return_value = [123]
    row = {"local_path": "/fake/path", "assembly_accession": "XYZ123"}
    assert fetch_entity_id(row) == 123

@patch('bakta_annotations.TRN', new_callable=MagicMock)
def test_fetch_entity_id_not_found(mock_trn):
    mock_trn.execute_fetchflatten.return_value = []
    row = {"local_path": "/fake/path", "assembly_accession": "XYZ123"}
    assert fetch_entity_id(row) is None

# Run tests
if __name__ == "__main__":
    pytest.main()
