import os
import shutil
from unittest import TestCase, main
from redgenes.quality_control import run_checkm, extract_checkm_results


CHECKM_OUTPUT = "redgenes/tests/tests_out/tests_checkm_out/storage/bin_stats_ext.tsv"
CHECKM_RES = [
    "k__Bacteria",
    100.0,
    17.225705329153605,
    3311,
    3314,
    8062,
    8062,
    1642,
    1642,
    906.0715795832075,
    905.1336753168376,
    0.8947607719058948,
    11,
    5028,
]


class TestQualityControlUtils(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.outdir = "./redgenes/tests/tests_out/tests_checkm_out"

    @classmethod
    def tearDownClass(cls):
        cls.safe_delete_directory(cls.outdir)

    @classmethod
    def safe_delete_directory(cls, dir_path):
        """Safely deletes a directory and all its contents."""
        try:
            if os.path.exists(dir_path) and os.path.isdir(dir_path):
                shutil.rmtree(dir_path)
        except Exception as e:
            raise Exception(f"Failed to delete the directory {dir_path}. Error: {e}")

    def test_1_run_checkm(self):
        indir = "./redgenes/tests/data/hA10"
        outdir = self.outdir
        res_actual = run_checkm(indir, outdir)
        res_expected = CHECKM_OUTPUT
        assert (
            res_actual == res_expected
        ), f"Actual result {res_actual} does not match expected result {res_expected}."

    def test_2_extract_checkm_results(self):
        inpath = CHECKM_OUTPUT
        res_actual = extract_checkm_results(inpath)
        res_expected = CHECKM_RES
        assert (
            res_actual == res_expected
        ), f"Actual result {res_actual} does not match expected result {res_expected}."


if __name__ == "__main__":
    main()
