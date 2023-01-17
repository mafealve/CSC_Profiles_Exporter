import unittest
from src.serving_layer.data_exporter_job import DataExporterJob
from tests.serving_layer.test_data_exporter_job_synthetic_data import TestDataExporterJobSyntheticData


class TestDataExporterJob(unittest.TestCase):
    """
    For testing Data Exporter Job class funtionalities.
    """

    def test_dataset_size(self):
        """
        Full dataset of three rows and the header.
        :return: None (assertion)
        """

        test_dataset = TestDataExporterJobSyntheticData.get_test_full_dataset()

        assert len(test_dataset) == 4  # Input

        result = DataExporterJob.convert_to_filecontent(dataset=test_dataset,
                                                        dt_col_list="ts",
                                                        dt_col_output_format="%Y-%m-%d %H:%M:%S.%f",
                                                        col_sep="|",
                                                        output_with_header="Y",
                                                        output_line_sep="\n")
        assert len(result) == 311  # Output: File content

    def test_empty_dataset_size(self):
        """
        Only the header.
        :return: None (assertion)
        """

        test_dataset = TestDataExporterJobSyntheticData.get_test_only_header_dataset()

        assert len(test_dataset) == 1  # Input

        result = DataExporterJob.convert_to_filecontent(dataset=test_dataset,
                                                        dt_col_list="ts",
                                                        dt_col_output_format="%Y-%m-%d %H:%M:%S.%f",
                                                        col_sep="|",
                                                        output_with_header="Y",
                                                        output_line_sep="\n")

        assert len(result) == 48 + 1  # Output: File content header (column names, separators and new line)



