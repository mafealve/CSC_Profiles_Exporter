class TestDataExporterJobSyntheticData:
    """
    Synthetic data to test the Data Exporter Job.
    """

    @staticmethod
    def get_header_list():
        """
        Get header as a list.
        :return: Header as list (often called in other methods).
        """
        header_list = [{'VarCharValue': 'ts'}, {'VarCharValue': 'user_alias'}, {'VarCharValue': 'placement_id'}, {'VarCharValue': 'question_id'}, {'VarCharValue': 'answer_id'}]
        return header_list

    @staticmethod
    def get_test_full_dataset():
        """
        Get a full synthetic dataset.
        :return: Dataset with the Athena structure.
        """
        dataset_list = list()

        header_list = TestDataExporterJobSyntheticData.get_header_list()
        row1_list = [{'VarCharValue': '2023-01-15 18:18:42.292570'}, {'VarCharValue': 'ben_folds'}, {'VarCharValue': 'bing.com/ads/'}, {'VarCharValue': 'YOUR-AGE-GROUP'}, {'VarCharValue': 'Middle-aged Adults'}]
        row2_list = [{'VarCharValue': '2023-01-15 18:22:24.832907'}, {'VarCharValue': 'michael_sembello'}, {'VarCharValue': 'cnn.com/international/'}, {'VarCharValue': 'YOUR-AGE-GROUP'}, {'VarCharValue': 'Children'}]
        row3_list = [{'VarCharValue': '2023-01-15 18:21:03.561761'}, {'VarCharValue': 'michael_bolton'}, {'VarCharValue': 'reddit.com/sports/'}, {'VarCharValue': 'SLEEP-MORE-THAN-8H'}, {'VarCharValue': 'NO'}]

        header_dict = dict()
        header_dict["Data"] = header_list

        row1_dict = dict()
        row1_dict["Data"] = row1_list

        row2_dict = dict()
        row2_dict["Data"] = row2_list

        row3_dict = dict()
        row3_dict["Data"] = row3_list

        dataset_list.append(header_dict)
        dataset_list.append(row1_dict)
        dataset_list.append(row2_dict)
        dataset_list.append(row3_dict)

        return dataset_list

    @staticmethod
    def get_test_only_header_dataset():
        """
        Get an only header dataset.
        :return: Dataset with the Athena structure.
        """
        dataset_list = list()

        header_list = TestDataExporterJobSyntheticData.get_header_list()
        header_dict = dict()
        header_dict["Data"] = header_list

        dataset_list.append(header_dict)
        return dataset_list
