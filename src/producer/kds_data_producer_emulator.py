import datetime
import json
import boto3
import random
from datetime import datetime
from datetime import timedelta


class KDSDataProduceEmulator:

    @staticmethod
    def read_file_content(path):
        with open(path, "r") as f:
            read_content = f.read()
            return read_content

    @staticmethod
    def file_content_key_values_list(file_content, key_column_name):
        result_list = list()
        content_list = file_content.split(row_sep)
        columns = content_list[0].split(col_sep)
        number_of_columns = len(columns)
        if key_column_name == "*":  # All columns
            for row in content_list[1:]:
                row_values = row.split(col_sep)
                if len(row_values) == number_of_columns:
                    result_list.append(row)
        else:  # Only values for the key column
            key_column_position = 0
            key_found = False
            for idx, val in enumerate(columns):
                if val == key_column_name:
                    key_column_position = idx
                    key_found = True
                    break

            if key_found:
                for row in content_list[1:]:
                    row_values = row.split(col_sep)
                    if len(row_values) == number_of_columns:
                        result_list.append(row_values[key_column_position])
            else:
                result_list = []
        return result_list

    @staticmethod
    def get_data(placement_id_list, user_alias_list, placements_questions_list):
        random_delay = random.randint(0, 30)
        ts = datetime.now() - timedelta(seconds=random_delay)

        try:
            placement_id = random.choice(placement_id_list)
            user_alias = random.choice(user_alias_list)
            placements_questions_row = random.choice(placements_questions_list)
        except Exception as ex:
            placement_id = placement_id_list[0]
            user_alias = user_alias_list[0]
            placements_questions_row = placements_questions_list[0]

        selected_question_id_according_to_placement = placements_questions_row.split(col_sep)[1]
        selected_placement_id_according_to_placement = placements_questions_row.split(col_sep)[0]

        if placement_id == selected_placement_id_according_to_placement:
            # Join
            answers_for_selected_question_list = list()
            for qa in questions_answers_list:
                q = qa.split(col_sep)[0]
                a = qa.split(col_sep)[1]
                if q == selected_question_id_according_to_placement:
                    answers_for_selected_question_list.append(a)

            if answers_for_selected_question_list is None or len(answers_for_selected_question_list) == 0:
                return None
            else:
                selected_answer_id = random.choice(answers_for_selected_question_list)

            result = {"ts": ts.strftime(ts_format),
                      "placement_id": placement_id,
                      "user_alias": user_alias,
                      "question_id": selected_question_id_according_to_placement,
                      "answer_id": selected_answer_id}
        else:  # No question (randomly generated) for this placement.
            result = None
        return result

    @staticmethod
    def generate_data_stream(stream_name,
                             kinesis_client,
                             placement_id_list,
                             user_alias_list,
                             placements_questions_list,
                             size=10):
        i = 0
        while i < size:
            data = KDSDataProduceEmulator.get_data(placement_id_list=placement_id_list,
                                                   user_alias_list=user_alias_list,
                                                   placements_questions_list=placements_questions_list)
            if data is not None:
                print(i, data)
                kinesis_client.put_record(
                    StreamName=stream_name,
                    Data=json.dumps(data),
                    PartitionKey=data["ts"]
                )
                i += 1


if __name__ == '__main__':

    STREAM_NAME = "cs-polls-data-kds"
    col_sep = ","
    row_sep = "\n"
    base_path = "/home/marcos.alzate/Documents/NEW_PC/CSc2/DB/"  # Local path
    ts_format = "%Y-%m-%d %H:%M:%S.%f"
    date_format = "%Y-%m-%d %H:%M:%S"

    number_of_events = 10

    credentials = KDSDataProduceEmulator.read_file_content(path=base_path + "credentials")
    credentials_list = credentials.split(row_sep)
    ACCESS_KEY_ID = credentials_list[0]
    SECRET_ACCESS_KEY = credentials_list[1]

    publisher_placement = KDSDataProduceEmulator.read_file_content(path=base_path + "publisher_placement.csv")
    questions = KDSDataProduceEmulator.read_file_content(path=base_path + "questions.csv")
    questions_answers = KDSDataProduceEmulator.read_file_content(path=base_path + "questions_answers.csv")
    users = KDSDataProduceEmulator.read_file_content(path=base_path + "users.csv")
    placements_questions = KDSDataProduceEmulator.read_file_content(path=base_path + "placements_questions.csv")

    placement_id_list = KDSDataProduceEmulator.file_content_key_values_list(file_content=publisher_placement,
                                                                            key_column_name="placement_id")
    user_alias_list = KDSDataProduceEmulator.file_content_key_values_list(file_content=users,
                                                                          key_column_name="user_alias")
    questions_answers_list = KDSDataProduceEmulator.file_content_key_values_list(file_content=questions_answers,
                                                                                 key_column_name="*")
    placements_questions_list = KDSDataProduceEmulator.file_content_key_values_list(file_content=placements_questions,
                                                                                    key_column_name="*")

    client = boto3.client("kinesis",
                          aws_access_key_id=ACCESS_KEY_ID,
                          aws_secret_access_key=SECRET_ACCESS_KEY)

    KDSDataProduceEmulator.generate_data_stream(stream_name=STREAM_NAME,
                                                kinesis_client=client,
                                                placement_id_list=placement_id_list,
                                                user_alias_list=user_alias_list,
                                                placements_questions_list=placements_questions_list,
                                                size=number_of_events
                                                )

