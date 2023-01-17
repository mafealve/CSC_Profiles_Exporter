import boto3
import time
import sys
import os
from awsglue.utils import getResolvedOptions
from datetime import datetime
from botocore.exceptions import NoCredentialsError


class DataExporterJob:
    """
    The purpose of this class is reporting data to multiple destinations (targets) based on different queries that run
    over a source database.
    """

    @staticmethod
    def get_query_results(base_query):
        """
        Method to execute a query.
        :param base_query: Base query to be executed.
        :return: Dataset with the result.
        """

        response = athena_client.start_query_execution(
            QueryString=base_query,
            ResultConfiguration={"OutputLocation": RESULT_OUTPUT_LOCATION},
            WorkGroup='primary'
        )
        execution_id = response["QueryExecutionId"]
        query_status = DataExporterJob.has_query_succeeded(execution_id=execution_id)

        if query_status:
            response = athena_client.get_query_results(
                QueryExecutionId=execution_id
            )
            results = response['ResultSet']['Rows']
        else:
            results = None
        return results

    @staticmethod
    def has_query_succeeded(execution_id):
        """
        This method determines if the query's execution is done or not.
        :param execution_id: Identification of the execution.
        :return: True or False depending on the status of the execution.
        """
        state = "RUNNING"
        max_execution = 5

        while max_execution > 0 and state in ["RUNNING", "QUEUED"]:
            max_execution -= 1
            response = athena_client.get_query_execution(QueryExecutionId=execution_id)
            if (
                "QueryExecution" in response
                and "Status" in response["QueryExecution"]
                and "State" in response["QueryExecution"]["Status"]
            ):
                state = response["QueryExecution"]["Status"]["State"]
                if state == "SUCCEEDED":
                    return True

            time.sleep(30)

        return False

    @staticmethod
    def create_and_change_to_download_directory(path, change_dir_path=None):
        """
        Method to create a temporary path on the computer that runs this job.
        :param path: Required path to be created.
        :param change_dir_path: Directory to change (if None, it will be the just created directory).
        :return:
        """
        os.makedirs(path, exist_ok=True)
        if change_dir_path is None:
            os.chdir(path)
        else:
            os.chdir(change_dir_path)
        cwd = os.getcwd()
        print("Current working directory: " + cwd)

    @staticmethod
    def upload_to_aws_s3(local_file_full_path, bucket, s3_file):
        """
        Static method to upload files from a local specific location to S3.
        :param local_file_full_path: Local path of the source file.
        :param bucket: Destination bucket.
        :param s3_file: Filename in S3 (full path without the bucket).
        :return: True or False depending on whether it was successful or not.
        """
        s3 = boto3.client('s3')
        try:
            s3.upload_file(local_file_full_path, bucket, s3_file)
            print("(Upload Successful)")
            print("(%s) Upload ----> Finished!" % s3_file)
            return True
        except NoCredentialsError:
            print("Credentials not available")
            return False
        except Exception as ex:
            print("Unexpected error:" + str(sys.exc_info()[0]))
            raise

    @staticmethod
    def convert_to_filecontent(dataset):
        """
        Method to convert the dataset (Athena result format) into output file content.
        :param dataset: Athena format dataset.
        :return: Output file content to be written out.
        """
        position = 0
        datetime_columns_positions = list()
        output_file_content_list = list()
        for row in dataset:
            if position == 0:  # Column headers.
                columns_list = list()
                for col in row["Data"]:
                    columns_list.append(col["VarCharValue"])

                # Column names joined with column separator parameter.
                column_names_line = column_separator_parameter.join(columns_list)
                print(column_names_line)
                for pos, column_name in enumerate(columns_list):
                    if column_name in datetime_columns_list:
                        datetime_columns_positions.append(pos)

                # Deciding if put the header or not.
                if output_with_header_parameter != "N":
                    # The row/line is added to the file (content).
                    output_file_content_list.append(column_names_line)
            else:  # Column values.
                row_values_list = list()
                current_position = 0
                for col in row["Data"]:
                    current_value = col["VarCharValue"]
                    # The column value requires datetime transformations.
                    if current_position in datetime_columns_positions:
                        try:
                            curr_val_dt = datetime.strptime(current_value, input_datetime_format)
                            curr_val_str = curr_val_dt.strftime(datetime_columns_output_format_parameter)
                        except Exception as ex:
                            curr_val_str = "(DATETIME CONVERSION ERROR)"
                        row_values_list.append(curr_val_str)
                    else:
                        # If the column separator is present in the column value it will be removed.
                        clean_current_value = current_value.replace(column_separator_parameter, "")
                        row_values_list.append(clean_current_value)
                    current_position += 1

                # Column values joined with column separator parameter.
                column_values_line = column_separator_parameter.join(row_values_list)
                print(column_values_line)

                # The row/line is added to the file (content).
                output_file_content_list.append(column_values_line)
            position += 1

        # File data content.
        file_data = output_line_sep.join(output_file_content_list) + output_line_sep
        return file_data

if __name__ == '__main__':

    # APPLICATION PARAMETERS
    athena_client = boto3.client("athena")

    DATABASE_NAME = "cs"
    TABLE_NAME = "observations"
    RESULT_OUTPUT_LOCATION = "s3://cs-datalake-dev-test/athena/temp/"
    input_datetime_format = "%Y-%m-%d %H:%M:%S.%f"
    filename_date_format = "%Y%m%d_%H%M%S"

    output_bucket_name = "cs-datalake-dev-test"
    path_in_bucket = "reporter_job_output"  # Any path inside the bucket (don't include final slash).
    output_path = "Downloads/cs/exporter/output"
    aws_s3_dir_sep = "/"
    dir_sep = "/"
    home = "/tmp"
    full_output_path = home + dir_sep + output_path
    output_line_sep = "\n"

    # JOB PARAMETERS
    etl_arguments = sys.argv

    args = getResolvedOptions(etl_arguments, ["publisher_id"])
    publisher_id_parameter = args.get("publisher_id", "")
    raw_publisher_id_parameter = publisher_id_parameter

    args = getResolvedOptions(etl_arguments, ["column_separator"])
    column_separator_parameter = args.get("column_separator", "")
    raw_column_separator_parameter = column_separator_parameter

    args = getResolvedOptions(etl_arguments, ["datetime_columns_list"])
    datetime_columns_list_parameter = args.get("datetime_columns_list", "")
    raw_datetime_columns_list_parameter = datetime_columns_list_parameter

    args = getResolvedOptions(etl_arguments, ["datetime_columns_output_format"])
    datetime_columns_output_format_parameter = args.get("datetime_columns_output_format", "")
    raw_datetime_columns_output_format_parameter = datetime_columns_output_format_parameter

    datetime_columns_list = datetime_columns_list_parameter.split(",")

    args = getResolvedOptions(etl_arguments, ["output_with_header"])
    output_with_header_parameter = args.get("output_with_header", "")
    raw_output_with_header_parameter = output_with_header_parameter
    output_with_header_parameter = output_with_header_parameter.upper()

    print("PARAMETERS")
    print("---->publisher_id: ", raw_publisher_id_parameter)
    print("---->column_separator: ", raw_column_separator_parameter)
    print("---->datetime_columns_list: ", raw_datetime_columns_list_parameter)
    print("---->datetime_columns_output_format: ", raw_datetime_columns_output_format_parameter)
    print("---->output_with_header: ", raw_output_with_header_parameter)

    # Create local temporary directory.
    DataExporterJob.create_and_change_to_download_directory(path=output_path)

    # Base query to be executed.
    query = """
with 
    raw_observations as 
    (
     select t."array" as content, 
            t.publisher_id, 
            t.year, 
            t.month, 
            t.day, 
            t.hour, 
            t.minute 
     from cs.observations as t
    ),
    observations as 
    (
     select transform(ro.content, parent -> parent.ts)[1] as ts,
        transform(ro.content, parent -> parent.placement_id)[1] as placement_id,
        transform(ro.content, parent -> parent.user_alias)[1] as user_alias,
        transform(ro.content, parent -> parent.question_id)[1] as question_id,
        transform(ro.content, parent -> parent.answer_id)[1] as answer_id,
        ro.publisher_id, 
        ro.year, 
        ro.month, 
        ro.day, 
        ro.hour, 
        ro.minute,
        ro.content
     from raw_observations as ro
    ) 
select obs.ts,
       obs.user_alias,
       obs.placement_id,
       obs.question_id,
       obs.answer_id
from observations as obs
     left outer join
     cs.lookup_questions as qst
     on obs.question_id = qst.question_id
     left outer join
     cs.lookup_publisher_placement as pubplc
     on obs.placement_id = pubplc.placement_id
where pubplc.publisher_id = '{publisher_id}'
union 
select obs.ts,
       obs.user_alias,
       obs.placement_id,
       obs.question_id,
       obs.answer_id
from observations as obs
     left outer join
     cs.lookup_questions as qst
     on obs.question_id = qst.question_id
where qst.is_syndicated = 1;
    """.format(publisher_id=publisher_id_parameter)

    # Query Results
    query_results = DataExporterJob.get_query_results(base_query=query)

    # Build the output based on the query results.
    if query_results is not None:
        file_data = DataExporterJob.convert_to_filecontent(dataset=query_results)

        # Making up filename and creating the file (in a local path).
        now = datetime.now()
        dt_str = now.strftime(filename_date_format)
        publisher_id_as_output_prefix = publisher_id_parameter.replace(" ", "_").upper()
        curr_file_name = "{publisher_id}_report_{dt}.txt".format(publisher_id=publisher_id_as_output_prefix,
                                                                 dt=dt_str)
        local_path = full_output_path + dir_sep + curr_file_name
        with open(local_path, "w") as file:
            try:
                file.write(file_data)
            except Exception as e:
                print("Error on creating file: {0} ({1})".format(local_path, e))

        # Uploading the file to S3.
        uploaded = DataExporterJob.upload_to_aws_s3(local_file_full_path=local_path,
                                                    bucket=output_bucket_name,
                                                    s3_file=path_in_bucket + aws_s3_dir_sep + curr_file_name)
    else:  # No results.
        print("NO RECORDS FOUND!. QUERY: ")
        print(query)
    print("DONE!")
