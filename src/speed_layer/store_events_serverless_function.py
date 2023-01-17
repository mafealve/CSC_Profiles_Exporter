from __future__ import print_function

import base64
import json
import boto3

from datetime import datetime
from boto3.dynamodb.conditions import Key

# Global parameters.
bucket_name = "cs-datalake-dev-test"
base_prefix = "observations/"
dynamodb_table_name = "cs-placements-attributes-dynamodbtable"

s3_client = boto3.client('s3')
dynamo_db = boto3.resource('dynamodb')
filename_date_format = "%Y%m%d_%H%M%S"

print("Loading function...")


def lambda_handler(event, context):
    table = dynamo_db.Table(dynamodb_table_name)  # DynamoDB table name
    results_dict = dict()

    # Event enrichment (look up the publisher)
    for record in event["Records"]:
        payload = base64.b64decode(record["kinesis"]["data"])
        payload = payload.decode("UTF-8")
        payload_dict = json.loads(payload)
        version="0"
        placement_id = payload_dict["placement_id"]

        response = table.query(
            KeyConditionExpression=Key("placement_id").eq(placement_id) & Key("version").eq(version)
        )
        items_count = response["Count"]
        items = response["Items"]

        publisher_id = ""
        if items_count > 0:
            for item in items:
                publisher_id = item["publisher_id"]
                break
        print("Publisher: ", publisher_id)
        print("Decoded payload: ", payload)

        if publisher_id in results_dict.keys():  # Existent key (publisher_id), append.
            temp_list = list()
            temp_list = results_dict[publisher_id][:]
            temp_list.append(payload)
            temp_list = list(set(temp_list))  # Drop duplicates
            results_dict[publisher_id] = temp_list
        else:  # Non-Existent key (publisher_id), create.
            results_dict[publisher_id] = [payload]

    number_of_publishers = len(results_dict.keys())

    # A single File (with 1 to many observations) for each publisher.
    for publisher_id, observations_list in results_dict.items():

        # Making up filename
        now = datetime.now()
        yyyy = str(now.year).zfill(4)
        mm = str(now.month).zfill(2)
        dd = str(now.day).zfill(2)
        hh24 = str(now.hour).zfill(2)
        mi = str(now.minute).zfill(2)
        file_name = "observations_" + now.strftime(filename_date_format) + ".json"
        lambda_path = "/tmp/" + file_name

        # Partitioned location for the file.
        s3_path = base_prefix + \
                  f"publisher_id={publisher_id}/year={yyyy}/month={mm}/day={dd}/hour={hh24}/minute={mi}/{file_name}"
        file_content = "[" + ",".join(observations_list) + "]"  # JSON array of objects.

        # Writing file to a temporary local path
        with open(lambda_path, 'w') as f:
            f.write(file_content)

        s3 = boto3.resource("s3")
        s3.meta.client.upload_file(lambda_path, bucket_name, s3_path)

    return {
        "statusCode": 200,
        "body": json.dumps(f"{number_of_publishers} file(s) was(were) created.")
    }

