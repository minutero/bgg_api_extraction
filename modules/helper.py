import os
import boto3
from botocore.exceptions import ClientError
import json
from config.config import path_to_json


def list_game_files():
    json_files = [
        pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith(".json")
    ]
    return json_files


def read_json_file(file_name):
    with open(os.path.join(path_to_json, file_name)) as json_file:
        json_text = json.load(json_file)
    return json_text


def get_secret(secret_name, region_name="sa-east-1"):
    if os.getenv("environment") == "local":
        if secret_name == "rds-postgresql-bgg":
            cred = {
                "dbInstanceIdentifier": os.getenv("db_name"),
                "host": os.getenv("db_host"),
                "username": os.getenv("db_user"),
                "password": os.getenv("db_pass"),
                "port": os.getenv("db_port"),
            }
        elif secret_name == "bgg-website-access":
            cred = {
                "bgg_user": os.getenv("bgg_user"),
                "bgg_pass": os.getenv("bgg_pass"),
            }
        else:
            cred = None
        return cred
    else:
        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)

        try:
            get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        except ClientError as e:
            # For a list of exceptions thrown, see
            # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
            raise e

        # Decrypts secret using the associated KMS key.
        return eval(get_secret_value_response["SecretString"])
