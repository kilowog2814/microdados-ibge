import zipfile
from dotenv import dotenv_values
import requests
from io import BytesIO
import logging
from boto3 import s3
import json

config = dotenv_values(".env")

with open("links.json", mode="r") as links_json:
    links = links_json.read()
    links_json.close()


def download_file(url: str):
    try:
        r = requests.get(url, timeout=100, stream=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao baixar o arquivo: {e}")
        return None


def unzip_file(zip_data):
    try:
        with zipfile.ZipFile(BytesIO(zip_data)) as zip_file:
            # zip_file.extractall("./data")

            for zipinfo in zip_file.infolist():
                with zip_file.open(zipinfo) as thefile:
                    yield zipinfo.filename, thefile

    except zipfile.BadZipFile as e:

        logging.error(f"Erro ao extrair arquivo zip: {e}")
        return None


def s3_upload():
    pass


if __name__ == "__main__":

    print(links["links"])

    unzip = unzip_file(config["links"][0])

    for un in unzip:

        if ".txt" in un[0]:
            name_unzip_file = f'data/{un[0].replace("SP1/", "")}'
            print(name_unzip_file)
            with open(name_unzip_file, encoding="UTF-8", mode="w") as file_unzip:
                file_unzip.write(un[1].read().decode("UTF-8"))
                file_unzip.close()
