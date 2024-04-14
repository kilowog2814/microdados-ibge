import zipfile
import pandas as pd
from dotenv import dotenv_values
import requests
from io import BytesIO
import logging

config = dotenv_values(".env")


def download_file():
    try:
        r = requests.get(config["URL_DOWNLOAD_2010"], timeout=100, stream=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.RequestException as e:
        logging.error(f"Erro ao baixar o arquivo: {e}")
        return None


def unzip_file(zip_data):
    try:
        with zipfile.ZipFile(BytesIO(zip_data)) as zip_file:
            for zipinfo in zip_file.infolist():
                with zip_file.open(zipinfo) as thefile:
                    yield zipinfo.filename, thefile

    except zipfile.BadZipFile as e:
        logging.error(f"Erro ao extrair arquivo zip: {e}")
        return None


if __name__ == "__main__":

    unzip = unzip_file(download_file())

    for un in unzip:
        if ".txt" in un[0]:
            print(type(un[1]))
