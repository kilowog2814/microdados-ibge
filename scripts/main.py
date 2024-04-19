import zipfile
from dotenv import dotenv_values
import requests
from io import BytesIO
from loguru import logger
import boto3
from botocore.exceptions import ClientError
import json
import asyncio

config = dotenv_values(".env")

with open("links.json", mode="r") as links_json:
    links_list = json.load(links_json)
    links_json.close()


def download_file(url: str) -> bytes:
    try:
        r = requests.get(url, timeout=100, stream=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.RequestException as e:
        logger.exception(f"Erro ao baixar o arquivo: {e}")
        raise


def extract_zip(input_zip) -> list:

    try:

        files_unzip = []

        files = zipfile.ZipFile(BytesIO(input_zip))

        for name in files.namelist():

            if ".txt" in name:
                file_content = files.read(name)

                return_context = {"fileName": name, "content": file_content}

                files_unzip.append(return_context)

        return files_unzip

    except zipfile.BadZipFile as e:

        logger.exception(f"Erro ao extrair arquivo zip: {e}")
        raise


async def s3_upload(unzip_file, link_download: str) -> None:

    try:
        s3 = boto3.client("s3")

        file_name = unzip_file["fileName"].replace("SP1/", "")

        # name_unzip_file = f'data/{unzip_file[0].replace("SP1/", "")}'

        logger.info(f"subindo arquivo {file_name} no bucket: {config['BUCKET_NAME']}")

        s3_path = config["STAGE_2000"]

        if "Censo_Demografico_2010" in link_download:
            s3_path = config["STAGE_2010"]

        s3.upload_fileobj(
            BytesIO(unzip_file["content"]),
            config["BUCKET_NAME"],
            f"{s3_path}/{file_name}",
        )

        logger.success(f"upload  do arquivo {file_name} finalizado....")
    except ClientError as e:
        logger.exception(f"Erro ao subir arquivo  {file_name} no s3, erro: {e}")

    except Exception as er:
        logger.exception(
            f"Erro gerenerico, arquivo: {file_name}, mensagem de erro: {er}"
        )


async def parallel_s3_uploads(unzip_files, link_download: str) -> None:
    coros = []
    for unzip_file in unzip_files:

        coros.append(s3_upload(unzip_file, link_download))

    await asyncio.gather(*coros)


if __name__ == "__main__":

    for link in links_list["links"]:

        logger.info(f"baixando bases do link: {link}")

        zip_file = download_file(link)

        logger.success("download finalizado")

        logger.info("iniciando processo de descompactacao e upload dos arquivos")

        event_loop = asyncio.new_event_loop()

        unzip_file = extract_zip(zip_file)

        event_loop.run_until_complete(parallel_s3_uploads(unzip_file, link))
