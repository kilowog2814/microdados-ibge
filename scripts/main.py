import zipfile
from dotenv import dotenv_values
import requests
from io import BytesIO
from loguru import logger
import boto3
from botocore.exceptions import ClientError
import json
import asyncio
import os

config = dotenv_values(".env")

with open("links.json", mode="r") as links_json:
    links_list = json.load(links_json)
    links_json.close()


def download_file(url: str) -> bytes:
    """Download dos arquivos pelos links"""

    try:
        r = requests.get(url, timeout=100, stream=True)
        r.raise_for_status()

        return r.content
    except requests.exceptions.RequestException as e:
        logger.exception(f"Erro ao baixar o arquivo: {e}")
        raise


def extract_zip(zipFile):
    """Extração dos arquivos compactados para a pasta do ./data depois do download"""

    try:

        extract_path = "./data/"

        with zipfile.ZipFile(BytesIO(zipFile)) as zip_ref:

            for member in zip_ref.namelist():
                if ".txt" in member:
                    filename = os.path.basename(member)
                    if filename:

                        destination = os.path.join(extract_path, filename)
                        zip_ref.extract(member, extract_path)

                        os.rename(os.path.join(extract_path, member), destination)

    except zipfile.BadZipFile as e:

        logger.exception(f"Erro ao extrair arquivo zip: {e}")
        raise


async def s3_upload(unzip_file, link_download: str) -> None:
    """Sobe os arquivos no S3"""

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
    """Cria as corotinas para subir os arquivos no s3"""
    coros = []
    for unzip_file in unzip_files:

        coros.append(s3_upload(unzip_file, link_download))

    await asyncio.gather(*coros)


def lambda_handler(event, context):
    """Handler da AWS Lambda"""

    try:
        for link in links_list["links"]:

            logger.info(f"baixando bases do link: {link}")

            zip_file = download_file(link)

            logger.success("download finalizado")

            logger.info("iniciando processo de descompactacao e upload dos arquivos")

            extract_zip(zip_file)

            return {"statusCode": 200, "body": json.dumps({"message": "processo ok"})}

    except Exception as e:
        return {"statusCode": 404, "Erro": e, "context": context, "event": event}

        """
        event_loop = asyncio.new_event_loop()

        unzip_file = extract_zip(zip_file)

        event_loop.run_until_complete(parallel_s3_uploads(unzip_file, link))
        """
