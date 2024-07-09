import zipfile
import requests
from io import BytesIO
from loguru import logger
import boto3
from botocore.exceptions import ClientError
import json
import asyncio

BUCKET_NAME = "micro-dados-sp-outros"

S3_STAGE_PATH = "stage/2000"

ZIP_FILE_LINK = (
    "https://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados/SP.zip"
)


def download_file(url: str) -> bytes:
    """Download dos arquivos pelos links"""

    try:
        r = requests.get(url, timeout=100, stream=True)
        r.raise_for_status()
        return r.content
    except requests.exceptions.RequestException as e:
        logger.exception(f"Erro ao baixar o arquivo: {e}")
        raise


def extract_zip(input_zip) -> list:
    """Extração dos arquivos compactados depois do download"""

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


async def s3_upload(unzip_file) -> None:
    """Sobe os arquivos no S3"""

    try:
        s3 = boto3.client("s3")

        file_name = unzip_file["fileName"].replace("SP1/", "")

        logger.info(f"subindo arquivo {file_name} no bucket: {BUCKET_NAME}")

        s3.upload_fileobj(
            BytesIO(unzip_file["content"]),
            BUCKET_NAME,
            f"{S3_STAGE_PATH}/{file_name}",
        )

        logger.success(f"upload  do arquivo {file_name} finalizado....")

        unzip_file = None

    except ClientError as e:
        logger.exception(f"Erro ao subir arquivo  {file_name} no s3, erro: {e}")

    except Exception as er:
        logger.exception(
            f"Erro gerenerico, arquivo: {file_name}, mensagem de erro: {er}"
        )


async def parallel_s3_uploads(unzip_files) -> None:
    """Cria as corotinas para subir os arquivos no s3"""

    coros = []
    for unzip_file in unzip_files:

        coros.append(s3_upload(unzip_file))

    await asyncio.gather(*coros)


def lambda_handler(event, context):
    """Handler da AWS Lambda"""

    try:

        logger.info(f"baixando bases do link: {ZIP_FILE_LINK}")

        zip_file = download_file(ZIP_FILE_LINK)

        logger.success("download finalizado")

        logger.info("iniciando processo de descompactacao e upload dos arquivos")

        event_loop = asyncio.get_event_loop()

        unzip_file = extract_zip(zip_file)

        zip_file = None

        event_loop.run_until_complete(parallel_s3_uploads(unzip_file))

        unzip_file = None

        return {"statusCode": 200, "body": json.dump({"message": "processo ok"})}
    except Exception as excep:
        return {
            "status": 404,
            "erro": json.dumps({"message": str(excep)}),
            "context": json.dumps({"context": context}),
        }
