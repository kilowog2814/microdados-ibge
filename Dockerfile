FROM public.ecr.aws/lambda/python:3.11

COPY requirements.txt ./scripts/micro-dados-2000.py scripts/micro-dados-2010.py .env ./

RUN pip install -r requirements.txt