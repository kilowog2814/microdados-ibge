FROM public.ecr.aws/lambda/python:3.11

# Copy requirements.txt
COPY  ./scripts/main.py requirements.txt links.json ./data ./

# Install the specified packages
RUN pip install -r requirements.txt

CMD [ "main.lambda_handler"]