FROM python:3.9.6-slim
WORKDIR /code
COPY requirements.txt /code
RUN pip install -r /code/requirements.txt --no-cache-dir
COPY . /code
CMD python /code/main.py