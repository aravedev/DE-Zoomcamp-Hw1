FROM python:3.9

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY pipeline.py pipeline.py

ENTRYPOINT [ "python","pipeline.py" ]