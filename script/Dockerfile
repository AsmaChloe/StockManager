FROM python:3.12

COPY data ./data

WORKDIR /app

COPY script/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY script/ingest_file.py ./

COPY script/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

CMD ["sh", "-c", "wait-for-it.sh postgres:5432 -- python ingest_file.py"]

