FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    bash \
    && rm -rf /var/lib/apt/lists/*

COPY ./src/ /app/src/
COPY requirements.txt /app/requirements.txt
COPY ./confg/ /app/config/

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

WORKDIR /app

ENV ETL_SLEEP_TIME 180

CMD ["python", "-u", "src/etl_notifier/main.py"]