FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    bash \
    && rm -rf /var/lib/apt/lists/*


COPY ./src /app/src
COPY ./config /app/config 
COPY ./scripts /app/scripts

WORKDIR /app

RUN chmod +x scripts/start.sh

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

ENTRYPOINT ["sh", "scripts/start.sh"]