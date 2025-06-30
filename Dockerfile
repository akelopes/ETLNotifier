FROM python:3.10-slim-bullseye

RUN apt-get update && apt-get install -y \
    bash \
    curl \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

# sql server drivers and bcp
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    ACCEPT_EULA=Y apt-get install -y mssql-tools18 && \
    echo 'export PATH="$PATH:/opt/mssql-tools18/bin"' >> ~/.bashrc && \
    apt-get install -y unixodbc-dev && \
    apt-get install -y libgssapi-krb5-2

COPY ./src/ /app/src/
COPY requirements.txt /app/requirements.txt
COPY ./config/ /app/config/

WORKDIR /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

ENV ETL_SLEEP_TIME 180

ENV PYTHONPATH="/app/src"

CMD ["python", "-u", "src/etl_notifier/main.py"]