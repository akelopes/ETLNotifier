import logging
import os
import struct

import pyodbc
from azure.identity import DefaultAzureCredential

from .database import DatabaseSource

logger = logging.getLogger(__name__)

SQL_COPT_SS_ACCESS_TOKEN = 1256


class AzureSqlDBSource(DatabaseSource):
    def __init__(self, connection_string: str, msi_client_id: str):
        self.connection_string = connection_string
        self.client_id = msi_client_id
        self.connection = None
        self.cursor = None
        self.connect()

    def connect(self):
        if self.connection:
            return
        logger.info("Building Azure SQL connection...")
        os.environ["AZURE_CLIENT_ID"] = self.client_id
        credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)
        self.connection = pyodbc.connect(
            self.connection_string,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        )
        self.cursor = self.connection.cursor()
        logger.info("Connected to Azure SQL Server.")
