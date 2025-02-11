#!/bin/bash

# Check if .env file exists
if [ -f .env ]; then
    read -p ".env file already exists. Do you want to overwrite it? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Copy example environment file
cp .env.example .env

echo "Environment file created. Please edit .env with your configuration values."
echo "The following values need to be set:"
echo "1. ETL_DB_CONNECTION_STRING - Your database connection string"
echo "2. ETL_TEAMS_WEBHOOK_URL - Your Microsoft Teams webhook URL"

# Make the file readable only by the owner
chmod 600 .env

echo "File permissions set to be secure (readable only by owner)"