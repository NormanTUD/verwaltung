#!/usr/bin/env bash

set -e

### === Defaults ===
DB_MODE="sqlite"
PORT="8080"
SECRET_KEY="$(openssl rand -hex 16)"
SQLITE_PATH="./sqlite_data/db.sqlite"
COMPOSE_FILE="docker-compose.yml"
USE_SUDO=""

### === Parse arguments ===
for arg in "$@"; do
	case $arg in
		--db=*)
			DB_MODE="${arg#*=}"
			shift
			;;
		--port=*)
			PORT="${arg#*=}"
			shift
			;;
		--secret=*)
			SECRET_KEY="${arg#*=}"
			shift
			;;
		--sqlite-path=*)
			SQLITE_PATH="${arg#*=}"
			shift
			;;
		--help)
			echo "Usage: ./docker.sh [--db=sqlite|mariadb] [--port=8080] [--secret=KEY] [--sqlite-path=PATH]"
			exit 0
			;;
	esac
done

### === Check Docker ===
if ! command -v docker &> /dev/null; then
	echo "❌ Docker is not installed. Please install Docker first."
	exit 1
fi

if ! docker info &> /dev/null; then
	echo "❌ Docker daemon is not running or requires sudo."
	if sudo -n true 2>/dev/null; then
		USE_SUDO="sudo"
		echo "ℹ️ Using sudo for Docker commands."
	else
		echo "❌ Cannot run Docker without sudo and no sudo permissions detected."
		exit 1
	fi
fi

### === Check Docker Compose ===
if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
	echo "❌ Docker Compose is not installed."
	exit 1
fi

### === Generate .env (optional) ===
if [ ! -f .env ]; then
	echo "DB_MODE=$DB_MODE" > .env
	echo "PORT=$PORT" >> .env
	echo "SECRET_KEY=$SECRET_KEY" >> .env
	echo "SQLITE_PATH=$SQLITE_PATH" >> .env
	echo "✅ .env file created."
else
	echo "ℹ️ .env already exists, keeping existing values."
fi

### === Generate docker-compose.yml if missing ===
if [ ! -f "$COMPOSE_FILE" ]; then
	cat > "$COMPOSE_FILE" <<EOF
version: '3.9'

services:
  web:
    build: .
    ports:
      - "\${PORT:-8080}:80"
    volumes:
      - ./sqlite_data:/app/sqlite_data
    environment:
      - DB_MODE=\${DB_MODE}
      - SQLITE_PATH=\${SQLITE_PATH}
      - SECRET_KEY=\${SECRET_KEY}
    depends_on:
      - db

  db:
    image: mariadb:11
    restart: always
    environment:
      MARIADB_ROOT_PASSWORD: rootpass
      MARIADB_DATABASE: myapp
      MARIADB_USER: myuser
      MARIADB_PASSWORD: mypass
    volumes:
      - db_data:/var/lib/mysql

volumes:
  db_data:
  sqlite_data:
EOF
echo "✅ docker-compose.yml generated."
else
	echo "ℹ️ docker-compose.yml already exists, not overwriting."
fi

### === Start Docker Compose ===
echo "🚀 Starting containers with Docker Compose..."
if command -v docker-compose &> /dev/null; then
	$USE_SUDO docker-compose up --build
else
	$USE_SUDO docker compose up --build
fi
