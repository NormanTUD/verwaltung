#!/bin/bash

# Default values
run_tests=0

# Help message
help_message() {
	echo "Usage: docker.sh [OPTIONS]"
	echo "Options:"
	echo "  --help             Show this help message"
}

_sudo() {
	if command -v sudo >/dev/null 2>&1; then
		sudo "$@"
	else
		"$@"
	fi
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
	case $1 in
		--help)
			help_message
			exit 0
			;;
		*)
			echo "Error: Unknown option '$1'. Use --help for usage."
			exit 1
			;;
	esac
	shift
done

is_package_installed() {
	dpkg-query -W -f='${Status}' "$1" 2>/dev/null | grep -c "ok installed"
}

UPDATED_PACKAGES=0

# Check if Docker is installed
if ! command -v docker &>/dev/null; then
	echo "Docker not found. Installing Docker..."
	# Enable non-free repository
	if [[ -e /etc/apt/sources.list ]]; then
		sed -i 's/main$/main contrib non-free/g' /etc/apt/sources.list
	fi

	# Update package lists
	if [[ $UPDATED_PACKAGES == 0 ]]; then
		_sudo apt update || {
			echo "apt-get update failed. Are you online?"
		}
	UPDATED_PACKAGES=1
	fi

	# Install Docker
	_sudo apt install -y docker.io docker-compose || {
		echo "sudo apt install -y docker.io failed"
	}
fi

# Check if wget is installed
if ! command -v wget &>/dev/null; then
	if [[ $UPDATED_PACKAGES == 0 ]]; then
		_sudo apt update || {
			echo "apt-get update failed. Are you online?"
		}
	UPDATED_PACKAGES=1
	fi
	_sudo apt-get install -y wget || {
		echo "sudo apt install -y wget failed"
	}
fi

# Check if git is installed
if ! command -v git &>/dev/null; then
	if [[ $UPDATED_PACKAGES == 0 ]]; then
		_sudo apt update || {
			echo "apt-get update failed. Are you online?"
		}
	UPDATED_PACKAGES=1
	fi
	_sudo apt-get install -y git || {
		echo "sudo apt install -y git failed"
	}
fi

# Wrapper function for docker compose
function docker_compose {
	# check if user is in docker group
	if [[ -n $USER ]]; then
		if id -nG "$USER" | grep -qw docker; then
			prefix=""
		else
			prefix="_sudo"
		fi
	else
		prefix="sudo"
	fi

	if ! command -v sudo 2>/dev/null >/dev/null; then
		prefix=""
	fi

	if command -v docker-compose >/dev/null 2>&1; then
		$prefix docker-compose "$@"
	else
		$prefix docker compose "$@"
	fi
}

# Build Docker images
docker_compose build || {
	echo "Failed to build container"
	exit 254
}

# Handle conflicting existing containers cleanly
EXISTING=$(docker ps -a --filter "name=^neo4j-db$" --format "{{.ID}}")

if [[ -n "$EXISTING" ]]; then
	echo "Container 'neo4j-db' already exists. Checking state..."

	# Check if container is part of current compose project
	if ! docker inspect "$EXISTING" --format '{{ index .Config.Labels "com.docker.compose.project" }}' | grep -q 'verwaltung'; then
		echo "Old or external container detected. Removing stale container..."
		docker rm -f neo4j-db >/dev/null 2>&1 || {
			echo "Failed to remove old container neo4j-db"
					exit 253
				}
		else
			echo "Container belongs to this compose project. Reusing it."
	fi
fi

echo "Starting containers (reusing existing ones if present)..."
docker_compose up -d --no-recreate --remove-orphans || {
	echo "Failed to start or reuse existing containers"
	exit 255
}

echo "All containers are up and running."
