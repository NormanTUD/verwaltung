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

if ! command -v wget &>/dev/null; then
	# Update package lists
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

if ! command -v git &>/dev/null; then
	# Update package lists
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

docker_compose build || {
	echo "Failed to build container"
	exit 254
}

docker_compose up -d || {
	echo "Failed to build container"
	exit 255
}
