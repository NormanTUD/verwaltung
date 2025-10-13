@echo off
setlocal enabledelayedexpansion

REM ============================================================
REM Neo4j Docker Startup Script (Windows CMD Version)
REM ============================================================
REM Features:
REM  - Checks if Docker is installed and running
REM  - Checks if container exists
REM  - Creates it if not
REM  - Starts it otherwise
REM  - Full error handling and logging
REM ============================================================

set CONTAINER_NAME=neo4j-db
set IMAGE_NAME=neo4j:5.14
set PORT_HTTP=7474
set PORT_BOLT=7687
set VOLUME_NAME=neo4j_data
set AUTH=neo4j/testTEST12345678

echo.
echo ============================================================
echo [INFO] Neo4j Docker Startup Script
echo ============================================================
echo.

REM ------------------------------------------------------------
REM 1. Check if Docker command is available
REM ------------------------------------------------------------
echo [CHECK] Checking if Docker is installed...

where docker >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Docker CLI not found in PATH.
    echo [HINT] Please install Docker Desktop or add Docker to PATH.
    exit /b 1
)

REM ------------------------------------------------------------
REM 2. Check if Docker daemon is running
REM ------------------------------------------------------------
echo [CHECK] Checking if Docker daemon is running...
docker info >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Docker daemon is not running.
    echo [HINT] Start Docker Desktop and wait until it is fully initialized.
    exit /b 1
)

REM ------------------------------------------------------------
REM 3. Check if container exists
REM ------------------------------------------------------------
echo [CHECK] Checking if container "%CONTAINER_NAME%" exists...
set "EXISTING="
for /f "tokens=*" %%i in ('docker ps -a --filter "name=%CONTAINER_NAME%" --format "{{.Names}}"') do set EXISTING=%%i

if "%EXISTING%"=="" (
    echo [INFO] Container "%CONTAINER_NAME%" does not exist. Creating it now...
    docker run --name %CONTAINER_NAME% ^
        -p%PORT_HTTP%:7474 -p%PORT_BOLT%:7687 ^
        -e NEO4J_AUTH=%AUTH% ^
        -v %VOLUME_NAME%:/data ^
        %IMAGE_NAME%

    if errorlevel 1 (
        echo [ERROR] Failed to create Neo4j container.
        echo [HINT] Check your Docker logs or network configuration.
        exit /b 1
    )

    echo [SUCCESS] Neo4j container created successfully.
) else (
    echo [INFO] Container "%CONTAINER_NAME%" already exists.
    echo [ACTION] Attempting to start container...

    docker start %CONTAINER_NAME% >nul 2>nul
    if errorlevel 1 (
        echo [ERROR] Failed to start Neo4j container "%CONTAINER_NAME%".
        echo [HINT] Check if Docker is healthy or container is corrupted.
        exit /b 1
    )
    echo [SUCCESS] Container "%CONTAINER_NAME%" started successfully.
)

REM ------------------------------------------------------------
REM 4. Check if container is running
REM ------------------------------------------------------------
echo [CHECK] Verifying container is running...
docker ps --filter "name=%CONTAINER_NAME%" --filter "status=running" --format "{{.Names}}" | find "%CONTAINER_NAME%" >nul
if errorlevel 1 (
    echo [ERROR] Container "%CONTAINER_NAME%" is not running after startup.
    echo [HINT] Run "docker logs %CONTAINER_NAME%" for details.
    exit /b 1
)

REM ------------------------------------------------------------
REM 5. Success message
REM ------------------------------------------------------------
echo.
echo ============================================================
echo [SUCCESS] Neo4j is now running on:
echo   - HTTP:  http://localhost:%PORT_HTTP%
echo   - Bolt:  bolt://localhost:%PORT_BOLT%
echo   - Auth:  %AUTH%
echo ============================================================
echo.

endlocal
exit /b 0
