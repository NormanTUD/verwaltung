# Nutze ein offizielles Python-Laufzeit-Image
FROM python:3.9-slim

# Setze das Arbeitsverzeichnis im Container
WORKDIR /app

# Kopiere die Abhängigkeitsdateien
COPY requirements.txt .

# Installiere die benötigten Python-Pakete
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den Rest der Anwendung
COPY . .

# Befehl, um den FastAPI-Server mit uvicorn zu starten
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
