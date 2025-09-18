# Basis-Image
FROM python:3.12-slim

# Arbeitsverzeichnis setzen
WORKDIR /usr/src/app

# Anforderungen kopieren und installieren
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# pytest für Tests installieren
RUN pip install --no-cache-dir pytest

COPY wait-for-it.sh /usr/src/app/wait-for-it.sh
RUN chmod +x /usr/src/app/wait-for-it.sh

# Quellcode kopieren
COPY . .

# Standard-Befehl (kann beim Start überschrieben werden)
CMD ["python3", "app.py"]
