FROM python:3.11-slim

# Systemabhängigkeiten
RUN apt-get update && apt-get install -y apache2 libapache2-mod-wsgi-py3 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Arbeitsverzeichnis
WORKDIR /app

# Anforderungen
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App und Apache-Konfiguration
COPY . /app
COPY app.wsgi /var/www/app.wsgi
COPY apache-flask.conf /etc/apache2/sites-available/000-default.conf

# SQLite-Verzeichnis für persistente Speicherung
RUN mkdir -p /app/sqlite_data && chown -R www-data:www-data /app/sqlite_data

# Apache starten
CMD ["apachectl", "-D", "FOREGROUND"]
