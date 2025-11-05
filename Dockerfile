FROM python:3.12

WORKDIR /usr/src/app

RUN apt-get update && \
	apt-get install -y docker-compose pkg-config gcc python3-dev && \
	rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

RUN pip install --no-cache-dir pytest

COPY wait-for-it.sh /usr/src/app/wait-for-it.sh
RUN chmod +x /usr/src/app/wait-for-it.sh

COPY . .

RUN touch /etc/docker_but_login_required

CMD ["python3", "app.py"]
