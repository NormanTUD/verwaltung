FROM node:18 AS build-stage
WORKDIR /app
COPY package.json ./
RUN npm install
COPY tailwind.config.js ./
COPY static/css/tailwind.css ./
RUN npx tailwindcss -i ./static/css/tailwind.css -o ./static/css/styles.css --minify

FROM python:3.9-slim
WORKDIR /app
COPY --from=build-stage /app/static/css/styles.css ./static/css/tailwind.css
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "app.py"]
