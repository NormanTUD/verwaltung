from flask import Flask, render_template, send_from_directory
import os

app = Flask(__name__)

API_URL = os.environ.get('API_URL', 'http://localhost:8000')

@app.route('/')
def index():
    return render_template('index.html', api_url=API_URL)

@app.route('/static/css/<path:path>')
def send_css(path):
    return send_from_directory('static/css', path)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
