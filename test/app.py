from flask import Flask, render_template
import threading
import uvicorn
from api import app as api_app

flask_app = Flask(__name__)

@flask_app.route("/")
def index():
    return render_template("index.html")

def start_api():
    uvicorn.run(api_app, host="0.0.0.0", port=8000)

if __name__ == "__main__":
    threading.Thread(target=start_api).start()
    flask_app.run(debug=True, port=5000)
