import sys
import csv
import io

from flask import Flask, request, jsonify, render_template, session

load_dotenv()
app = Flask(__name__)

if __name__ == '__main__':
    try:
        app.run(debug=True)
    except (KeyboardInterrupt, OSError):
        print("You pressed CTRL-C")
        sys.exit(0)
