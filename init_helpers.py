import argparse

def parsing():
    parser = argparse.ArgumentParser(description="Starte die Flask-App mit konfigurierbaren Optionen.")
    parser.add_argument('--debug', action='store_true', help='Aktiviere den Debug-Modus')
    parser.add_argument('--disable_login', action='store_true', help='Deaktivier den Login')
    parser.add_argument('--port', type=int, default=5000, help='Port für die App (Standard: 5000)')
    parser.add_argument('--engine-db', type=str, default='sqlite:///instance/database.db', help='URI für create_engine()')
    return parser.parse_args()
