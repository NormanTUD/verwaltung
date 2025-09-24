import secrets

def load_or_generate_secret_key():
    path = "/etc/oasis/secret_key"
    try:
        # Datei existiert
        with open(path, "r") as f:
            key = f.read().strip()
            if not key:
                raise ValueError("Secret-Key-Datei ist leer")
            print(f"Secret-Key geladen aus {path}")
            return key
    except FileNotFoundError:
        # Datei existiert nicht
        key = secrets.token_urlsafe(64)
        return key
    except Exception as e:
        # Andere Fehler beim Lesen
        key = secrets.token_urlsafe(64)
        print(f"Fehler beim Laden des Secret-Keys ({e}). Temporärer Key wird verwendet: {key}")
        return key
