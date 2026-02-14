import os
import sqlite3

from flask import current_app, g


def get_db():
    if "db" not in g:
        db_path = current_app.config.get("DATABASE")
        if not db_path:
            os.makedirs(current_app.instance_path, exist_ok=True)
            db_path = os.path.join(current_app.instance_path, "rabiesresq.sqlite")

        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        g.db = conn
    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

def init_app(app):
    app.teardown_appcontext(close_db)

