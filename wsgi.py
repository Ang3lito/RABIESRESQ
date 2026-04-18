"""WSGI entry point for production hosts (e.g. PythonAnywhere)."""
from app import create_app

application = create_app()
