# manage.py


import os
from dotenv import load_dotenv
import sys
import unittest

from flask.cli import FlaskGroup

from project import create_app, db       # new
from project.api.models import Products

dotenv_path = os.path.join(os.path.dirname(__file__), "qa.env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

app = create_app()
cli = FlaskGroup(create_app=create_app)

if __name__ == '__main__':
    cli()
