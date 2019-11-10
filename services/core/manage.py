# manage.py


import sys
import unittest

from flask.cli import FlaskGroup

from project import create_app, db       # new
from project.api.models import Products

app = create_app()
cli = FlaskGroup(create_app=create_app)


@cli.command()
def test():
    """Runs the tests without code coverage"""
    tests = unittest.TestLoader().discover('project/tests', pattern='test*.py')
    result = unittest.TextTestRunner(verbosity=2).run(tests)
    if result.wasSuccessful():
        return 0
    sys.exit(result)


@cli.command('recreate_db')
def recreate_db():
    db.drop_all()
    db.create_all()
    db.session.commit()


@cli.command('seed_db')
def seed_db():
    """Seeds the database."""
    db.session.add(Products(
        name=('Dew Of The Sea Hairoil - 100ml'),
        sku='18729769697378'
    ))
    db.session.add(Products(
        name=('Lin Dior Hairoil - 100ml'),
        sku='20439188308066'
    ))
    db.session.add(Products(
        name=('Combination Kit 2'),
        sku='18377561276514'
    ))
    db.session.commit()


if __name__ == '__main__':
    cli()
