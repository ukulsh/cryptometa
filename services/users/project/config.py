# services/users/project/config.py


import os


class BaseConfig:
    """Base configuration"""
    DEBUG = False  # new
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = 'my_precious'
    DEBUG_TB_ENABLED = False
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    BCRYPT_LOG_ROUNDS = 13  # new


class DevelopmentConfig(BaseConfig):
    """Development configuration"""
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    DEBUG_TB_ENABLED = True
    BCRYPT_LOG_ROUNDS = 4  # new


class TestingConfig(BaseConfig):
    """Testing configuration"""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_TEST_URL')
    BCRYPT_LOG_ROUNDS = 4  # new


class ProductionConfig(BaseConfig):
    """Production configuration"""
    DEBUG = False  # new
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')