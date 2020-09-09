# services/users/project/config.py


import os


class BaseConfig:
    """Base configuration"""
    DEBUG = False
    TESTING = False
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = os.environ.get('SECRET_KEY')
    DEBUG_TB_ENABLED = False
    DEBUG_TB_INTERCEPT_REDIRECTS = False
    BCRYPT_LOG_ROUNDS = 13
    TOKEN_EXPIRATION_DAYS = 30    # new
    TOKEN_EXPIRATION_SECONDS = 0  # new


class StagingConfig(BaseConfig):
    """Testing configuration"""
    TESTING = True
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')
    BCRYPT_LOG_ROUNDS = 4
    TOKEN_EXPIRATION_DAYS = 30     # new
    TOKEN_EXPIRATION_SECONDS = 3  # new


class ProductionConfig(BaseConfig):
    """Production configuration"""
    DEBUG = False
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL')