"""
Configuration settings for Telegradd Web Application
"""
import os
from datetime import timedelta

class Config:
    """Base configuration class"""
    
    # Flask settings
    SECRET_KEY = os.environ.get('SECRET_KEY') or 'telegradd-secret-key-change-in-production'
    
    # Security settings
    WTF_CSRF_ENABLED = True
    WTF_CSRF_TIME_LIMIT = 3600  # 1 hour
    
    # Session settings
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    SESSION_COOKIE_SECURE = False  # Set to True in production with HTTPS
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    
    # Rate limiting settings
    RATELIMIT_STORAGE_URL = "memory://"
    RATELIMIT_DEFAULT = "100 per hour"
    
    # Admin credentials (change in production)
    ADMIN_USERNAME = os.environ.get('ADMIN_USERNAME') or 'admin'
    ADMIN_PASSWORD = os.environ.get('ADMIN_PASSWORD') or '016997791aA!!'
    
    # Application settings
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file upload
    
    # Telegram settings
    TELEGRAM_SESSION_PATH = os.path.join(os.path.dirname(__file__), 'sessions')
    TELEGRAM_ACCOUNTS_FILE = os.path.join(os.path.dirname(__file__), 'accounts.txt')
    
    # Background task settings
    TASK_TIMEOUT = 3600  # 1 hour
    MAX_CONCURRENT_TASKS = 5
    
    # Logging settings
    LOG_LEVEL = os.environ.get('LOG_LEVEL') or 'INFO'
    LOG_FILE = os.path.join(os.path.dirname(__file__), 'logs', 'app.log')
    
    # Database settings (if needed in future)
    DATABASE_URL = os.environ.get('DATABASE_URL') or 'sqlite:///telegradd.db'
    
    @staticmethod
    def init_app(app):
        """Initialize application with configuration"""
        # Create necessary directories
        os.makedirs(Config.TELEGRAM_SESSION_PATH, exist_ok=True)
        os.makedirs(os.path.dirname(Config.LOG_FILE), exist_ok=True)

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    TESTING = False
    
class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    TESTING = False
    SESSION_COOKIE_SECURE = True
    WTF_CSRF_SSL_STRICT = True
    
    # Use Redis for rate limiting in production
    RATELIMIT_STORAGE_URL = os.environ.get('REDIS_URL') or "redis://localhost:6379"

class TestingConfig(Config):
    """Testing configuration"""
    TESTING = True
    WTF_CSRF_ENABLED = False
    
# Configuration dictionary
config = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'testing': TestingConfig,
    'default': DevelopmentConfig
}