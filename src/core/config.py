import os
from typing import Dict, Any
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

def get_env_bool(key: str, default: bool = False) -> bool:
    """Convert environment variable to boolean"""
    value = os.getenv(key, str(default)).lower()
    return value in ('true', '1', 'yes', 'on')

class Settings:
    """Application settings loaded from environment variables"""
    
    # ClickHouse settings
    CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '9440'))
    CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', '')
    CLICKHOUSE_SECURE: bool = get_env_bool('CLICKHOUSE_SECURE', True)
    CLICKHOUSE_DATABASE: str = os.getenv('CLICKHOUSE_DATABASE', 'default')
    
    # API settings
    API_HOST: str = os.getenv('API_HOST', '0.0.0.0')
    API_PORT: int = int(os.getenv('API_PORT', '8001'))
    
    @property
    def clickhouse_settings(self) -> Dict[str, Any]:
        """Get ClickHouse connection settings"""
        return {
            'host': self.CLICKHOUSE_HOST,
            'port': self.CLICKHOUSE_PORT,
            'user': self.CLICKHOUSE_USER,
            'password': self.CLICKHOUSE_PASSWORD,
            'secure': self.CLICKHOUSE_SECURE,
            'database': self.CLICKHOUSE_DATABASE,
            'settings': {
                'max_execution_time': 60
            }
        }
    
    @property
    def api_settings(self) -> Dict[str, Any]:
        """Get API server settings"""
        return {
            'host': self.API_HOST,
            'port': self.API_PORT
        }

# Create global settings instance
settings = Settings() 