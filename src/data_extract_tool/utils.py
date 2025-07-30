"""
Configuration and database utilities for the data_extract_tool package.
Provides centralized configuration management and Teradata connection functionality.
"""

import os
import teradatasql
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

class Config:
    """Configuration manager for all environment variables."""
    
    def __init__(self):
        # Teradata configuration
        self.teradata = {
            "host": os.getenv("TERADATA_HOST"),
            "user": os.getenv("TERADATA_USER"),
            "password": os.getenv("TERADATA_PASSWORD"),
            "database": os.getenv("TERADATA_DATABASE"),
        }
        
        # OpenAI configuration
        self.openai = {
            "api_key": os.getenv("OPENAI_API_KEY"),
        }
    
    def get_teradata_config(self):
        """Get Teradata configuration."""
        return self.teradata.copy()
    
    def get_openai_config(self):
        """Get OpenAI configuration."""
        return self.openai.copy()
    
    def validate_teradata_config(self):
        """Validate Teradata configuration."""
        required_params = ["host", "user", "password", "database"]
        missing_params = []
        
        for param in required_params:
            if not self.teradata.get(param):
                missing_params.append(param)
        
        if missing_params:
            print(f"Missing required Teradata configuration parameters: {', '.join(missing_params)}")
            return False
        
        return True
    
    def validate_openai_config(self):
        """Validate OpenAI configuration."""
        if not self.openai.get("api_key"):
            print("Missing required OpenAI API key")
            return False
        
        return True
    
    def validate_all_config(self):
        """Validate all configuration."""
        return self.validate_teradata_config() and self.validate_openai_config()

# Global configuration instance
config = Config()

def get_config():
    """
    Get the global configuration instance.
    
    Returns:
        Config: Configuration instance
    """
    return config

def get_teradata_config():
    """
    Get Teradata configuration from environment variables.
    
    Returns:
        dict: Dictionary containing Teradata connection parameters
    """
    return config.get_teradata_config()

def get_openai_config():
    """
    Get OpenAI configuration from environment variables.
    
    Returns:
        dict: Dictionary containing OpenAI configuration parameters
    """
    return config.get_openai_config()

def connect_to_teradata(teradata_config=None):
    """
    Create a connection to Teradata using provided or default configuration.
    
    Args:
        teradata_config (dict, optional): Teradata configuration. If None, uses default config.
    
    Returns:
        teradatasql.Connection: Active Teradata connection
        
    Raises:
        Exception: If connection fails
    """
    if teradata_config is None:
        teradata_config = config.get_teradata_config()
    
    return teradatasql.connect(
        host=teradata_config["host"],
        user=teradata_config["user"],
        password=teradata_config["password"],
        database=teradata_config["database"]
    )

def test_connection(teradata_config=None):
    """
    Test the Teradata connection.
    
    Args:
        teradata_config (dict, optional): Teradata configuration. If None, uses default config.
    
    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        conn = connect_to_teradata(teradata_config)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        conn.close()
        return result[0] == 1
    except Exception as e:
        print(f"Connection test failed: {e}")
        return False

def validate_config():
    """
    Validate that all required configuration parameters are present.
    
    Returns:
        bool: True if all parameters are set, False otherwise
    """
    return config.validate_all_config()
