# Database Configuration
# Easily switch between SQLite and PostgreSQL

# Current configuration: SQLite (for development)
DATABASE_CONFIG = {
    "default": {
        "engine": "sqlite",
        "name": "/app/data/music_data.db",
        "options": {
            "echo": False,  # Set to True for SQL query logging
            "pool_pre_ping": True
        }
    },
    
    # PostgreSQL configuration (for production)
    "postgresql": {
        "engine": "postgresql",
        "host": "localhost",
        "port": 5432,
        "name": "music_data",
        "user": "postgres", 
        "password": "",
        "options": {
            "echo": False,
            "pool_size": 10,
            "max_overflow": 20,
            "pool_pre_ping": True
        }
    }
}

# Environment variable mappings for PostgreSQL
POSTGRES_ENV_MAPPING = {
    "host": "DB_HOST",
    "port": "DB_PORT", 
    "name": "DB_NAME",
    "user": "DB_USER",
    "password": "DB_PASSWORD"
}

# Database table configurations
TABLE_CONFIGS = {
    "music_records": {
        "indexes": ["raw_artist", "callsign", "time"],
        "constraints": []
    },
    "artist_images": {
        "indexes": ["artist_name"],
        "constraints": ["UNIQUE(artist_name)"]
    }
}

# Migration settings (for future use)
MIGRATION_SETTINGS = {
    "auto_migrate": True,
    "backup_before_migrate": True,
    "migration_timeout": 300  # 5 minutes
}