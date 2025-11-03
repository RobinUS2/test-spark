#!/bin/bash

# Database Setup Helper Script
# This script helps you switch between SQLite and PostgreSQL configurations

echo "🗄️  Database Configuration Helper"
echo "=================================="

# Function to setup PostgreSQL
setup_postgresql() {
    echo "Setting up PostgreSQL configuration..."
    
    # Check if PostgreSQL environment variables are set
    if [ -z "$DB_HOST" ]; then
        echo "WARNING: DB_HOST environment variable not set. Using localhost."
        export DB_HOST="localhost"
    fi
    
    if [ -z "$DB_NAME" ]; then
        echo "WARNING: DB_NAME environment variable not set. Using music_data."
        export DB_NAME="music_data"
    fi
    
    if [ -z "$DB_USER" ]; then
        echo "WARNING: DB_USER environment variable not set. Using postgres."
        export DB_USER="postgres"
    fi
    
    if [ -z "$DB_PASSWORD" ]; then
        echo "WARNING: DB_PASSWORD environment variable not set."
        echo "Please set it: export DB_PASSWORD='your_password'"
        exit 1
    fi
    
    echo "PostgreSQL configuration ready:"
    echo "   Host: $DB_HOST"
    echo "   Database: $DB_NAME" 
    echo "   User: $DB_USER"
    echo "   Password: [HIDDEN]"
    
    # Update the DB_CONFIG type in database.py
    sed -i "s/'type': 'sqlite'/'type': 'postgresql'/" database.py
    echo "Updated database.py to use PostgreSQL"
}

# Function to setup SQLite
setup_sqlite() {
    echo "Setting up SQLite configuration..."
    
    # Ensure data directory exists
    mkdir -p /app/data
    
    # Update the DB_CONFIG type in database.py  
    sed -i "s/'type': 'postgresql'/'type': 'sqlite'/" database.py
    echo "Updated database.py to use SQLite"
    echo "   Database file: /app/data/music_data.db"
}

# Main menu
case "$1" in
    "postgresql"|"postgres"|"pg")
        setup_postgresql
        ;;
    "sqlite"|"lite")
        setup_sqlite
        ;;
    *)
        echo "Usage: $0 [postgresql|sqlite]"
        echo ""
        echo "Commands:"
        echo "  postgresql  - Configure for PostgreSQL database"
        echo "  sqlite      - Configure for SQLite database (default)"
        echo ""
        echo "Example:"
        echo "  $0 postgresql"
        echo "  $0 sqlite"
        exit 1
        ;;
esac

echo "🎉 Database configuration complete!"