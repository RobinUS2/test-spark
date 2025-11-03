#!/bin/bash
# Script to fetch test data from the TechcaseRobinVerlangen repository

set -e  # Exit on any error

REPO_URL="git@github.com:Team-Rockstars-IT/TechcaseRobinVerlangen.git"
TEMP_DIR="temp_repo"
TARGET_FILE="testdata.txt"
SOURCE_FILE="rock-songs-raw-data.txt"

echo "🔄 Fetching test data from GitHub repository..."
echo "📂 Repository: ${REPO_URL}"
echo "📄 Source file: ${SOURCE_FILE}"
echo "🎯 Target file: ${TARGET_FILE}"

# Clean up any existing temp directory
if [ -d "$TEMP_DIR" ]; then
    echo "🧹 Cleaning up existing temp directory..."
    rm -rf "$TEMP_DIR"
fi

# Clone the repository to temporary directory
echo "📥 Cloning repository..."
git clone "$REPO_URL" "$TEMP_DIR"

# Check if the source file exists
if [ ! -f "$TEMP_DIR/$SOURCE_FILE" ]; then
    echo "❌ Error: Source file '$SOURCE_FILE' not found in repository"
    echo "📋 Available files in repository:"
    ls -la "$TEMP_DIR/"
    rm -rf "$TEMP_DIR"
    exit 1
fi

# Copy the file to current directory
echo "📋 Copying '$SOURCE_FILE' to '$TARGET_FILE'..."
cp "$TEMP_DIR/$SOURCE_FILE" "$TARGET_FILE"

# Clean up temp directory
echo "🧹 Cleaning up temporary files..."
rm -rf "$TEMP_DIR"

# Verify the file was copied successfully
if [ -f "$TARGET_FILE" ]; then
    FILE_SIZE=$(wc -l < "$TARGET_FILE")
    echo "✅ Successfully fetched test data!"
    echo "📊 File info:"
    echo "   • File: $TARGET_FILE"
    echo "   • Lines: $FILE_SIZE"
    echo "   • Size: $(du -h "$TARGET_FILE" | cut -f1)"
    
    # Show first few lines as preview
    echo "👀 Preview (first 5 lines):"
    head -5 "$TARGET_FILE" | sed 's/^/   /'
else
    echo "❌ Error: Failed to copy test data file"
    exit 1
fi

echo "🎉 Test data fetch completed successfully!"