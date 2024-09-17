"""
This script is used to backup the DuckDB file to a specified folder.
"""

import os
import shutil

BACKUP_FOLDER = "exports"
DB_PATH = '../pdr-backend/pdr-backend/lake_data/duckdb.db'
BACKUP_FILE_NAME = "duckdb_backup.db"  # Static file name for the backup

def copy_db_file(db_path, backup_folder, backup_file_name):
    """
    Copy the DuckDB file to the backup folder.
    """

    # Ensure the backup folder exists
    os.makedirs(backup_folder, exist_ok=True)

    # Define the destination path for the backup
    backup_path = os.path.join(backup_folder, backup_file_name)

    try:
        # Copy the DuckDB file to the backup folder
        shutil.copy2(db_path, backup_path)
        print(f"Database copied successfully to {backup_path}")
    except Exception as e:
        print(f"Failed to copy the database: {e}")

def main():
    """
    Main function to run the backup.
    """
    copy_db_file(DB_PATH, BACKUP_FOLDER, BACKUP_FILE_NAME)
    print("Backup completed.")

if __name__ == "__main__":
    main()
