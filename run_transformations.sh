#!/bin/bash

# Enable command tracing (prints commands as they are executed)
set -x
# source .venv/bin/activate
# Run the Python script (using Linux forward slashes)
uv run ./create_secrets.py

# Change directory
cd sqlmesh_motherduck

# Remove the db.db file to ensure a fresh start for the migration (uncomment if needed)
# rm db.db

# Run SQLMesh commands
sqlmesh migrate
sqlmesh plan --auto-apply

# Return to the parent directory
cd ..

# Disable command tracing
set +x