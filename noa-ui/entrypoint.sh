#!/usr/bin/env bash
set -e  # Exit on error
set -x  # Debugging output

echo "Starting entrypoint script..."

# Activate virtual environment if needed
# source /app/venv/bin/activate  # Uncomment if using a virtual environment

echo "Postgres is up - executing command"

# Run Django migrations
echo "Running migrations..."
python manage.py migrate --noinput

# Collect static files
echo "Collecting static files..."
python manage.py collectstatic --noinput

# Start the Django server
echo "Starting Django application..."
exec python manage.py runserver 0.0.0.0:8000

