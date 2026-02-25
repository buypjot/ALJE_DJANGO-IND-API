# Use a lightweight Python base image
FROM python:3.12-slim

# Set environment variables for optimal Python behavior
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies for MSSQL, Redis, and Python
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl apt-transport-https gnupg2 unixodbc-dev && \
    curl https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg && \
    mkdir -p /etc/apt/keyrings && \
    echo "deb [signed-by=/etc/apt/keyrings/microsoft.gpg] https://packages.microsoft.com/debian/10/prod/ buster main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y --no-install-recommends \
    msodbcsql17 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Redis CLI for testing (optional, can be removed if not needed)
#RUN apt-get install -y redis-tools

# Copy the requirements files and install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application files
COPY . /app/

# Ensure Gunicorn is installed for running the app
RUN pip install --no-cache-dir gunicorn

# Collect static files if applicable
RUN python manage.py collectstatic --noinput || echo "No static files to collect"

# Expose the port the app will run ons
EXPOSE 8000

# Command to run the application
CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:8000", "ALJE_PROJECT.wsgi:application"]
