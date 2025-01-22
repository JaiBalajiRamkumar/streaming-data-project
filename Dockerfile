# Use Python as base image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy project files to the container
COPY . /app

# Install project dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port your application listens on
EXPOSE 8080

# Set the entrypoint script
CMD ["sh", "script/entrypoint.sh"]
