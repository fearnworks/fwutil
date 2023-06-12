# Use an official Python runtime as a parent image
FROM python:3.9

# Creates a non-root user and adds permission to access the /app folder
RUN addgroup --system app && adduser --system --group app
RUN apt-get update && apt-get install -y dos2unix
RUN apt-get update && apt-get install -y net-tools iproute2

# Creates work directory
WORKDIR /app/

# Copies everything from the current directory to the work directory in the Docker image
COPY ./app /app/
COPY ./app/.env /.env
RUN dos2unix /app/run.sh && dos2unix /.env && apt-get --purge remove -y dos2unix && rm -rf /var/lib/apt/lists/*

# Upgrade pip
RUN pip install --upgrade pip

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app

# Install app
RUN pip install .

# Install python-dotenv
RUN pip install python-dotenv[cli]

# Give execution permissions to the run.sh script
RUN chmod +x /app/run.sh

# Change the ownership of all files to the app user
RUN chown -R app:app /app

# Switch to the app user
USER app

# Expose port
EXPOSE 8001

# During debugging, this entry point will be overridden. 
CMD ["./run.sh" ]
