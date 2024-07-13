FROM python:3.9-slim

# Setting the working directory in the container
WORKDIR /app

# Copying the current directory contents into the container at /app location
COPY . /app

# Installing any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Command to run when the container starts
CMD ["python", "generate_sample_csv.py"]
