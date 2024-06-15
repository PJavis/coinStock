FROM docker.io/bitnami/spark:3.3

# Copy the requirements file into the image
COPY requirements.txt /app/requirements.txt

# Set the working directory
WORKDIR /app

# Install Python packages
RUN pip install -r /app/requirements.txt

# Set environment variables
ENV PYTHONPATH=/app
ENV PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:/opt/bitnami/python/bin:$PATH

# Copy the application code
COPY . /app
