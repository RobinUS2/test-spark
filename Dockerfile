# Use the official Apache Spark image with Python support
FROM apache/spark-py:latest

# Install additional Python packages as root user
USER root
RUN pip install requests
RUN pip install pandas sqlalchemy

# Create data directory for SQLite database
RUN mkdir -p /app/data && chmod 777 /app/data

# Switch back to spark user and set working directory
#USER spark
WORKDIR /app

# Copy the Python modules and data files to the container
COPY main.py /app/
COPY utils.py /app/
COPY database.py /app/
COPY lastfm_api.py /app/
COPY musicbrainz_api.py /app/
COPY testdata.txt /app/
COPY db_config.py /app/
COPY setup_db.sh /app/
RUN chmod +x /app/setup_db.sh

# Set environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-*.zip

# Run the Python script using spark-submit
CMD ["spark-submit", "--master", "local[*]", "/app/main.py"]