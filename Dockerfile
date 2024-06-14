# Use the Bitnami Spark image
FROM bitnami/spark:latest

# Switch to root user to install required packages and dependencies
USER root

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    passwd \
    && rm -rf /var/lib/apt/lists/*

# Install required Python dependencies
RUN pip install pyspark cassandra-driver boto3

# Download and set up Hadoop AWS dependency
RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -o /opt/bitnami/spark/jars/hadoop-aws-3.2.0.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar && \
    curl -o /opt/bitnami/spark/jars/aws-java-sdk-bundle-1.11.375.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar

# Create a user and set it as the default user for the container
RUN useradd -ms /bin/bash sparkuser
USER sparkuser

# Copy the Spark job script to the container
COPY product_views_processor.py /opt/spark/work-dir/
COPY config/minio_config.py /opt/spark/work-dir/config/
COPY config/cassandra_config.py /opt/spark/work-dir/config/

# Set the working directory
WORKDIR /opt/spark/work-dir/

# Set environment variables for Ivy and Spark
ENV IVY_HOME /home/sparkuser/.ivy2
ENV SPARK_HOME /opt/bitnami/spark

# Run the Spark job
ENTRYPOINT ["spark-submit", "--conf", "spark.jars.ivy=/home/sparkuser/.ivy2", "product_views_processor.py"]
