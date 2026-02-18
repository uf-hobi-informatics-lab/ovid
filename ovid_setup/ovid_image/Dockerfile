# Use Bitnami Spark as the base image
FROM bitnamilegacy/spark:3.3
# Set the working directory
WORKDIR /app

# Copy the requirements.txt file
COPY requirements.txt /app

# Install dependencies as root
USER root

# Install a C compiler, pip3, and other required tools
RUN apt-get update && \
    apt-get install -y gcc wget && \
    pip3 install --no-cache-dir -r requirements.txt

RUN apt-get update && apt-get install -y zip

# Create required directories
RUN mkdir -p /docker_data && chmod 777 /docker_data

# Fix permissions for Spark directories
RUN chmod 777 /opt/bitnami/spark/tmp \
              /opt/bitnami/spark/conf \
              /opt/bitnami/spark/jars \
              /app

RUN mkdir -p /docker_data/spark_tmp && chmod -R 777 /docker_data
             
# Move app directory
# RUN mv /app /docker_app

# Environment Variables
ENV PYARROW_IGNORE_TIMEZONE=1

# Copy and Replace XOR.py (if needed)
COPY XOR.py /opt/bitnami/python/lib/python3.10/site-packages/Crypto/Cipher/

# Download the correct Spark-Snowflake connector (Scala 2.12, Spark 3.3)
RUN wget -O /opt/bitnami/spark/jars/spark-snowflake_2.12-2.16.0-spark_3.3.jar \
    https://repo1.maven.org/maven2/net/snowflake/spark-snowflake_2.12/2.16.0-spark_3.3/spark-snowflake_2.12-2.16.0-spark_3.3.jar

# Download the latest Snowflake JDBC driver
RUN wget -O /opt/bitnami/spark/jars/snowflake-jdbc.jar \
    https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/3.14.2/snowflake-jdbc-3.14.2.jar

# Ensure JAR files have the correct permissions
RUN chmod 644 /opt/bitnami/spark/jars/*.jar

# Verify JAR files exist
RUN ls -lah /opt/bitnami/spark/jars/

# Set user back to non-root
USER 1001

