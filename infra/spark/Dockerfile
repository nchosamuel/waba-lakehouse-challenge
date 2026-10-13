FROM apache/spark:3.5.5
USER root
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.9.1/iceberg-spark-runtime-3.5_2.12-1.9.1.jar /opt/spark/jars/
ADD https://repo.maven.apache.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.9.1/iceberg-aws-bundle-1.9.1.jar /opt/spark/jars/
RUN pip install --no-cache-dir boto3==1.37.38 && chmod a+r /opt/spark/jars/*.jar
USER spark
