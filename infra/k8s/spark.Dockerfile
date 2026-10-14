FROM waba-lakehouse-streaming:latest
USER root
ADD https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar /opt/spark/jars/
ADD https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar /opt/spark/jars/
RUN chmod a+r /opt/spark/jars/*.jar
COPY --chown=185:185 src /app/src
COPY --chown=185:185 jobs /app/jobs
COPY --chown=185:185 infra/k8s /app/infra/k8s
ENV PYTHONPATH=/app/src
USER spark
