# flink/Dockerfile
FROM flink:latest
USER root

# --- Install Python & dependencies ---
RUN apt-get update && \
    apt-get install -y python3 python3-pip python3-dev build-essential curl && \
    if [ ! -e /usr/bin/python ]; then ln -s /usr/bin/python3 /usr/bin/python; fi && \
    pip3 install --no-cache-dir --upgrade pip

# --- Working dir for the job ---
WORKDIR /opt/flink/flink_app


COPY requirements.txt /opt/flink/flink_app/requirements.txt

# Install python deps if requirements.txt exists
RUN if [ -f /opt/flink/flink_app/requirements.txt ]; then \
    pip3 install --no-cache-dir -r /opt/flink/flink_app/requirements.txt ;\
fi

# --- Copy python job and requirements ---
COPY flink_job.py /opt/flink/flink_app/flink_job.py

# --- Copy the wait-and-submit script ---
COPY wait-for-jobmanager.sh /opt/flink/wait-for-jobmanager.sh
RUN chmod +x /opt/flink/wait-for-jobmanager.sh

# --- Prepare log dir & permissions ---
RUN mkdir -p /opt/flink/log && \
    chown -R flink:flink /opt/flink && \
    chmod -R 755 /opt/flink && \
    chmod -R 777 /opt/flink/log

COPY connectors/*.jar /opt/flink/lib/

# Run as flink user
USER flink

# ENTRYPOINT: wait for cluster jobmanager then submit the python job
ENTRYPOINT ["/opt/flink/wait-for-jobmanager.sh"]
