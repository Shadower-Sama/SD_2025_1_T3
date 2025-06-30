FROM openjdk:11-jdk

RUN apt-get update && apt-get install -y \
    curl \
    wget \
    python3 \
    python3-pip \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /app

ENV HADOOP_VERSION=3.4.1
ENV HADOOP_HOME=/opt/hadoop
ENV HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

RUN wget -q https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz \
    && tar -xzf hadoop-$HADOOP_VERSION.tar.gz \
    && mv hadoop-$HADOOP_VERSION $HADOOP_HOME \
    && rm hadoop-$HADOOP_VERSION.tar.gz

ENV PIG_VERSION=0.17.0
ENV PIG_HOME=/opt/pig
ENV PATH=$PATH:$PIG_HOME/bin

RUN wget -q https://archive.apache.org/dist/pig/pig-$PIG_VERSION/pig-$PIG_VERSION.tar.gz \
    && tar -xzf pig-$PIG_VERSION.tar.gz \
    && mv pig-$PIG_VERSION $PIG_HOME \
    && rm pig-$PIG_VERSION.tar.gz

RUN echo "export JAVA_HOME=/usr/local/openjdk-11" >> $HADOOP_HOME/etc/hadoop/hadoop-env.sh

COPY requirements.txt .

RUN pip3 install --no-cache-dir -r requirements.txt

COPY main.py .

RUN mkdir -p /app/scripts /app/logs
VOLUME ["/app/scripts"]

RUN useradd -m -u 1001 pig && \
    chown -R pig:pig /app && \
    chown -R pig:pig $HADOOP_HOME && \
    chown -R pig:pig $PIG_HOME
USER pig

ENV PROCESSING_MODE=full_pipeline

CMD ["python", "main.py"]

HEALTHCHECK --interval=60s --timeout=30s --start-period=60s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1