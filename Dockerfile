FROM apache/spark-py

USER root
RUN mkdir /app
COPY . /app
WORKDIR /app

RUN pip install numpy pandas pyarrow

VOLUME [ "/data" ]

ENTRYPOINT ["/opt/spark/bin/pyspark"]
