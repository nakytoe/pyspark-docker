# pyspark-docker

My working directory for learning spark. No planned direction, but I may add here some scripts or solutions I find inspiring.

Build container with:

`docker build . -t pyspark`

Launch interactive shell:

`docker run -it pyspark`

To submit a script:

`docker run -it --entrypoint "/opt/spark/bin/spark-submit" pyspark script.py`

Note that no volume mount is installed, so any changes to scripts must be updated by rebuilding the container.

