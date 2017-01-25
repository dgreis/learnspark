FROM dgreis/hadoop:latest

RUN echo 'root:screencast' | chpasswd
RUN sed -i 's/PermitRootLogin without-password/PermitRootLogin yes/' /etc/ssh/sshd_config

#Spark
ADD base_dependencies/spark-2.0.2.tgz .
#RUN cd spark-0.9.2/
#RUN sbt/sbt assembly 
RUN cd spark-2.0.2 && ./build/mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests clean package

#Spark Env Settings
RUN sed '1d' /etc/hosts > tmpHosts
RUN cat tmpHosts > /etc/hosts
RUN rm tmpHosts 

ENV SPARK_MASTER_OPTS="-Dspark.driver.port=7001 -Dspark.fileserver.port=7002 \
 -Dspark.broadcast.port=7003 -Dspark.replClassServer.port=7004 \
 -Dspark.blockManager.port=7005 -Dspark.executor.port=7006 \
 -Dspark.ui.port=4040 \
 -Dspark.broadcast.factory=org.apache.spark.broadcast.HttpBroadcastFactory"
ENV SPARK_WORKER_OPTS="-Dspark.driver.port=7001 -Dspark.fileserver.port=7002 \
 -Dspark.broadcast.port=7003 -Dspark.replClassServer.port=7004 \
 -Dspark.blockManager.port=7005 -Dspark.executor.port=7006 \
 -Dspark.ui.port=4040 -Dspark.broadcast.factory=org.apache.spark.broadcast.HttpBroadcastFactory"

ENV SPARK_MASTER_PORT 7077
ENV SPARK_MASTER_WEBUI_PORT 8080
ENV SPARK_WORKER_PORT 8888
ENV SPARK_WORKER_WEBUI_PORT 8081
ENV SPARK_MASTER_HOST 172.17.0.2

EXPOSE 8080 7077 8888 8081 4040 7001 7002 7003 7004 7005 7006

#Apache Zeppelin
ADD ./base_dependencies/zeppelin-0.6.2-bin-all.tgz .
ENV ZEPPELIN_PORT 5050
ENV SPARK_HOME /spark-2.0.2
ENV PYTHONPATH $SPARK_HOME:/usr/bin/python
ENV PYTHONPATH $SPARK_HOME/python/lib/py4j-0.10.3-src.zip:$PYTHONPATH
ENV PYTHONPATH $SPARK_HOME/python/build:$PYTHONPATH
ENV PYTHONPATH $SPARK_HOME/python:$PYTHONPATH

#Not sure why I need this block, but I do
RUN sudo apt-get update
RUN sudo apt-get install 
RUN sed -i 's/usr\/java\/default/usr\/bin\/usr\/lib\/jvm\/java-1.7.0-openjdk-amd64/' /etc/profile

#Jupyter install
#RUN sudo apt-get install python-pip python-dev build-essential -y
RUN sudo apt-get install python-dev python-pip python-numpy python-scipy python-pandas gfortran -y
RUN sudo pip install --upgrade pip 
RUN sudo pip install --upgrade virtualenv 
RUN pip install --upgrade pip
RUN sudo pip install nose "ipython[notebook]"

ENV PYSPARK_DRIVER_PYTHON ipython
ENV PYSPARK_DRIVER_PYTHON_OPTS "notebook --no-browser --port=7777 --i 0.0.0.0"

RUN sudo apt-get install vim -y
