FROM dgreis/spark:latest

#For Movie Project
RUN mkdir /src
ADD movie_projects/src/ src/
ADD movie_projects/base_dependencies/ /Volumes/data

ENV PATH /usr/local/hadoop/bin:$PATH
ENV PYTHONPATH /src:$PYTHONPATH
ADD bootstrap.sh .

#To migrate to spark
RUN rm -rf /usr/lib/python2.7/dist-packages/numpy*
RUN rm -rf /usr/lib/python2.7/dist-packages/pandas*
RUN pip install numpy --force-reinstall
RUN pip install pandas --force-reinstall
RUN rm -rf /usr/local/lib/python2.7/dist-packages/pkg_resources/
#RUN wget https://bootstrap.pypa.io/ez_setup.py -O - | python
RUN curl https://bootstrap.pypa.io/get-pip.py | python
#RUN pip install setuptools --upgrade --no-use-wheel
RUN pip install nltk
RUN python -m nltk.downloader punkt
RUN python -m nltk.downloader averaged_perceptron_tagger
ENV SPARK_WORKER_MEMORY 2500M

RUN cp /spark-2.0.2/conf/spark-defaults.conf.template /spark-2.0.2/conf/spark-defaults.conf
RUN echo "spark.driver.memory 1500M" >> /spark-2.0.2/conf/spark-defaults.conf
RUN echo "spark.executor.memory	2500M" >> /spark-2.0.2/conf/spark-defaults.conf
RUN echo "spark.sql.shuffle.partitions 1" >> /spark-2.0.2/conf/spark-defaults.conf
RUN echo "spark.default.parallelism 2" >> /spark-2.0.2/conf/spark-defaults.conf
RUN echo "spark.memory.fraction 0.8" >> /spark-2.0.2/conf/spark-defaults.conf
RUN echo "spark.executor.extraJavaOptions=-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps" >> /spark-2.0.2/conf/spark-defaults.conf