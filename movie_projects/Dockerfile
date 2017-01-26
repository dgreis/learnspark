FROM dgreis/spark:latest

#For Movie Project
RUN mkdir /src
ADD movie_projects/src/ src/
ADD movie_projects/base_dependencies/ /Volumes/data

ENV PATH /usr/local/hadoop/bin:$PATH
ENV PYTHONPATH /src:$PYTHONPATH
ADD bootstrap.sh .



