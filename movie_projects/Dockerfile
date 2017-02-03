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
RUN wget https://bootstrap.pypa.io/ez_setup.py -O - | python
RUN pip install nltk



