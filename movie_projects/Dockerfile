FROM dgreis/devspark:latest

#For Movie Project
RUN mkdir /src
ADD src/ src/
ADD base_dependencies/ /Volumes/data

ENV PATH /usr/local/hadoop/bin:$PATH
ADD bootstrap.sh .



