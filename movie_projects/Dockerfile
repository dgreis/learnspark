FROM dgreis/devspark:latest

#For Movie Project
RUN mkdir /src
ADD src/ src/
ADD base_dependencies/ /Volumes/data

RUN sudo apt-get update
RUN sudo apt-get install 
RUN sed -i 's/usr\/java\/default/usr\/bin\/usr\/lib\/jvm\/java-1.7.0-openjdk-amd64/' /etc/profile

#Jupyter install
#(To migrate to devspark DockerFile)
#RUN sudo apt-get install python-pip python-dev build-essential -y
RUN sudo apt-get install python-dev python-pip python-numpy python-scipy python-pandas gfortran -y
RUN sudo pip install --upgrade pip 
RUN sudo pip install --upgrade virtualenv 
RUN pip install --upgrade pip
RUN sudo pip install nose "ipython[notebook]"


ENV PATH /usr/local/hadoop/bin:$PATH
ADD bootstrap.sh .



