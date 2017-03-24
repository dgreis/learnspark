# learnspark

I created a docker-machine with 4GB but it can definitely be run with more memory if your computer can handle it.

`docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-disk-size "40000" default`

I wanted to create my environment almost entirely from scratch (including HDFS to use as file storage), so to build base-spark image first the base-hadoop image has to be built. I built that image using the https://github.com/sequenceiq/docker-hadoop-ubuntu image as a base with a few slight changes. I am going see if I can push my version to Docker Hub (along with the spark image) in the next few days so that it will be easy to pull these from the web. I am realizing now it's too complicated otherwise. Bear with me...