# learnspark

I created a docker-machine with 4GB but it can definitely be run with more memory if your computer can handle it.

`docker-machine create -d virtualbox --virtualbox-memory 4096 --virtualbox-disk-size "40000" default`

[dgreis/spark](https://hub.docker.com/r/dgreis/spark/) was built using the Dockerfile in this directory. It is available to pull from Docker Hub—just clieck on the link above. dgreis/spark has [sequenceiq/docker-hadoop-ubuntu](https://github.com/sequenceiq/docker-hadoop-ubuntu) as its base image.

For info on movie projects, look in the `movie_projects` folder, but switch to the `movie` branch first.
