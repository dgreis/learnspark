docker rm master
docker run -i -p 47223:22 -p 5050:5050 -p 4040:4040 -p 48080:8080 -p 47777:7777 --name master dgreis/movie:latest bin/bash -c "bash bootstrap.sh -d -master 2" &

docker rm slave2
docker run -i -p 47224:22 -p 8081:8081 --name slave2 dgreis/movie:latest bin/bash -c "bash bootstrap.sh -d -slave 2" &
