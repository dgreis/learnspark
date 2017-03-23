#refresh

docker kill master && docker kill slave2
docker build -f $(pwd)/movie_projects/Dockerfile . -t dgreis/movie:latest
bash movie_projects/quickstart.sh