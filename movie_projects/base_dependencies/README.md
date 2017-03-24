## Dependencies To Download:
## 	(to create base movie image)

- Put all the files downloaded from Movielens dataset in this folder:
	* movies.dat
	* users.dat
	* ratings.dat

Note: scrapeplot.sh (run on Docker host machine -- not inside a container) will build a plots.dat file here. This file might need a bit of massaging after it's done. There were a few new line characters that will break topic_modeling.py. It's not difficult to clean these up using pandas.
