from __future__ import division
import operator
import wikipedia as wikipedia
from wikipedia import DisambiguationError
from bs4 import BeautifulSoup
import os
import sys
import pandas as pd

def search_wikipedia(query):
    results = filter(lambda x: 'disambiguation' not in x, wikipedia.search(query))
    return results

def extract_plot_paragraphs(soup):
    h2s = soup.find_all("h2")
    for h2 in h2s:
        if (h2.find("span", {"id": "Plot"})):
            prime_el = h2
        else:
            pass
    if 'prime_el' not in locals():
        return 'Algo search did not find movie article'
    paragraphs = list()
    beyond = False
    while beyond != True:
        new_el = prime_el.next_sibling
        if new_el.name is not None:
        # try:
            el_name = new_el.name
        else:
        #except AttributeError:
            prime_el = new_el
            continue
        if el_name == 'p':
            paragraphs.append(new_el.get_text())
            prime_el = new_el
        else:
            #print 'not a p. it is a: ' + el_name
            beyond = True
    return ('\n').join(paragraphs)

def determine_film_art(results):
    artdict = dict()
    for r in results:
        try:
            pg_i = wikipedia.page(r)
        except DisambiguationError:
            continue
        #num_cats_i = len(pg_i.categories)
        num_film_cats_i = len(filter(lambda x: 'film' in x.lower(), pg_i.categories))
        #film_frac_i = num_film_cats_i / num_cats_i
        #film_frac_i = num_film_cats_i
        #artdict[r] = film_frac_i
        artdict[r] = num_film_cats_i
    sorted_x = sorted(artdict.items(), key=operator.itemgetter(1))
    sorted_x.reverse()
    top_result = sorted_x[0][0]
    return top_result

def load_data(samp_size):
    cwd = os.getcwd()
    parent_dir = cwd.split('learnspark')[0]
    data_dir = parent_dir +'learnspark/movie_projects/base_dependencies/'
    if os.path.isfile(data_dir + 'plot_summaries.dat'):
        infile = data_dir + 'plot_summaries.dat'
        df = pd.read_csv(infile, sep='\t')
        finished_movies = df['title'].unique().tolist()
        print "excluding movies: " + (',').join(finished_movies)
    movie_file = data_dir + 'movies.dat'
    movies = process_input(movie_file)
    movies_to_finish = filter(lambda x: x not in finished_movies, movies)
    if samp_size is not None:
        return movies[0:samp_size]
    else:
        return movies

def persist_data(df):
    cwd = os.getcwd()
    parent_dir = cwd.split('learnspark')[0]
    data_dir = parent_dir +'learnspark/movie_projects/base_dependencies/'
    plot_file = data_dir + 'plot_summaries.dat'
    if os.path.isfile(plot_file):
        dfe = pd.read_csv(infile, sep='\t')
    dfe = dfe.append(df)
    dfe.to_csv(plot_file, sep='\t', encoding='utf-8',index=False)

def process_input(file):
    with open(file) as f:
        lines = f.readlines()
        titles = list()
        for l in lines:
            tit = l.split('::')[1].split('(')[0].strip()
            titles.append(tit)
    return titles

def main():
    try:
        samp_size = int(sys.argv[1])
    except IndexError:
        samp_size = None
    movies = load_data(samp_size)
    plot_dict = dict()
    #movies = ["Goldeneye"]
    for mov in movies:
        print mov
        search_results = search_wikipedia(mov)
        print search_results
        film_art_title = determine_film_art(search_results)
        film_art_page = wikipedia.page(film_art_title)
        soup = BeautifulSoup(film_art_page.html())
        plot_pars = extract_plot_paragraphs(soup)
        plot_dict[mov] = plot_pars
    df = pd.DataFrame.from_dict(plot_dict,orient='index')
    df = df.reset_index()
    df.columns = ['title','plot_summary']
    persist_data(df)



if __name__ == '__main__':
    main()