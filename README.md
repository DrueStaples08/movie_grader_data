# movie_grader_data
This is a sub project that focuses on scraping web data for movie titles, genre, year, etc. and stores them in a Big Query database. Everytime a new movie comes out we can run this script to update the database (which can then update the cache in Google Cloud MemoryStore)

- The following scripts will download or scrape movie data (which can run every week, but include a try except that will bypass errors incase the movie website changed their frontend syntax)


download_datasets/

- Kaggle IMBD Data
