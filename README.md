# My second research paper

I started working on this paper in my 6th semester, and got it presented in 6th semester itself at IEEE INDISCON, MNIT Jaipur.

# overview

i started working on it, because i thought that there is a lot of biasness in today's news and articles.
so we took five major news outlets which were: Al Jazeera, CNN, Fox News, PBS NewsHour and The Guardian.
they were covering different topics, people and events differently. We took 7 months data of these articles, there were around 54,000 articles. We studied them to understand which subjects recieve the most attention, how positive or negative the language is, and whether certain public figures (like donald trump) dominate coverage.
Overall, we found that politics recieves far more attention than science and technology, political stories are generally more negative, and donald trump is mentioned much more often than any other person. 
So it is basically a broad comparison of media coverage.

## What it does

- Discovers article URLs through RSS feeds, sitemaps, and Wayback
  Machine or New York Times archive providers.
- Extracts and normalizes article text.
- collects YouTube metadata and transcripts.
- exploratory data analysis and publication figures
- complete analysis of the data

## Analysis materials

- `notebooks/news-articles-eda.ipynb` contains exploratory analysis.
- `notebooks/news-articles-analysis.ipynb` contains the main analysis workflow.
- `publication_charts/` contains generated figures.
- `docs/` contains the research paper, certificate and presentation.
- Rest files are news scrapers and scripts for dataset cleaning, auditing.
