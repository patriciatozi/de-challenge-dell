import urllib.request

url = 'https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv'

urllib.request.urlretrieve(url, 'data-ingestion/temp/vaccinations.csv')