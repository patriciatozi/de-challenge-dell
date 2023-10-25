import urllib.request

url = 'https://storage.googleapis.com/covid19-open-data/v3/vaccinations.csv'

urllib.request.urlretrieve(url, 'data_ingestion/temp/vaccinations.csv')