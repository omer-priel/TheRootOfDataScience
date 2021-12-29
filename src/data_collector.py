# Data Collector

# System
import os
import json

# Web Scraping
import requests
from bs4 import BeautifulSoup

# Data
import numpy as np
import pandas as pd


# Utilities
def httpRequest(url: str):
    res = requests.get(url)
    soup = BeautifulSoup(res.content, 'html.parser')

    return soup

def readCSV(name: str, index_label='id') -> pd.DataFrame:
    return pd.read_csv('../data/' + name + '.csv', index_col=index_label)

def saveCSV(df: pd.DataFrame, name: str, index_label='id'):
    df.to_csv('../data/' + name + '.csv', index_label=index_label)

# Classes
busines_template = {
    'Name': None,
    'Url': None,
    'Loaded': None,
    "ExpensiveLevel": None,
    'Category': None,
    'Claimed': None,
    'HasWebsite': None,
    'Stars': None,
    'Reviews': None,
    'Photos': None,

    'MenuCount': None,
    'MenuStartsCount': None,
    'MenuReviewsCount': None,
    'MenuPhotosCount': None,

    'Attributes Has': None,
    'AttributesCount': None,
    'QuestionsCount': None
}

for i in range(1, 8):
    busines_template['OpenHour' + str(i)] = None
    busines_template['EndHour' + str(i)] = None

def empty_busines():
    return busines_template.copy()

# Collectors
def collectLocations():
    soup = httpRequest('https://www.yelp.com/locations')
    links = soup.select('.cities a')
    locations = []
    while len(links) > 0:
        locations.append(links.pop().getText())

    locations = pd.Series(locations)
    locations.sort_values(inplace=True)

    df = pd.DataFrame({'Name': locations.tolist(), 'Collected': np.full(len(locations), False)})
    df.drop(index=df.tail(8).index, inplace=True)

    saveCSV(df, 'locations')
    saveCSV(df, 'original/locations')

def collectUrl(locations: pd.DataFrame):
    # Get Location name
    indexes = locations[locations['Collected'] == False].index.tolist()
    if len(indexes) == 0:
        return False
    locationID = indexes[0]
    locationName = locations.iloc[locationID]['Name']

    # Collect Url
    print(locationName)
    categoryName = 'Restaurants'

    url = 'https://www.yelp.com/search?find_loc={}&find_desc={}'.format(locationName, categoryName)

    while url != None:
        print(url)
        soup = httpRequest(url)

        main = soup.select_one('main ul')
        print(main.find_all('h3', string="All Results"))
        print(main.find_all('h3'))

        url = None

    # Set Collected to True
    locations.at[[locationID], 'Collected'] = True
    return True

def collectUrls():
    locations = readCSV('locations')

    hasNext = True
    while hasNext:
        hasNext = collectUrl(locations)
        hasNext = False

    #saveCSV(locations, 'locations')

def collectPage(url: str):
    busines = empty_busines()
    busines['Loaded'] = False
    busines['Url'] = url

    soup = httpRequest(url)

    root = soup.select_one('yelp-react-root div')
    header = root.select_one('[data-testid="photoHeader"]')
    footer = header.parent.findChildren("div", recursive=False)[3]

    print(header.getText())

    print('----------------------------')

    print(footer.getText())

# Entry Point

# collectLocations()

collectUrls()

#collectPage('https://www.yelp.com/biz/farmhouse-kitchen-thai-cuisine-san-francisco')