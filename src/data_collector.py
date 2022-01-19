# Data Collector

# System
import os
import json

# Web Scraping
import requests
from bs4 import BeautifulSoup

import urllib.parse

# Data
import numpy as np
import pandas as pd

# user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36

# Utilities
def httpRequest(url: str, useSplash: bool):
    if useSplash:
        print(url)
        url = urllib.parse.quote(url)
        url = 'http://192.168.1.117:8050/render.html?url=' + url
        res = requests.get(url)
    else:
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
    soup = httpRequest('https://www.yelp.com/locations', False)
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

    # Start to Collect Urls
    categoryName = 'Restaurants'
    print('Collactting \"' + categoryName + '\" in \"' + locationName + '\"')

    links = []

    startParam = 0
    url = ""
    while url != None:
        url = 'https://www.yelp.com/search?find_loc={}&find_desc={}&start={}'.format(locationName, categoryName, startParam)
        startParam += 10
        soup = httpRequest(url, True)
        main = soup.select_one('main ul')
        if main == None:
            url = None
        else:
            for link in main.select('a'):
                if link.get('href').find('/biz/') != -1:
                    links.append(link.get('href'))

    
    print("Create Table For The Data")

    # Remove Duplicates Links
    links = list(set(links))

    # Save Urls
    busines = readCSV('busines')

    new_busines = pd.Series(links)

    busines_data = empty_busines()

    for key in busines_data:
        busines_data[key] = np.full(len(links), None)

    busines_data['Loaded'] = np.full(len(links), False)
    busines_data['Url'] = new_busines.tolist()
    busines_data['Category'] = np.full(len(links), categoryName)

    df = pd.DataFrame(busines_data)

    df.index += len(busines.index)

    busines = pd.concat([busines, df])

    print("Save New Busines to load")
    saveCSV(busines, 'busines')

    # Set Collected to True
    locations.at[[locationID], 'Collected'] = True
    saveCSV(locations, 'locations')
    return True

def collectUrls():
    locations = readCSV('locations')

    hasNext = True
    while hasNext:
        hasNext = collectUrl(locations)

def removeDuplicatesUrls():
    busines = readCSV('busines')
    print('Before remove ' + str(len(busines.index)))

    busines.drop_duplicates(subset='Url', keep='first', inplace=True)
    busines.index = np.arange(len(busines.index))

    print('After remove ' + str(len(busines.index)))

    saveCSV(busines, 'busines')

def collectPage(url: str):
    busines = empty_busines()
    busines['Loaded'] = False
    busines['Url'] = url

    soup = httpRequest(url, False)

    root = soup.select_one('yelp-react-root div')
    header = root.select_one('[data-testid="photoHeader"]')
    footer = header.parent.findChildren("div", recursive=False)[3]

    print(header.getText())

    print('----------------------------')

    print(footer.getText())

# Entry Point

#collectLocations()

#collectUrls()
#removeDuplicatesUrls()

#collectPage('https://www.yelp.com/biz/farmhouse-kitchen-thai-cuisine-san-francisco')