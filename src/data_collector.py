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

def readCSV(name: str, index_label='id'):
    return pd.read_csv('../data/' + name + '.csv', index_col=index_label)

def saveCSV(df: pd.DataFrame, name: str, index_label='id'):
    df.to_csv('../data/' + name + '.csv', index_label=index_label)

# Entry Point

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

# collectors
def collectLocations():
    soup = httpRequest('https://www.yelp.com/locations')
    links = soup.select('.cities a')
    locations = []
    while len(links) > 0:
        locations.append(links.pop().getText())

    locations = pd.Series(locations)
    locations = locations.sort_values().tolist()
    df = pd.DataFrame({'Name': locations, 'Collected': np.zeros(len(locations))})
    saveCSV(df, 'locations')

def collectUrl():
    pass

def collectUrls():
    pass

def collectPage(url):
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


collectLocations()

df = readCSV('locations')

#collectPage('https://www.yelp.com/biz/farmhouse-kitchen-thai-cuisine-san-francisco')