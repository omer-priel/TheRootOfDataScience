# Data Collector

# System
import os
import json
import time

# Web Scraping
import requests
import seleniumwire
import seleniumwire.webdriver
from bs4 import BeautifulSoup
import re

import urllib.parse

# Data
import numpy as np
import pandas as pd

DRIVER_PATH = 'C:\\Projects\\Python\\TheRootOfDataScience\\bin\\chromedriver_win32\\chromedriver.exe'

options = {
    'proxy': {
        'http': "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
        'https': "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
        'no_proxy': "localhost,127.0.0.1",
    }
}

driver = seleniumwire.webdriver.Chrome(DRIVER_PATH, seleniumwire_options=options)

print('get')

def httpReqest(url):
    driver.get(url)
    return BeautifulSoup( driver.page_source, 'html.parser')

driver.get('https://www.yelp.com')

