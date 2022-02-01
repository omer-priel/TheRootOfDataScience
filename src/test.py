# Data Collector

# System
import os
import json

# Web Scraping
import requests
from bs4 import BeautifulSoup
import re

import urllib.parse

# Data
import numpy as np
import pandas as pd

def httpProxyRequest(url:  str):
    splshHost = 'https://2tgcg9vk-splash.scrapinghub.com'
    splshUsername = 'e9e31311d4144b92b99618c0b3f7a0ab'

    fullUrl = splshHost + '/execute'

    f = open("./lua/splash_script.lua", "r")
    script = f.read()
    f.close()
    print(script)
    response = requests.get(
        fullUrl,
        auth=(splshUsername, ''),
        params={
            'url': url,
            'lua_source': script
            }
        )
    print(response.content)
    soup = json.loads(response.content)

    return soup

url = 'https://www.yelp.com/biz/little-nnq-adelaide?osq=Restaurants'

res = httpProxyRequest(url)
command = res.get('command')
soup = res.get('html')
#soup = BeautifulSoup(soup, 'html.parser'),
elements = res.get('elements')
#print(len(soup.body.select('yelp-react-root div section[aria-label=\"Amenities and More\"] span')))