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

    script = """
        function main(splash)
          local url = splash.args.url
          assert(splash:go(url))
          assert(splash:wait(0.5))
          splash:runjs("document.title='';")
          assert(splash:wait(3))
          local title = splash:evaljs("document.title")
          
          return splash:html()
        end
        """
    response = requests.get(
        fullUrl,
        auth=(splshUsername, ''),
        params={
            'url': url,
            'lua_source': script
            }
        )

    soup = BeautifulSoup(response.content, 'html.parser')
    return soup

url = 'https://www.yelp.com/biz/little-nnq-adelaide?osq=Restaurants'

#res = httpProxyRequest(url)
#print(res)