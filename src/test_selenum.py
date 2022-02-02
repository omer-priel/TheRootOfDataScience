# Data Collector

# System
import os
import json

# Web Scraping
import requests
import selenium
import selenium.webdriver
import selenium.webdriver.chrome
from bs4 import BeautifulSoup
import re

import urllib.parse

# Data
import numpy as np
import pandas as pd

DRIVER_PATH = 'C:\\Projects\\Python\\TheRootOfDataScience\\bin\\chromedriver_win32\\chromedriver.exe'
PROXY_HOST = 'proxy.crawlera.com:8011'
PROXY_AUTH = '771c716e95864cda990b52bac3f61b8d:'
CRT_PATH = 'C:\\Projects\\Python\\TheRootOfDataScience\\src\\zyte-proxy-ca.crt'

#chrome_service = selenium.webdriver.chrome.service.Service(DRIVER_PATH)

#chrome_options = selenium.webdriver.ChromeOptions()
#chrome_options.add_argument('-proxy-server=%s' % PROXY_HOST)
#chrome_options.add_argument('-proxy-type=https')
#chrome_options.add_argument('–proxy-auth=%s' % PROXY_AUTH)
#chrome_options.add_argument('–ssl-client-certificate-file=%s' % CRT_PATH)
#chrome_options.add_argument('ignore-certificate-errors')

options = {
    'proxy': {
        'http': "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
        'https': "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
        'no_proxy': "localhost,127.0.0.1",
    }
}

#browser = selenium.webdriver.Chrome(DRIVER_PATH)
#browser.get("https://www.google.com/")

#browser = selenium.webdriver.Chrome(service=chrome_service, options=chrome_options)

#browser.maximize_window()

#browser.get('https://www.google.com')