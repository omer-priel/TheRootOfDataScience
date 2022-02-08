# Data Collector

# System
import os
import json
import sys

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

# Consts
DRIVER_PATH = 'C:\\Projects\\Python\\TheRootOfDataScience\\bin\\chromedriver_win32\\chromedriver.exe'


# Utilities Requests
def http_request(url: str, use_splash: bool) -> BeautifulSoup:
    if use_splash:
        print(url)
        url = urllib.parse.quote(url)
        url = 'http://192.168.1.117:8050/render.html?url=' + url
        response = requests.get(url)
    else:
        response = requests.get(url)

    soup = BeautifulSoup(response.content, 'html.parser')

    return soup


def http_proxy_request(url: str, use_splash: bool) -> BeautifulSoup:
    if use_splash:
        splash_host = 'https://2tgcg9vk-splash.scrapinghub.com'
        splash_username = 'e9e31311d4144b92b99618c0b3f7a0ab'

        full_url = splash_host + '/render.html'

        response = requests.get(
            full_url,
            auth=(splash_username, ''),
            params={
                'url': url
            }
        )
    else:
        response = requests.get(
            url,
            proxies={
                "http": "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
                "https": "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
            },
            verify='./zyte-proxy-ca.crt'

        )

    soup = BeautifulSoup(response.content, 'html.parser')
    return soup


def create_selenium_driver() -> seleniumwire.webdriver.Chrome:
    driver_options = {
        'proxy': {
            'http': "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
            'https': "http://771c716e95864cda990b52bac3f61b8d:@proxy.crawlera.com:8011/",
            'no_proxy': "localhost,127.0.0.1",
        }
    }

    driver = seleniumwire.webdriver.Chrome(DRIVER_PATH, seleniumwire_options=driver_options)

    return driver


def http_selenium_request(url: str, driver: seleniumwire.webdriver.Chrome) -> BeautifulSoup:
    driver.get(url)
    return BeautifulSoup(driver.page_source, 'html.parser')


# Utilities Files
def read_csv(name: str, index_label='id') -> pd.DataFrame:
    return pd.read_csv('../data/' + name + '.csv', index_col=index_label)


def save_csv(df: pd.DataFrame, name: str, index_label='id'):
    df.to_csv('../data/' + name + '.csv', index_label=index_label)


# Classes
business_template = {
    'Name': None,
    'Url': None,
    'Loaded': None,
    "ExpensiveLevel": None,
    'Category': None,
    'SubCategories': None,
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
    'QuestionsCount': None,

    'OpenDaysCount': None,
    'HasBreaks': None
}

for i in range(1, 8):
    business_template['OpenHour' + str(i)] = None
    business_template['EndHour' + str(i)] = None
    business_template['CountHour' + str(i)] = None
    business_template['HasBreaks' + str(i)] = None


def empty_business() -> dict:
    return business_template.copy()


# Collectors
def collect_locations():
    soup = http_request('https://www.yelp.com/locations', False)
    links = soup.select('.cities a')
    locations = []
    while len(links) > 0:
        locations.append(links.pop().getText())

    locations = pd.Series(locations)
    locations.sort_values(inplace=True)

    df = pd.DataFrame({'Name': locations.tolist(), 'Collected': np.full(len(locations), False)})
    df.drop(index=df.tail(8).index, inplace=True)

    save_csv(df, 'locations')
    save_csv(df, 'original/locations')


def collect_url(locations: pd.DataFrame) -> bool:
    # Get Location name
    indexes = locations[locations['Loaded'] == False].index.tolist()
    if len(indexes) == 0:
        return False
    location_id = indexes[0]
    location_name = locations.iloc[location_id]['Name']

    # Start to Collect Urls
    category_name = 'Restaurants'
    print('Collecting \"' + category_name + '\" in \"' + location_name + '\"')

    links = []

    start_param = 0
    url = ""
    while url is not None:
        url = 'https://www.yelp.com/search?find_loc={}&find_desc={}&start={}'.format(
            location_name, category_name, start_param)

        start_param += 10
        soup = http_request(url, True)
        main = soup.select_one('main ul')
        if main is None:
            url = None
        else:
            for link in main.select('a'):
                if link.get('href').find('/biz/') != -1:
                    links.append('https://www.yelp.com' + link.get('href'))

    print("Create Table For The Data")

    # Remove Duplicates Links
    links = list(set(links))

    # Save Urls
    businesses = read_csv('businesses')

    new_businesses = pd.Series(links)

    businesses_data = empty_business()

    for key in businesses_data:
        businesses_data[key] = np.full(len(links), None)

    businesses_data['Loaded'] = np.full(len(links), False)
    businesses_data['Url'] = new_businesses.tolist()
    businesses_data['Category'] = np.full(len(links), category_name)

    businesses_data['Name'] = businesses_data['Name'].astype(str)
    businesses_data['SubCategories'] = businesses_data['SubCategories'].astype(object)
    businesses_data['Attributes Has'] = businesses_data['Attributes Has'].astype(object)

    df = pd.DataFrame(businesses_data)
    df.index += len(businesses.index)

    for i in df.index:
        df.at[i, 'Name'] = ' '
        df.at[i, 'SubCategories'] = []
        df.at[i, 'Attributes Has'] = []

    businesses = pd.concat([businesses, df])

    print("Save New Businesses to load")
    save_csv(businesses, 'businesses')

    # Set Collected to True
    locations.at[[location_id], 'Collected'] = True
    save_csv(locations, 'locations')
    return True


def collect_urls():
    locations = read_csv('locations')

    has_next = True
    while has_next:
        has_next = collect_url(locations)


def remove_duplicates_urls():
    businesses = read_csv('businesses')
    print('Before remove ' + str(len(businesses.index)))

    businesses.drop_duplicates(subset='Url', keep='first', inplace=True)
    businesses.index = np.arange(len(businesses.index))

    print('After remove ' + str(len(businesses.index)))

    save_csv(businesses, 'businesses')


def collect_pages(collector_number=None, collectors=None):
    print("Collect Pages")

    if collectors is None:
        if len(sys.argv) != 3:  # python data_collector.py <collectorNumber> <collectors>
            print('Not have args!')
            return None
        collectors = int(sys.argv[2])
    if collector_number is None:
        collector_number = int(sys.argv[1])

    businesses = read_csv('businesses_' + str(collector_number))

    errors = 0
    status = 1
    while status != 0:
        status = collect_page(businesses, errors, collector_number, collectors)

        if status == 2:
            errors = errors + 1
            print('errors: ', errors)

    print('status: ' + str(status))


def collect_page(businesses: pd.DataFrame, errors: int, collector_number: int, collectors: int):
    business_id = None
    try:
        # Get Business Url
        indexes = businesses[
            (businesses.index % collectors == collector_number) & (businesses['Loaded'] == 0)].index.tolist()
        if len(indexes) <= errors:
            return 0

        business_id = indexes[errors]
        print(business_id)

        if not business_id < len(businesses.index):
            return 0
    except Exception as inst:
        print('except 1: ', inst)
        return 1

    try:
        url = businesses.iloc[business_id]['Url']

        # Start to collect the Page
        try:
            soup = http_proxy_request(url, False)

            if (soup.getText().find('Sorry, you') != -1) and soup.getText().find(
                    're not allowed to access this page.') != -1:
                print('Sorry, you are not allowed to access this page.')
                return 0

            # driver.execute_script('document.querySelector(\'section[aria-label="Amenities and More"] button[aria-controls]\').click();')
            # soup = BeautifulSoup(driver.page_source, 'html.parser')

            root = soup.select_one('yelp-react-root div')
            header = root.select_one('[data-testid="photoHeader"]')

            is_photo_header = True
            if header is None:
                is_photo_header = False
                header = root.find('h1').parent.parent.parent.parent
        except:
            soup = http_proxy_request(url, False)

            if (soup.getText().find('Sorry, you') != -1) and soup.getText().find(
                    're not allowed to access this page.') != -1:
                print('Sorry, you are not allowed to access this page.')
                return 0

            # driver.execute_script('document.querySelector(\'section[aria-label="Amenities and More"] button[aria-controls]\').click();')
            # soup = BeautifulSoup(driver.page_source, 'html.parser')

            root = soup.select_one('yelp-react-root div')
            header = root.select_one('[data-testid="photoHeader"]')

            is_photo_header = True
            if header is None:
                is_photo_header = False
                header = root.find('h1').parent.parent.parent.parent

        collected_head = collect_head_info(header, is_photo_header)
        collected_body = collect_page_body(root, header, is_photo_header)

        for key in collected_head:
            businesses.at[business_id, key] = collected_head[key]

        for key in collected_body:
            businesses.at[business_id, key] = collected_body[key]

        # Set Collected to True
        businesses.at[business_id, 'Loaded'] = 1

        save_csv(businesses, 'businesses_' + str(collector_number))
        return 1

    except:
        businesses.at[collector_number, 'Loaded'] = 2

        save_csv(businesses, 'businesses_' + str(collector_number))
        return 2


def collect_head_info(header, isPhotoHeader):
    # find "photo count" in header
    photoCount = 0
    if isPhotoHeader and header.find(string=re.compile('Add photo or video')) == None:
        photoCount = header.find(string=re.compile('See \d+ photos'))
        photoCount = int(re.findall("\d+", photoCount)[0])

    # gets into where the fun stuff is so we wont have to search as much
    headInfo = header
    if isPhotoHeader:
        headInfo = header.find(class_=re.compile('photo\-header\-content__.+'))
    headInfo = headInfo.findChild().findChild(class_=re.compile('.+ arrange-unit-fill.+'))

    # find "expensive level" in header
    elms = headInfo.find(string=re.compile('\$+'))
    expensiveLevel = 0
    if elms != None:
        expensiveLevel = len(elms)

    # find the rating of the resturant
    starRating = headInfo.find(class_=re.compile('.i-stars.+'))['aria-label']
    starRating = float((re.findall("[0-5]\.?5?", starRating))[0])
    starRating = int(starRating * 2)

    # find the number of ratings
    reviews = headInfo.find(text=re.compile('.+review'))
    reviews = int(re.findall("\d+", reviews)[0])

    # find whether a resturant is claimed
    isClaimed = headInfo.find(text=re.compile('.+laimed'))
    isClaimed = (isClaimed == "Claimed")

    # only categories have this type of link as it seems
    categorieslinks = headInfo.findAll(href=re.compile('/c/.+'))
    categories = []
    for category in (categorieslinks):
        categories.append(category.get_text())

    return {
        "ExpensiveLevel": float(expensiveLevel),
        "Stars": float(starRating),
        "Claimed": float(isClaimed),
        "Reviews": float(reviews),
        "SubCategories": categories,
        "Photos": float(photoCount)
    }


def collect_page_body(root: BeautifulSoup, header: BeautifulSoup, is_photo_header):
    data = {
        'Name': ' ',
        'HasWebsite': None,
        'MenuCount': None,
        'MenuStartsCount': None,  # Removed
        'MenuReviewsCount': None,  # Removed
        'MenuPhotosCount': None,  # Removed
        'Attributes Has': None,
        'AttributesCount': None,
        'QuestionsCount': None
    }

    for i in range(1, 8):
        data['OpenHour' + str(i)] = None
        data['EndHour' + str(i)] = None
        data['CountHour' + str(i)] = None
        data['HasBreak' + str(i)] = None

    # Get Name
    try:
        name_elem = header.select_one('h1')
        if name_elem is not None:
            data['Name'] = name_elem.getText()
    except:
        pass

    # Get Has Website
    try:
        has_website = 0
        label_elem = root.find('p', string='Business website')
        if label_elem is not None:
            website_url_elem = label_elem.findNext('a')
            if website_url_elem is not None:
                has_website = float(website_url_elem.getText().find('http') != -1)

        data['HasWebsite'] = has_website
    except:
        pass

    # Get OpenHour, EndHour, CountHour
    try:
        labels = root.select('table tr th p')

        if len(labels) == 7:
            day_number = 1
            for i in [6, 0, 1, 2, 3, 4, 5]:
                try:
                    value_cell = labels[i].parent.parent.select_one('td')
                    if value_cell is not None:
                        values = value_cell.select('li')
                        if len(values) > 0:
                            if values[0].getText() == 'Closed':
                                data['CountHour' + str(day_number)] = 0
                                data['HasBreak' + str(day_number)] = float(0)
                            else:
                                data['HasBreak' + str(day_number)] = float(len(values) - 1)

                                count_hour = 0
                                first_open_hour = None
                                last_end_hour = None

                                for j in range(len(values)):
                                    sp = values[j].getText().split(' - ')
                                    open_hour = time_to_number(sp[0], True)
                                    end_hour = time_to_number(sp[1], False)

                                    if first_open_hour is None:
                                        first_open_hour = open_hour
                                    last_end_hour = end_hour

                                    count_hour += end_hour - open_hour

                                data['OpenHour' + str(day_number)] = first_open_hour
                                data['EndHour' + str(day_number)] = last_end_hour
                                data['CountHour' + str(day_number)] = count_hour
                except:
                    pass
                day_number += 1
    except:
        pass

    # Get Attributes
    try:
        attributes_panel = root.select('section[aria-label="Amenities and More"] > div')[1]
        attributes_sub_panel = attributes_panel.findChild('div').findChild('div').findChild('div')
        attributes = []
        for elem in attributes_sub_panel.select('div span'):
            if elem.select_one('svg') is None and elem.getText() != "":
                attributes.append(elem.getText())

        data['Attributes Has'] = attributes
        data['AttributesCount'] = len(attributes)

    except:
        pass

    # Get QuestionsCount
    try:
        label = root.select_one('h4:-soup-contains(\"Frequently Asked Questions about \")')
        questions_parent = label.parent.parent.parent
        questions_count = len(questions_parent.select('p[data-font-weight="bold"]'))
        data['QuestionsCount'] = float(questions_count)

    except:
        pass

    return data


time_open_select = [
    '12:00 AM', '12:30 AM',
    '1:00 AM', '1:30 AM',
    '2:00 AM', '2:30 AM',
    '3:00 AM', '3:30 AM',
    '4:00 AM', '4:30 AM',
    '5:00 AM', '5:30 AM',
    '6:00 AM', '6:30 AM',
    '7:00 AM', '7:30 AM',
    '8:00 AM', '8:30 AM',
    '9:00 AM', '9:30 AM',
    '10:00 AM', '10:30 AM',
    '11:00 AM', '11:30 AM',
    '12:00 PM', '12:30 PM',
    '1:00 PM', '1:30 PM',
    '2:00 PM', '2:30 PM',
    '3:00 PM', '3:30 PM',
    '4:00 PM', '4:30 PM',
    '5:00 PM', '5:30 PM',
    '6:00 PM', '6:30 PM',
    '7:00 PM', '7:30 PM',
    '8:00 PM', '8:30 PM',
    '9:00 PM', '9:30 PM',
    '10:00 PM', '10:30 PM',
    '11:00 PM', '11:30 PM',
]

time_end_select = [
    '12:00 AM', '12:30 AM',
    '1:00 AM', '1:30 AM',
    '2:00 AM', '2:30 AM',
    '3:00 AM', '3:30 AM',
    '4:00 AM', '4:30 AM',
    '5:00 AM', '5:30 AM',
    '6:00 AM', '6:30 AM',
    '7:00 AM', '7:30 AM',
    '8:00 AM', '8:30 AM',
    '9:00 AM', '9:30 AM',
    '10:00 AM', '10:30 AM',
    '11:00 AM', '11:30 AM',
    '12:00 PM', '12:30 PM',
    '1:00 PM', '1:30 PM',
    '2:00 PM', '2:30 PM',
    '3:00 PM', '3:30 PM',
    '4:00 PM', '4:30 PM',
    '5:00 PM', '5:30 PM',
    '6:00 PM', '6:30 PM',
    '7:00 PM', '7:30 PM',
    '8:00 PM', '8:30 PM',
    '9:00 PM', '9:30 PM',
    '10:00 PM', '10:30 PM',
    '11:00 PM', '11:30 PM',
    '12:00 AM (Next day)', '12:30 AM (Next day)',
    '1:00 AM (Next day)', '1:30 AM (Next day)',
    '2:00 AM (Next day)', '2:30 AM (Next day)',
    '3:00 AM (Next day)', '3:30 AM (Next day)',
    '4:00 AM (Next day)', '4:30 AM (Next day)',
    '5:00 AM (Next day)', '5:30 AM (Next day)',
    '6:00 AM (Next day)', '6:30 AM (Next day)',
]


def time_to_number(ts: str, is_first: bool):
    global time_open_select
    global time_end_select

    if is_first:
        return time_open_select.index(ts)
    else:
        return time_end_select.index(ts)


def combine_data(collectors):
    businesses = read_csv('businesses')
    for i in np.arange(collectors):
        print('i: ', i)
        try:
            businesses_add = read_csv('businesses_' + str(i))
            indexes = businesses_add[(businesses_add['Loaded'] != 0) & (businesses['Loaded'] == 0)].index
            print('len: ', len(indexes))
            businesses.iloc[indexes] = businesses_add.iloc[indexes]
            print('len 2: ', len(businesses[businesses['Loaded'] == 2]))
        except:
            print('error!')
    save_csv(businesses, 'businesses')


# Tests
def first_unload_url():
    businesses = read_csv('businesses')
    indexes = businesses[businesses['Loaded'] == 0].index.tolist()
    print(len(indexes))
    if len(indexes) == 0:
        return None
    business_id = indexes[0]
    url = businesses.iloc[business_id]['Url']
    return url

# Entry Point

# collect_locations()

# collect_urls()

# remove_duplicates_urls()

# collect_pages()

# businesses.drop(index=businesses[businesses['Loaded'] != 1].index, inplace=True)