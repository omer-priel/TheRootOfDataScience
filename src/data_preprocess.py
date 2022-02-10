# Data Preprocess (Clean the data)

# System
import ast
from enum import unique
import os
import json

# Data
import numpy as np
import pandas as pd

# View
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import true


# Utilities Files
def read_csv(name: str, index_label='id') -> pd.DataFrame:
    return pd.read_csv('../data/' + name + '.csv', index_col=index_label)


def save_csv(df: pd.DataFrame, name: str, index_label='id'):
    df.to_csv('../data/' + name + '.csv', index_label=index_label)


# takes as input the api dataframe which includes a row of attributes and a row of names and the orignal data frame
# returns the original dataframe with a collumn for each attribute
# kinda slow
def addattr(apidf: pd.DataFrame, originaldf: pd.DataFrame):
    for index, row in apidf.iterrows():
        attributes = row["attributes"]
        if not (attributes == None):
            for attr in attributes:
                originaldf.loc[originaldf["Name"] == row["name"], attr] = attributes[attr]
    return originaldf


# Unused
def create_businesses_has_extra_data():
    # Load the Data
    df = read_csv('businesses')

    # Remove Duplicates
    df.drop_duplicates(subset=['Name'], inplace=True)

    # Remove extra columns
    df.drop(columns=['MenuCount', 'MenuStartsCount', 'MenuReviewsCount', 'MenuPhotosCount'], inplace=True)

    # Handle Attributes
    df.rename({'Attributes Has': 'AttributesHas'}, axis='columns', inplace=True)

    df['AttributesHas'] = df['AttributesHas'].astype(object)

    for i in df[df.isna()['AttributesHas']].index:
        df.at[i, 'AttributesHas'] = np.arange(0)
        df.at[i, 'AttributesHas'] = 0

    df['ExtraName'] = np.full(len(df.index), None)
    df['ExtraAttributes'] = np.full(len(df.index), None)

    df['ExtraName'] = df['ExtraName'].astype(str)
    df['ExtraAttributes'] = df['ExtraAttributes'].astype(str)
    for i in df.index:
        df.at[i, 'ExtraName'] = ' '
        df.at[i, 'ExtraAttributes'] = ' '

    print('Loading Extra Data')
    extra_data_arr = []

    f = open('../data/yelp_academic_dataset_business.json', encoding='utf8')
    line = f.readline()
    count = 0
    while line:
        count += 1
        try:
            full_extra_data = json.loads(line)
            extra_data = {
                'name': full_extra_data['name'],
                'attributes': full_extra_data['attributes'],
            }
            extra_data_arr.append(extra_data)
            line = f.readline()
        except Exception as ex:
            print(count, ex)

    f.close()

    print('Extra Data Loaded')

    count = 0
    for extra_data in extra_data_arr:
        indexes = df[df['Name'] == extra_data['name']].index
        if len(indexes) > 0:
            index = indexes[0]
            if df.at[index, 'ExtraName'] == ' ':
                df.at[index, 'ExtraName'] = extra_data['name']
                df.at[index, 'ExtraAttributes'] = json.dumps(extra_data['attributes'])

                count += 1
                if count % 250 == 0:
                    print(count)

    print(count)

    df.drop(df[df['ExtraName'] == ' '].index, inplace=True)

    df.index = np.arange(len(df.index))

    # Save
    save_csv(df, 'businesses_has_extra_data')


# find all unique subcategories kinda faster with string
def findUniqCat(df:pd.DataFrame):
    categories = df["SubCategories"].unique()
    tmp = []
    for i in range(len(categories)):
        tmp = tmp + categories[i]
    tmp = np.array(tmp)
    return np.unique(tmp)


# turn a column containing a string represantation of list into list
# gets name of column and a dataframe
def strToList(df: pd.DataFrame, name: str):
    for i in range(len(df[name])):
        string = df[name][i]
        df.at[i,name] = ast.literal_eval(string)

def isinlist(lis,obj):
    return obj in lis
def createSubCat(collumns:list):
    vectfunc= np.vectorize(isinlist)
    for subcat in collumns:
        df[subcat] = vectfunc(df['SubCategories'],subcat)


subcategories=findUniqCat()
createSubCat(subcategories)
df.drop(columns='',inplace=true)


# Entry Point

# Config view Settings
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)

# Load the Data
df = read_csv('businesses')

# Remove Duplicates
df.drop_duplicates(subset=['Name'], inplace=True)
df.index = np.arange(len(df.index))

# Remove extra columns
df.drop(columns=['MenuCount', 'MenuStartsCount', 'MenuReviewsCount', 'MenuPhotosCount'], inplace=True)

df.drop(columns=['Loaded'], inplace=True)

# Add Attributes Columns
df['At_Reservations'] = np.full(len(df.index), -1)
df['At_Delivery'] = np.full(len(df.index), -1)
df['At_Takeout'] = np.full(len(df.index), -1)

# Handle Attributes
df.rename({'Attributes Has': 'AttributesHas'}, axis='columns', inplace=True)

df['AttributesHas'] = df['AttributesHas'].astype(object)

for i in df[df.isna()['Attributes Has']].index:
    df.at[i, 'Attributes Has'] = np.arange(0)
    df.at[i, 'AttributesCount'] = 0

attributes_names_orignal = []
attributes_names = []

arrs = df.index
for i in arrs:
    df.at[i, 'AttributesHas'] = json.loads(df.at[i, 'AttributesHas'].replace('\'', '\"'))
    attributes_has = []

    for attributes_has_elem in df.at[i, 'AttributesHas']:
        if attributes_has_elem not in attributes_names_orignal:
            attributes_names_orignal.append(attributes_has_elem)

        for attributes_name in attributes_has_elem.split(','):
            attributes_has.append(attributes_name)
            if attributes_name not in attributes_names:
                attributes_names.append(attributes_name)

    attributes_has = list(set(attributes_has))
    df.at[i, 'AttributesHas'] = attributes_has
    df.at[i, 'AttributesCount'] = len(attributes_has)

    if 'Takes Reservations' in attributes_has:
        df.at[i, 'At_Reservations'] = 1
    elif 'No Reservations' in attributes_has:
        df.at[i, 'At_Reservations'] = 0

    if 'Offers Delivery' in attributes_has:
        df.at[i, 'At_Delivery'] = 1
    elif 'No Delivery' in attributes_has:
        df.at[i, 'At_Delivery'] = 0

    if 'Offers Takeout' in attributes_has:
        df.at[i, 'At_Takeout'] = 1
    elif 'No Takeout' in attributes_has:
        df.at[i, 'At_Takeout'] = 0

attributes_names_orignal = sorted(attributes_names_orignal)
attributes_names = sorted(attributes_names)

f = open('../data/attributes_names_orignal.txt', 'w', encoding='utf8')
for name in attributes_names_orignal:
    f.write(name + '\n')
f.close()

f = open('../data/attributes_names.txt', 'w', encoding='utf8')
for name in attributes_names:
    f.write(name + '\n')
f.close()

for name in attributes_names:
    df.insert(len(df.columns), 'ExtraAt_' + name, np.full(len(df.index), 0))

for i in df.index:
    attributes_has = df.at[i, 'AttributesHas']

    for name in attributes_names:
        if name in attributes_has:
            df.at[i, 'ExtraAt_' + name] = 1

arr = []
for name in attributes_names:
    arr.append('ExtraAt_' + name)

# df[arr].sum().sort_values()
# df[arr].sum()[df[arr].sum() >= 500].sort_values()

# Any attributes with les 500 samples is extraordinary
# So we need to consider only attributes with more the 500 samples

# TEMP
index = df[arr].sum()[df[arr].sum() >= 500].index

arr = []
for name in index:
    arr.append(name.replace('ExtraAt_', ''))

f = open('../data/attributes_names_importent.txt', 'w', encoding='utf8')
for name in arr:
    f.write(name + '\n')
f.close()

# Save
save_csv(df, 'businessestmp')
