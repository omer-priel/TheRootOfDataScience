# Data Preprocess (Clean the data)

# System
import os
import json
import ast

# Data
import numpy as np
import pandas as pd

# View
import matplotlib.pyplot as plt
import seaborn as sns


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


def change_column_location(df: pd.DataFrame, name: str, index: int):
    column_to_move = df.pop(name)

    df.insert(index, name, column_to_move)
    return df

# find all unique subcategories kinda faster with string
def find_uniq_cat(df: pd.DataFrame):
    categories = df["SubCategories"].unique()
    uniq_cat = []
    for category in categories:
        if type(category) == str:
            category = json.loads(category.replace('\'', '\"'))
        uniq_cat += category

    if '' in uniq_cat:
        uniq_cat.remove('')

    uniq_cat = np.array(uniq_cat)
    uniq_cat = np.unique(uniq_cat)
    return uniq_cat


# turn a column containing a string represantation of list into list
# gets name of column and a dataframe
def str_to_list(df: pd.DataFrame, name: str):
    for i in range(len(df[name])):
        string = df[name][i]
        df.at[i, name] = ast.literal_eval(string)


def is_in_list(lis, obj):
    return obj in lis


def convert_str_to_list(value):
    if type(value) == str:
        return json.loads(value.replace('\'', '\"'))
    return value

def create_sub_cat(df: pd.DataFrame):
    column_names = find_uniq_cat(df)

    column_full_names = []
    for sub_cat in column_names:
        column_full_names.append('Cat_' + sub_cat)

    new_df = pd.DataFrame(columns = column_full_names)
    vect_func = np.vectorize(is_in_list)

    for sub_cat in column_names:
        new_df['Cat_' + sub_cat] = vect_func(df['SubCategories'], sub_cat)

    df = pd.concat([df, new_df], axis=1)

    df['SubCategories'] = df['SubCategories'].apply(convert_str_to_list)

    vect_length = np.vectorize(len)
    df["SubCategoriesCount"] = vect_length(df['SubCategories'])
    
    return df


# Entry Point

# Config view Settings
pd.set_option('display.width', 1000)
pd.set_option('display.max_columns', 15)

# Load the Data
print('Load the Data')
df = read_csv('businesses')

# Remove Duplicates
df.drop_duplicates(subset=['Name'], inplace=True)
df.index = np.arange(len(df.index))

# Remove extra columns
df.drop(columns=['MenuCount', 'MenuStartsCount', 'MenuReviewsCount', 'MenuPhotosCount'], inplace=True)

for day in range(1, 8):
    df.drop(columns=['HasBreaks' + str(day)], inplace=True)

df.drop(columns=['Loaded', 'Category'], inplace=True)

# Add Attributes Columns
df['At_Reservations'] = np.full(len(df.index), -1)
df['At_Delivery'] = np.full(len(df.index), -1)
df['At_Takeout'] = np.full(len(df.index), -1)

df['At_HealthScoreHas'] = np.full(len(df.index), 0)
df['At_HealthScoreLevelNumber'] = np.full(len(df.index), -1)
df['At_HealthScoreLevelOther'] = np.full(len(df.index), -1)

health_score_levels_other = [
    'A',
    'B',
    'NA',
    'GN',
    'Green',
    'Compliant',
    'Estimated Health Score',
    'Excellent',
    'Fair',
    'Good',
    'In Compliance',
    'In Violation',
    'Inspected & Permitted',
    'Marginal',
    'Non-Compliance/Case Closed',
    'Not compliant',
    'Pass',
    'Unacceptable',
    'Undetermined'
]

other_importent_attributes = [
    'Accepts Credit Cards',
    'Accepts Debit Cards',
    'All staff fully vaccinated',
    'Good for Groups',
    'Many Vegetarian Options',
    'Masks required',
    'Outdoor Seating',
    'Staff wears masks',
    'Vegan Options'
]

for attribute in other_importent_attributes:
    df['At_' + attribute] = np.full(len(df.index), 0)

# Handle Attributes
df.rename({'Attributes Has': 'AttributesHas'}, axis='columns', inplace=True)

df['AttributesHas'] = df['AttributesHas'].astype(object)

for i in df[df.isna()['AttributesHas']].index:
    df.at[i, 'AttributesHas'] = np.arange(0)
    df.at[i, 'AttributesCount'] = 0

attributes_names_orignal = []
attributes_names = []

for i in df.index:
    if type(df.at[i, 'AttributesHas']) is str:
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

    if 'Health Score' in attributes_has or 'Estimated Health Score' in attributes_has:
        df.at[i, 'At_HealthScoreHas'] = 1

        for attribute in attributes_has:
            if attribute.find(' out of 100') != -1:
                df.at[i, 'At_HealthScoreLevelNumber'] = float(attribute.replace(' out of 100', ''))
                break
            elif attribute in health_score_levels_other:
                df.at[i, 'At_HealthScoreLevelOther'] = health_score_levels_other.index(attribute)
                break

    for attribute in other_importent_attributes:
        if attribute in attributes_has:
            df.at[i, 'At_' + attribute] = 1

attributes_names_orignal = sorted(attributes_names_orignal)
attributes_names = sorted(attributes_names)

f = open('../data/temp/attributes_names_orignal.txt', 'w', encoding='utf8')
for name in attributes_names_orignal:
    f.write(name + '\n')
f.close()

f = open('../data/temp/attributes_names.txt', 'w', encoding='utf8')
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

index = df[arr].sum()[df[arr].sum() >= 500].index

arr = []
for name in index:
    arr.append(name.replace('ExtraAt_', ''))

f = open('../data/temp/attributes_names_importent.txt', 'w', encoding='utf8')
for name in arr:
    f.write(name + '\n')
f.close()

# Handle subcategories
print('Handle subcategories')
df = create_sub_cat(df)

# Handle Time
names_open_end = []
names_count_hour = []
names_has_break = []
for day in range(1, 8):
    names_open_end.append('OpenHour' + str(day))
    names_open_end.append('EndHour' + str(day))
    names_count_hour.append('CountHour' + str(day))
    names_has_break.append('HasBreak' + str(day))

for name in names_open_end:
    df[name].fillna(-1, inplace=True)

for name in (names_count_hour + names_has_break):
    df[name].fillna(0, inplace=True)

# WeeklyHours WeeklyDays WeeklyBreaks
df.rename({'OpenDaysCount': 'WeeklyHours'}, axis='columns', inplace=True)
df.rename({'HasBreaks': 'WeeklyBreaks'}, axis='columns', inplace=True)

df['WeeklyDays'] = 0
df['WeeklyHours'] = df['CountHour1']
df['WeeklyBreaks'] = df['HasBreak1']

for day in range(2, 8):
    df['WeeklyDays'] = df['WeeklyDays'] + (df['CountHour' + str(day)] > 0)
    df['WeeklyHours'] = df['WeeklyHours'] + df['CountHour' + str(day)]
    df['WeeklyBreaks'] = df['WeeklyBreaks'] + df['HasBreak' + str(day)]


# Relocate columns
first_names = [
    'Name', 'Url',
    'ExpensiveLevel',
    'Claimed', 'HasWebsite',
    'Stars', 'Reviews', 'Photos',
    'SubCategoriesCount', 'AttributesCount',
    'QuestionsCount',
    'WeeklyHours', 'WeeklyBreaks', 'WeeklyDays',
]

for day in range(1, 8):
    first_names.append('OpenHour' + str(day))
    first_names.append('EndHour' + str(day))
    first_names.append('CountHour' + str(day))
    first_names.append('HasBreak' + str(day))

first_names = first_names + ['SubCategories', 'AttributesHas']

for index, name in enumerate(first_names):
    df = change_column_location(df, name, index)

# Change types
change_to_float = df.columns.to_list()
not_float = [
    'Name', 'Url',
    'SubCategories', 'AttributesHas'
]

for name in not_float:
    change_to_float.remove(name)

for name in change_to_float:
    df[name] = df[name].astype(float)

df.index = np.arange(len(df.index))

# Save
print('Saving')
save_csv(df, 'businesses')

print('Complit')
