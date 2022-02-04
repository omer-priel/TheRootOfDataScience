# Data Preprocess (Clean the data)

# System
import os

# Data
import numpy as np
import pandas as pd

# View
import matplotlib.pyplot as plt
import seaborn as sns
def reorganizeIndex():
    df=pd.read_csv('data\\businesses.csv')
    df[id]=df.index
    print(df[id].tail())
    df.to_csv('data\\businessestmp.csv')
#reorganizeIndex()