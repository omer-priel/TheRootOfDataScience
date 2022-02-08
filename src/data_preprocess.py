# Data Preprocess (Clean the data)

# System
import os

# Data
import numpy as np
import pandas as pd

# View
import matplotlib.pyplot as plt
import seaborn as sns

def reorganize_index():
    df = pd.read_csv('../data/businesses.csv')
    df[id] = df.index
    df.to_csv('../data/businessestmp.csv')

# reorganize_index()