from pyspark.sql import DataFrame
from itertools import combinations

def get_primary_key(df: DataFrame, all=False):    
    rows = df.count()
    pk = []

    for x in range(1, len(df.columns)+1):
        for combination in combinations(df.columns, x):
            candidate = df.distinct(combination).count()
            if (candidate == rows):
                pk.append(column)
                if not all:
                    return pk



        
