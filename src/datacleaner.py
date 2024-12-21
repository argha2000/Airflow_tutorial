def data_cleaner():
    import numpy as np
    import pandas as pd
    import re

    df = pd.read_csv("~/store_files_airflow/raw_store_transaction.csv")

    # The below inner function removes any special character from the store location column
    # as they are uneeded
    def clean_store_location(st_loc):
        return re.sub(r"[^\w\s]","",st_loc).strip()
    
    # Below iner function cleans the product id column by removing
    #any non integer values and keeping only integers
    def clean_product_id(pd_id):
        matches = re.findall(r"\d+",pd_id)

        if matches:
            return matches[0]
        return pd_id
    
    # this function replaces the dollar symbol and returns the value in numeric form
    def replace_dollar(amt):
        return float(amt.replace("$",""))
    
    df["STORE_LOCATION"] = df["STORE_LOCATION"].map(lambda x: clean_store_location(x))
    df["PRODUCT_ID"] = df["PRODCUT_ID"].map(lambda x: clean_product_id(x))

    for to_clean in ["MRP","CP","DISCOUNT","SP"]:
        df[to_clean] = df[to_clean].map(lambda x : replace_dollar(x))

    df.to_csv("~/store_files_airflow/clean_store_transaction",index=False)
