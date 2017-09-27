#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: PSChua
"""
#import essential library
import pandas as pd

#authorize client secret & open spreadsheet
import pygsheets
gc = pygsheets.authorize(service_file="client_secret.json")
sheet = gc.open_by_key("1FHBo_yCtn-suJTXk2okpiPA0y7e7-7ycJsF4-361_EE")

#obtain last 7 days' data into a dataframe
last_7_days = sheet.worksheet_by_title("LAST_7_DAYS")
last_7_df = last_7_days.get_as_df(has_header=False, numerize = True)
last_7_header = last_7_df.iloc[1]
last_7_df = last_7_df[2:]
last_7_df.columns = last_7_header

#obtain last 30 days' data into a dataframe
last_30_days = sheet.worksheet_by_title("LAST_30_DAYS")
last_30_df = last_30_days.get_as_df(has_header=False, numerize = True)
last_30_header = last_30_df.iloc[1]
last_30_df = last_30_df[2:]
last_30_df.columns = last_30_header

#replace potential wildcards that disrupt regex
last_7_df['Search term'] = last_7_df['Search term'].str.replace("\*","")

#generate a list of lists of tokens
from nltk.tokenize import word_tokenize
last_7_words = last_7_df["Search term"]
tokens_list = [word_tokenize(i) for i in last_7_words]

#flatten the tokens list of lists while de-duping
tokens = []
for row in tokens_list:
    for item in row:
        if item not in tokens:
            tokens.append(item)

term_metrics_header = ["Impr_last7","Clk_last7","Cost_last7","Conv_last7","Impr_last30","Clk_last30","Cost_last30","Conv_last30"]
#calculate the metrics for the tokens
filtered_data = []     
for token in tokens:
    filtered_last7 = last_7_df[last_7_df["Search term"].str.contains(r"\b" + token + r"\b")]
    filtered_last30 = last_30_df[last_30_df["Search term"].str.contains(r"\b" + token + r"\b")]
    filtered_data.append([filtered_last7["Impressions"].sum(),filtered_last7["Clicks"].sum(),filtered_last7["Cost"].sum(),filtered_last7["Conversions"].sum(),filtered_last30["Impressions"].sum()/30*7,filtered_last30["Clicks"].sum()/30*7,filtered_last30["Cost"].sum()/30*7,filtered_last30["Conversions"].sum()/30*7])

#create dataframe to push back
consolidated = pd.DataFrame(data = filtered_data, columns = term_metrics_header)
consolidated.insert(0,"Terms",tokens)

#write to spreadsheet
py_token = sheet.worksheet_by_title("temp")
py_token.set_dataframe(consolidated, (1,1))