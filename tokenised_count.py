import gspread 
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from oauth2client.service_account import ServiceAccountCredentials
#import nltk
#nltk.download()
from nltk.tokenize import word_tokenize
import pandas as pd

scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

sheet = client.open_by_key("1FHBo_yCtn-suJTXk2okpiPA0y7e7-7ycJsF4-361_EE")

#testing faster dataframe
last_7_df_test = get_as_dataframe(worksheet = last_7_days, skiprows=1, header=True)

#obtain data from the spreadsheet
last_7_days = sheet.worksheet("LAST_7_DAYS")
last_30_days = sheet.worksheet("LAST_30_DAYS")
py_token = sheet.worksheet("Py_Token")
last_7 = last_7_days.get_all_values()
last_30 = last_30_days.get_all_values()

#convert the data into a dataframe
last_7_values_header = last_7[1][:]
last_7_values = last_7[2:len(last_7)-1][:]
last_7_df = pd.DataFrame(last_7_values,columns = last_7_values_header)
metrics_header = ["Impressions","Clicks","Cost","Conversions"]
last_7_df[metrics_header] = last_7_df[metrics_header].astype(float)

#convert the data into a dataframe
last_30_values_header = last_30[1][:]
last_30_values = last_30[2:len(last_7)-1][:]
last_30_df = pd.DataFrame(last_30_values,columns = last_30_values_header)
last_30_df[metrics_header] = last_30_df[metrics_header].astype(float)

#replace potential wildcards that disrupt regex
last_7_df['Search term'] = last_7_df['Search term'].str.replace("\*","")

#generate a list of lists of tokens
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
set_with_dataframe(worksheet = py_token, dataframe = consolidated)