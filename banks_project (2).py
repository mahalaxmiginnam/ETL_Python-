import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import requests

def log_progress(message):

    now = datetime.now()
    formatted_time = now.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{formatted_time} - {message}")
 
def extract(url, table_attribs):

    html_page = requests.get(url).text  # Extract the webpage as text
    data = BeautifulSoup(html_page, 'html.parser') # Parse the text into an HTML object
    df = pd.DataFrame(columns=table_attribs) # Create a pandas dataframe with table attributes as columns

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows: 
        columns = row.find_all('td')

        if len(columns) != 0:
            #name = str(columns[1].contents[0])  # Extract the second column
            #mc_usd_billion = columns[2].contents[0]  # Extract the third column
            name = columns[1].get_text().strip()
            mc_usd_billion = columns[2].get_text().strip()
            try:
                mc_usd_billion_float = float(mc_usd_billion)
                data_dict = {"Name": name, "MC_USD_Billion": mc_usd_billion_float}
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
            except ValueError:
                # Handle the case where mc_usd_billion cannot be converted to a float
                print(f"Skipping row with invalid MC_USD_Billion: {mc_usd_billion}")

    return df

def transform(df, csv_path):
    
    exchange_rate = pd.read_csv(exchange_rate_file_path, delimiter=',')
    print(exchange_rate)
    exchange_rate_dict = exchange_rate.set_index('Currency')['Rate'].to_dict()
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate_dict['GBP'], 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate_dict['EUR'], 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * exchange_rate_dict['INR'], 2)

    return df

def load_to_csv(df, output_path):
    df.to_csv('final.csv', index=False)

def load_to_db(df, sql_connection, table_name):
    
    df.to_sql(table_name, sql_connection, index=False, if_exists='replace')


def run_query(query_statement, sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)
    
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
table_attribs = ['Name', 'MC_USD_Billion'] 
db_name = 'Banks.db'
table_name = 'Largest_banks'
exchange_rate_csv_path = './exchange_rate.csv'
csv_path = './largest_banks_data.csv'  


log_progress('Preliminaries complete. Initiating ETL process')

df = extract(url, table_attribs)

log_progress('Data extraction complete. Initiating Transformation process')

exchange_rate_file_path ='./exchange_rate.csv'
df = transform(df,exchange_rate_file_path)
log_progress('Data transformation complete. Initiating loading process')

load_to_csv(df, csv_path)

log_progress('Data saved to CSV file')

table_name = 'Largest_banks'
sql_connection =  sqlite3.connect('Banks.db')

log_progress('SQL Connection initiated.')


load_to_db(df, sql_connection, table_name)
log_progress('Data loaded to Database as table.')

# Print the contents of the entire table
query_statement = f"SELECT * FROM {table_name}"
run_query(query_statement, sql_connection)

# Print the average market capitalization of all the banks in Billion USD.
query_statement = f"SELECT AVG(MC_GBP_Billion) FROM {table_name}"
run_query(query_statement, sql_connection)

# Print only the names of the top 5 banks
query_statement = f"SELECT Name from {table_name} LIMIT 5"
run_query(query_statement, sql_connection)

log_progress('Process Complete')

sql_connection.close()



'''
theia@theia-jhklaxmi:/home/project$ python3.11 banks_project.py
2023-11-27 05:35:24 - Preliminaries complete. Initiating ETL process
/home/project/banks_project.py:35: FutureWarning: The behavior of DataFrame concatenation with empty or all-NA entries is deprecated. In a future version, this will no longer exclude empty or all-NA columns when determining the result dtypes. To retain the old behavior, exclude the relevant entries before the concat operation.
  df = pd.concat([df, df1], ignore_index=True)
2023-11-27 05:35:27 - Data extraction complete. Initiating Transformation process
  Currency   Rate
0      EUR   0.93
1      GBP   0.80
2      INR  82.95
2023-11-27 05:35:27 - Data transformation complete. Initiating loading process
2023-11-27 05:35:27 - Data saved to CSV file
2023-11-27 05:35:27 - SQL Connection initiated.
2023-11-27 05:35:27 - Data loaded to Database as table.
SELECT * FROM Largest_banks
                                      Name  ...  MC_INR_Billion
0                           JPMorgan Chase  ...        35910.71
1                          Bank of America  ...        19204.58
2  Industrial and Commercial Bank of China  ...        16138.75
3               Agricultural Bank of China  ...        13328.41
4                                HDFC Bank  ...        13098.63
5                              Wells Fargo  ...        12929.42
6                        HSBC Holdings PLC  ...        12351.26
7                           Morgan Stanley  ...        11681.85
8                  China Construction Bank  ...        11598.07
9                            Bank of China  ...        11348.39

[10 rows x 5 columns]
SELECT AVG(MC_GBP_Billion) FROM Largest_banks
   AVG(MC_GBP_Billion)
0              151.987
SELECT Name from Largest_banks LIMIT 5
                                      Name
0                           JPMorgan Chase
1                          Bank of America
2  Industrial and Commercial Bank of China
3               Agricultural Bank of China
4                                HDFC Bank
2023-11-27 05:35:27 - Process Complete

'''
