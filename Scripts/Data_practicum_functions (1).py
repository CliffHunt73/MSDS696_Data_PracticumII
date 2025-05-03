
#######Code written by Cliff Hunt for MSDS692 Summer 2024##############
########### Import libraries #################################################################

import pandas as pd
import requests
from time import sleep
import certifi
import time
import re
import ssl
from urllib.request import urlopen
import json


#########Function Definitions ##################################################################

####This function is used in the merging process and accomodates the various formats used in different 
####years of the CDP data sheets
def get_summary_data_excel2(file_path, year, API_KEY):
    if year != 2023:
        try:

            # Read the Excel file and check if 'Summary Data' sheet exists
            xls = pd.ExcelFile(file_path)
            if 'Summary Data' in xls.sheet_names:
                # Read the 'Summary Data' worksheet into a DataFrame
                print(f"reading in excel worksheet...{file_path}")
                
                Summary_Data_df = pd.read_excel(file_path, sheet_name='Summary Data')


                def generate_symbol(ticker):

                    suffix_mapping = {
                                'US': '',
                                'TT': '.TW',
                                'SM': '.MC',
                                'IR': '.CO',
                                'IN': '.BO',
                                'IJ': '.JK',
                                'ID': '.L',
                                'IT': '.TA',
                                'IM': '.MI',
                                'JP': '.T',
                                'LX': '.AS',
                                'MK': '.KL',
                                'LN': '.L',
                                'NA': '.AS',
                                'NZ': '.NZ',
                                'DW': '.WA',
                                'PL': '.LS',
                                'QD': '.QA',
                                'BM': '.ME',
                                'SJ': '.JO',
                                'SM': '.MC',
                                'SS': '.ST',
                                'TB': '.BK',
                                'TI': '.IS',
                                'HK': '.HK',
                                'FH': '.HE',
                                'DC': '.CO',
                                'CK': '.PR',
                                'NO': '.OL',
                                'CI': '.SN',
                                'BB': '.BR',
                                'FP': '.PA',
                                'CN': '.TO',
                                'GA': '.AT',
                                'KS': '.KS',
                                'BS': '.SA',
                                'CH': '.SZ',
                                'GR': '.DE',
                                'VX': '.SW',
                                'SW': '.SW',
                                'LN': '.L',
                                'AU': '.AX',
                                'BZ': '.SA',
                                'MM': '.MX',

                        }

                    swap_ticker_mapping = {
                            'ATCOA SS': 'ATCO-A.ST',
                            'AV/ LN' : 'AV.L',
                            'BA/ LN': 'BA.L',
                            'BP/ LN': 'BP.L',
                            'BXB AU': 'BXB.AX',
                            'BF/B US': 'BF-B',
                            'CCL/A CN': 'CCL-A.TO',
                            'NG/ LN': 'NG.L',
                            'QQ/ LN': 'QQ.L',
                            'RB/ LN': 'RKT.L',
                            'RCI/B CN': 'RCI-B.TO',
                            'RR/ LN': 'RR.L',
                            'SN/ LN': 'SN.L',
                            'TW/ LN': 'TW.L',
                            'TCL/A CN': 'TCL-A.TO',
                            'UU/ LN': 'UU.L',
                            'TECK/A CN': 'TECK-A.TO',
                            'WG/ LN': 'WG.L',
                            'BAM/A CN': 'BAM-A.TO',
                            'BF/B US': 'BF-A',
                            'BT/A LN': 'BT-A.L',
                            'PE&OLES* MM': 'PE&OLES.MX',
                            'BEI-U CN': 'BEI-UN.TO',
                            'CARLB DC': 'CARL-B.CO',
                            '293 HK': '0293.HK',
                            '144 HK': '0144.HK',
                            '386 HK': '0386.HK',
                            '762 HK': '0762.HK',
                            '2 HK': '0002.HK',
                            '11 HK': '0011.HK',
                            '6 HK': '0006.HK',
                            '44 HK': '.0044HK',
                            '823 HK': '0823.HK',
                            '66 HK': '0066.HK',
                            '857 HK': '0857.HK',
                            '69 HK': '0069.HK',
                            '19 HK': '0019.HK',
                            '992 HK': '0992.HK',
                            '8089Z US': 'LEVI',
                            '104230 KF': '229,640.KS',
                            'MTELEKOM HB': 'MOL.BD',
                            '600383 CH': '600383.SS',
                            '601186 CH': '601186.SS',
                            '6188 TT': '6188.TWO',
                            'EKTAB SS': 'EKTA-B.ST',
                            'ERICB SS': 'ERIC-B.ST',
                            'FUM1V FH': 'FORTUM.HE',
                            'FI/N SW': 'GF.SW',
                            'GETIB SS': 'GETI-B.ST',
                            'HOLMB SS': 'HOLM-B.ST',



                        }
                    if pd.isna(ticker):
                        return 'unknown'
                    else:
                        ticker = ticker.strip()  # Trim any leading or trailing whitespace

                        if ticker in swap_ticker_mapping:
                            ticker = swap_ticker_mapping[ticker]
                            return ticker

                        ticker_parts = ticker.split(' ')


                    # Check the suffix mapping dictionary
                    for suffix, replacement in suffix_mapping.items():
                        if ticker.endswith(' ' + suffix):
                            return ticker_parts[0] + replacement


                    return ticker_parts[0]




                if 'ticker' in Summary_Data_df.columns.str.lower():
                    Summary_Data_df['Symbol_1'] = Summary_Data_df['ticker'].apply(generate_symbol)
                elif 'tickers' in Summary_Data_df.columns.str.lower():
                    Summary_Data_df['Symbol_1'] = Summary_Data_df['Tickers'].apply(generate_symbol)
                else:
                    raise ValueError("Neither 'ticker' nor 'Tickers' column found in DataFrame.")

                #Summary_Data_df['Symbol_1'] = Summary_Data_df['ticker'].apply(generate_symbol)

                return Summary_Data_df
            else:
                print("The 'Summary Data' worksheet does not exist in the provided Excel file.")
                return None
        except FileNotFoundError:
            print(f"The file at {file_path} was not found.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None
    else: #2023 data stuff
        try:
            
            #read the premade data dictionary file with ticker symbols from previous years as last resort
            csv_path = r"C:\Users\cliff\OneDrive\Documents\Data Practicum1\CDP_Data\Corporate_Data\data_dict.csv"

            ticker_id_df = pd.read_csv(csv_path, dtype={'account_id': int})
            ticker_dict = ticker_id_df.set_index('account_id')['Symbol_1'].to_dict()
            
            
            # Read the Excel file and check if 'Summary Data' sheet exists
            xls = pd.ExcelFile(file_path)
            if 'C0.8' in xls.sheet_names:
                # Read the 'Summary Data' worksheet into a DataFrame
                print(f"reading in excel worksheet...{file_path}")
                Summary_Data_df = pd.read_excel(file_path, sheet_name='C0.8', skiprows=1)
                
                #changing the column names to make them more manageable in the code
                Summary_Data_df = Summary_Data_df.rename(columns={'C0.8_C1_Does your organization have an ISIN code or another unique identifier (e.g., Ticker, CUSIP, etc.)? - Indicate whether you are able to provide a unique identifier for your organization': 'Symbol_Type'})
                Summary_Data_df = Summary_Data_df.rename(columns={'C0.8_C2_Does your organization have an ISIN code or another unique identifier (e.g., Ticker, CUSIP, etc.)? - Provide your unique identifier': 'ticker'})
                total_rows = len(Summary_Data_df)
                Summary_Data_df = Summary_Data_df#.head(50)
                request_counter = 0
                start_time = time.time()
                
                def get_jsonparsed_data(url):
                    context = ssl.create_default_context(cafile=certifi.where())
                    response = urlopen(url, context=context)
                    data = response.read().decode("utf-8")
                    return json.loads(data)
                
                pattern = re.compile(r'[:() ]')
                
                def make_api_request(url, request_counter, start_time):
                    #global request_counter, start_time
    
                    try:
                        # Check if the request limit is reached
                        if request_counter >= 300:
                            elapsed_time = time.time() - start_time
                            if elapsed_time < 60:
                                time.sleep(60 - elapsed_time)  # Wait for the remaining time to complete 1 minute
                            # Reset the counter and start time
                            request_counter = 0
                            start_time = time.time()

                        json_data = get_jsonparsed_data(url)
                        request_counter += 1

                        if json_data and 'symbol' in json_data[0]:
                            return json_data[0]['symbol']
                        else:
                            return 'Not found'

                    except Exception as e:
                        return f'Error: {str(e)}'
                
                for index, row in Summary_Data_df.iterrows():
                    if isinstance(row['ticker'], str):
                        row['ticker'] = row['ticker'].strip()
                        
                    json_data = None
                    
                    if row['Symbol_Type'] == 'No':
                        Summary_Data_df.at[index, 'Symbol_1'] = 'unknown'
                        print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                        
                    elif row['Symbol_Type'] == 'Yes, a Ticker symbol' and isinstance(row['ticker'], str):
                        # if not pattern.search(row['ticker']):
                        #     Summary_Data_df.at[index, 'Symbol_1'] = row['ticker']
                        if 'SWX: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.SW'
                        elif 'NYSE: ' in row['ticker'] or 'NASDAQ: ' in row['ticker'] or 'Nasdaq: ' in row['ticker'] or 'Class A: ' in row['ticker']  or 'Ticker symbol: ' in row['ticker'] or 'Ticker: ' in row['ticker'] or 'stock code: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1]
                            print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                        elif 'KRX: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.KS'
                        elif 'HKEX: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.HK'
                        elif 'LON: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.L'
                        elif  'B3: ' in row['ticker'] or 'BMFV: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.SA'
                        elif  'Stock Code: ' in row['ticker'] and '.SH' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1]
                        elif  'TSE: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.TO'
                        elif  'TWSE: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.TW'
                        elif  'ASX: ' in row['ticker']:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker'].split(': ')[1] + '.AX'
                        else:
                            Summary_Data_df.at[index, 'Symbol_1'] = row['ticker']
                            print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                        
                    elif row['Symbol_Type'] == 'Yes, an ISIN code' and isinstance(row['ticker'], str):
                        ticker = row['ticker'].strip().replace(' ', '')
                        ticker = row['ticker'][:12]
                        url = f"https://financialmodelingprep.com/api/v4/search/isin?isin={ticker}&apikey={API_KEY}"
                        Summary_Data_df.at[index, 'Symbol_1'] = make_api_request(url, request_counter, start_time)
                        print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                        
                    elif row['Symbol_Type'] == 'Yes, a CUSIP number' and isinstance(row['ticker'], str):
                        ticker = row['ticker'].strip()
                        url = f"https://financialmodelingprep.com/api/v3/cusip/{ticker}?apikey={API_KEY}"
                        Summary_Data_df.at[index, 'Symbol_1'] = make_api_request(url, request_counter, start_time)
                        print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                    
                    
                    else:
                        try:
                            Summary_Data_df.at[index, 'Symbol_1'] = ticker_dict[row['account_id']]
                            print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                        except KeyError:
                            Summary_Data_df.at[index, 'Symbol_1'] = 'Not found'
                            # Print the progress
                            print(f"\rProcessing {index + 1}/{total_rows}: Ticker = {ticker}", end='')
                
                
                # Print the progress
                #print(f"\rSecond pass at dictionary for failed data pulls", end='')
                for index, row in Summary_Data_df.iterrows():
                    if pd.isnull(row['Symbol_1']) or row['Symbol_1'].lower() == 'not found' or 'Error: URL can' in row['Symbol_1'] or row['Symbol_1'] == 'unknown':
                        try:
                            symbol = ticker_dict.get(row['Account number'])
                            if symbol is not None:
                                Summary_Data_df.at[index, 'Symbol_1'] = symbol
                            else:
                                Summary_Data_df.at[index, 'Symbol_1'] = 'Not found'
                        except KeyError:
                            Summary_Data_df.at[index, 'Symbol_1'] = 'Not found'
                            
                        
                    #print(f'Processing row {index + 1}/{total_rows}', end='\r')
                #print(Summary_Data_df)
                
                return Summary_Data_df
            else:
                print("The 'Summary Data' worksheet does not exist in the provided Excel file.")
                return None
        except FileNotFoundError:
            print(f"The file at {file_path} was not found.")
            return None
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

###This function was the original used to accomodate various formats in the merging process
def get_summary_data_excel(file_path, year):
    
    try:
        
        # Read the Excel file and check if 'Summary Data' sheet exists
        xls = pd.ExcelFile(file_path)
        if 'Summary Data' in xls.sheet_names:
            # Read the 'Summary Data' worksheet into a DataFrame
            print(f"reading in excel worksheet...{file_path}")
            Summary_Data_df = pd.read_excel(file_path, sheet_name='Summary Data')
            
            
            def generate_symbol(ticker):
                
                suffix_mapping = {
                            'US': '',
                            'TT': '.TW',
                            'SM': '.MC',
                            'IR': '.CO',
                            'IN': '.BO',
                            'IJ': '.JK',
                            'ID': '.L',
                            'IT': '.TA',
                            'IM': '.MI',
                            'JP': '.T',
                            'LX': '.AS',
                            'MK': '.KL',
                            'LN': '.L',
                            'NA': '.AS',
                            'NZ': '.NZ',
                            'DW': '.WA',
                            'PL': '.LS',
                            'QD': '.QA',
                            'BM': '.ME',
                            'SJ': '.JO',
                            'SM': '.MC',
                            'SS': '.ST',
                            'TB': '.BK',
                            'TI': '.IS',
                            'HK': '.HK',
                            'FH': '.HE',
                            'DC': '.CO',
                            'CK': '.PR',
                            'NO': '.OL',
                            'CI': '.SN',
                            'BB': '.BR',
                            'FP': '.PA',
                            'CN': '.TO',
                            'GA': '.AT',
                            'KS': '.KS',
                            'BS': '.SA',
                            'CH': '.SZ',
                            'GR': '.DE',
                            'VX': '.SW',
                            'SW': '.SW',
                            'LN': '.L',
                            'AU': '.AX',
                            'BZ': '.SA',
                            'MM': '.MX',
                    
                    }
                
                swap_ticker_mapping = {
                        'ATCOA SS': 'ATCO-A.ST',
                        'AV/ LN' : 'AV.L',
                        'BA/ LN': 'BA.L',
                        'BP/ LN': 'BP.L',
                        'BXB AU': 'BXB.AX',
                        'BF/B US': 'BF-B',
                        'CCL/A CN': 'CCL-A.TO',
                        'NG/ LN': 'NG.L',
                        'QQ/ LN': 'QQ.L',
                        'RB/ LN': 'RKT.L',
                        'RCI/B CN': 'RCI-B.TO',
                        'RR/ LN': 'RR.L',
                        'SN/ LN': 'SN.L',
                        'TW/ LN': 'TW.L',
                        'TCL/A CN': 'TCL-A.TO',
                        'UU/ LN': 'UU.L',
                        'TECK/A CN': 'TECK-A.TO',
                        'WG/ LN': 'WG.L',
                        'BAM/A CN': 'BAM-A.TO',
                        'BF/B US': 'BF-A',
                        'BT/A LN': 'BT-A.L',
                        'PE&OLES* MM': 'PE&OLES.MX',
                        'BEI-U CN': 'BEI-UN.TO',
                        'CARLB DC': 'CARL-B.CO',
                        '293 HK': '0293.HK',
                        '144 HK': '0144.HK',
                        '386 HK': '0386.HK',
                        '762 HK': '0762.HK',
                        '2 HK': '0002.HK',
                        '11 HK': '0011.HK',
                        '6 HK': '0006.HK',
                        '44 HK': '.0044HK',
                        '823 HK': '0823.HK',
                        '66 HK': '0066.HK',
                        '857 HK': '0857.HK',
                        '69 HK': '0069.HK',
                        '19 HK': '0019.HK',
                        '992 HK': '0992.HK',
                        '8089Z US': 'LEVI',
                        '104230 KF': '229,640.KS',
                        'MTELEKOM HB': 'MOL.BD',
                        '600383 CH': '600383.SS',
                        '601186 CH': '601186.SS',
                        '6188 TT': '6188.TWO',
                        'EKTAB SS': 'EKTA-B.ST',
                        'ERICB SS': 'ERIC-B.ST',
                        'FUM1V FH': 'FORTUM.HE',
                        'FI/N SW': 'GF.SW',
                        'GETIB SS': 'GETI-B.ST',
                        'HOLMB SS': 'HOLM-B.ST',
                        
                        
                        
                    }
                if pd.isna(ticker):
                    return 'unknown'
                else:
                    ticker = ticker.strip()  # Trim any leading or trailing whitespace
                    
                    if ticker in swap_ticker_mapping:
                        ticker = swap_ticker_mapping[ticker]
                        return ticker
                    
                    ticker_parts = ticker.split(' ')
        
        
                # Check the suffix mapping dictionary
                for suffix, replacement in suffix_mapping.items():
                    if ticker.endswith(' ' + suffix):
                        return ticker_parts[0] + replacement
            
            
                return ticker_parts[0]
                    
                    
                    
            
            if 'ticker' in Summary_Data_df.columns.str.lower():
                Summary_Data_df['Symbol_1'] = Summary_Data_df['ticker'].apply(generate_symbol)
            elif 'tickers' in Summary_Data_df.columns.str.lower():
                Summary_Data_df['Symbol_1'] = Summary_Data_df['Tickers'].apply(generate_symbol)
            else:
                raise ValueError("Neither 'ticker' nor 'Tickers' column found in DataFrame.")

            #Summary_Data_df['Symbol_1'] = Summary_Data_df['ticker'].apply(generate_symbol)
            
            return Summary_Data_df
        else:
            print("The 'Summary Data' worksheet does not exist in the provided Excel file.")
            return None
    except FileNotFoundError:
        print(f"The file at {file_path} was not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    

####This function gets the employee count history from the financialmodeling prep website
####code written using financial modeling prep reference code as a starting point 
####as well as reference taken from https://stackoverflow.com/questions/68186451/what-is-the-proper-way-of-using-python-requests-requests-requestget-o
####All APIs in this project use a fixed window method to limit the number of API calls
####to 300 per minute.  Reference: https://itsrorymurphy.medium.com/api-rate-limits-a-beginners-guide-7f27cb3975cb
def get_employee_count(Summary_df, year_in, API_Key_in):
    tickers = Summary_df['Symbol_1'].tolist()
    year = year_in
    API_KEY = API_Key_in

    # Define maximum API calls per minute
    MAX_CALLS_PER_MINUTE = 300
    SECONDS_PER_MINUTE = 60

    # Function to get JSON data with error handling
    def get_jsonparsed_data(url):
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                return data
            else:
                print(f"Error retrieving data for {url.split('/')[-1]} ({response.status_code})", end="\r")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return []

    # Initialize the new column
    Summary_df['Employee_Count'] = None

    calls_made = 0  # Track API calls made

    for i, ticker in enumerate(tickers):
        if ticker == 'unknown':
            # Set Employee_Count to None for 'unknown' tickers
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Employee_Count'] = None
            print(f"Skipping ticker {i+1}/{len(tickers)}: 'unknown'", end='\r')
        elif Summary_df['Employee_Count'] == None:
            # Construct URL
            url = f"https://financialmodelingprep.com/api/v4/historical/employee_count?symbol={ticker}&apikey={API_KEY}"

            # Call the function and handle data
            data = get_jsonparsed_data(url)
            #print(data)
            # Extract employee count for the specified year
            employee_count = None
            for record in data:
                if 'periodOfReport' in record and record['periodOfReport'].startswith(str(year)):
                    employee_count = record.get('employeeCount', None)
                    break

            # Update the DataFrame with the retrieved employee count
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Employee_Count'] = employee_count

            # Print progress and wait if call limit reached
            calls_made += 1
            print(f"Processing ticker for {year} Employee Count data {i+1}/{len(tickers)} ({calls_made} calls made)                                                    ", end='\r')  # Overwrite previous line
            if calls_made >= MAX_CALLS_PER_MINUTE:
                sleep(SECONDS_PER_MINUTE / MAX_CALLS_PER_MINUTE)  # Wait for a minute before next call
                calls_made = 0  # Reset counter

    # Print completion message
    print(f"\nSummary data retrieval complete. Retrieved data for {len(tickers)} tickers.")
    
    return Summary_df







def get_market_cap_for_year(Summary_df, year_in, API_Key_in):
    tickers = Summary_df['Symbol_1'].tolist()
    year = year_in
    API_KEY = API_Key_in

    # Define maximum API calls per minute
    MAX_CALLS_PER_MINUTE = 300
    SECONDS_PER_MINUTE = 60

    # Function to get JSON data with error handling
    def get_jsonparsed_data(url):
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                # Check if data is a list with at least one element
                if isinstance(data, list) and len(data) > 0:
                    return data[0]  # Assuming the first element is the dictionary
                else:
                    print(f"API response for {url.split('/')[-1]} is empty", end='\r')
                    return {"symbol": url.split('/')[-1], "marketCap": None}  # Placeholder for empty responses
            else:
                print(f"Error retrieving data for {url.split('/')[-1]} ({response.status_code})", end="\r")
                return {"symbol": url.split('/')[-1], "marketCap": None}
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return {"symbol": url.split('/')[-1], "marketCap": None}

    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    Summary_df = Summary_df.copy()

    # Initialize the new columns
    Summary_df['Year'] = year
    Summary_df['Market_Cap'] = None

    calls_made = 0  # Track API calls made

    for i, ticker in enumerate(tickers):
        if ticker == 'unknown':
            # Set Market_Cap to None for 'unknown' tickers
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Market_Cap'] = None
            print(f"Skipping ticker {i+1}/{len(tickers)}: 'unknown'", end='\r')
        elif pd.isna(Summary_df['Market_Cap']).any():
            # Construct URL
            url = f"https://financialmodelingprep.com/api/v3/historical-market-capitalization/{ticker}?limit=100&from={year}-01-01&to={year}-12-31&apikey={API_KEY}"

            # Call the function and handle data
            data = get_jsonparsed_data(url)
            
            # Extract market cap if data retrieval was successful
            market_cap = data.get("marketCap", None)
            
            # Update the DataFrame with the retrieved market cap
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Market_Cap'] = market_cap

            # Print progress and wait if call limit reached
            calls_made += 1
            print(f"Processing ticker for {year} Market Cap data {i+1}/{len(tickers)} ({calls_made} calls made)                                                    ", end='\r')  # Overwrite previous line
            if calls_made >= MAX_CALLS_PER_MINUTE:
                sleep(SECONDS_PER_MINUTE / MAX_CALLS_PER_MINUTE)  # Wait for a minute before next call
                calls_made = 0  # Reset counter

    # Print completion message
    print(f"\nMarket Cap data retrieval complete. Retrieved data for {len(tickers)} tickers.")
    return Summary_df


def get_employee_count_for_year(Summary_df, year_in, API_Key_in):
    tickers = Summary_df['Symbol_1'].tolist()
    year = year_in
    API_KEY = API_Key_in

    # Define maximum API calls per minute
    MAX_CALLS_PER_MINUTE = 300
    SECONDS_PER_MINUTE = 60

    # Function to get JSON data with error handling
    def get_jsonparsed_data(url):
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                return data
            else:
                print(f"Error retrieving data for {url.split('/')[-1]} ({response.status_code})", end="\r")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return []

    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    Summary_df = Summary_df.copy()

    # Initialize the new column
    Summary_df['Employee_Count'] = None

    calls_made = 0  # Track API calls made

    for i, ticker in enumerate(tickers):
        if ticker == 'unknown':
            # Set Employee_Count to None for 'unknown' tickers
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Employee_Count'] = None
            print(f"Skipping ticker {i+1}/{len(tickers)}: 'unknown'", end='\r')
        elif pd.isna(Summary_df['Employee_Count']).any():
            # Construct URL
            url = f"https://financialmodelingprep.com/api/v4/historical/employee_count?symbol={ticker}&apikey={API_KEY}"

            # Call the function and handle data
            data = get_jsonparsed_data(url)
            # Extract employee count for the specified year
            employee_count = None
            for record in data:
                if 'periodOfReport' in record and record['periodOfReport'].startswith(str(year)):
                    employee_count = record.get('employeeCount', None)
                    break

            # Update the DataFrame with the retrieved employee count
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Employee_Count'] = employee_count

            # Print progress and wait if call limit reached
            calls_made += 1
            print(f"Processing ticker for {year} Employee Count data {i+1}/{len(tickers)} ({calls_made} calls made)                                                    ", end='\r')  # Overwrite previous line
            if calls_made >= MAX_CALLS_PER_MINUTE:
                sleep(SECONDS_PER_MINUTE / MAX_CALLS_PER_MINUTE)  # Wait for a minute before next call
                calls_made = 0  # Reset counter

    # Print completion message
    print(f"\nEmployee Count data retrieval complete. Retrieved data for {len(tickers)} tickers.")
    
    return Summary_df

def get_income_statement_for_year(Summary_df, year_in, API_Key_in):
    tickers = Summary_df['Symbol_1'].tolist()
    year = year_in
    API_KEY = API_Key_in

    # Define maximum API calls per minute
    MAX_CALLS_PER_MINUTE = 300
    SECONDS_PER_MINUTE = 60

    # Function to get JSON data with error handling
    def get_jsonparsed_data(url):
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                return data
            else:
                print(f"Error retrieving data for {url.split('/')[-1]} ({response.status_code})", end="\r")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return []

    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    Summary_df = Summary_df.copy()

    # Initialize the new column
    Summary_df['ebitda'] = None
    Summary_df['Revenue'] = None
    Summary_df['grossProfit'] = None
    Summary_df['netIncome'] = None
    Summary_df['reportedCurrency'] = None

    calls_made = 0  # Track API calls made

    for i, ticker in enumerate(tickers):
        if ticker == 'unknown':
            # Set financial income data to None for 'unknown' tickers
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'ebitda'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Revenue'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'grossProfit'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'netIncome'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'reportedCurrency'] = None
            print(f"Skipping ticker {i+1}/{len(tickers)}: 'unknown'", end='\r')
        elif (pd.isna(Summary_df[['ebitda', 'Revenue', 'grossProfit', 'netIncome', 'reportedCurrency']])).all(axis=1).any():
            # Construct URL
            url = f"https://financialmodelingprep.com/api/v3/income-statement/{ticker}?period=annual&apikey={API_KEY}"

            # Call the function and handle data
            data = get_jsonparsed_data(url)
            # Extract needed data for the specified year
            ebitda = None
            Revenue = None
            grossProfit = None
            netIncome = None
            reportedCurrency = None
            
            for record in data:
                if 'calendarYear' in record and record['calendarYear'].startswith(str(year)):
                    ebitda = record.get('ebitda', None)
                    Revenue = record.get('revenue', None)
                    grossProfit = record.get('grossProfit', None)
                    netIncome = record.get('netIncome', None)
                    Reported_Currency = record.get('reportedCurrency', None)
                    break

            # Update the DataFrame with the retrieved employee count
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'ebitda'] = ebitda
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'Revenue'] = Revenue
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'grossProfit'] = grossProfit
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'netIncome'] = netIncome
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'reportedCurrency'] = Reported_Currency
            # Print progress and wait if call limit reached
            calls_made += 1
            print(f"Processing ticker for {year} financial income data {i+1}/{len(tickers)} ({calls_made} calls made)                                                    ", end='\r')  # Overwrite previous line
            if calls_made >= MAX_CALLS_PER_MINUTE:
                sleep(SECONDS_PER_MINUTE / MAX_CALLS_PER_MINUTE)  # Wait for a minute before next call
                calls_made = 0  # Reset counter

    # Print completion message
    print(f"\nIncome Statement data retrieval complete. Retrieved data for {len(tickers)} tickers.")
    
    return Summary_df

def get_balance_sheet_statement_for_year(Summary_df, year_in, API_Key_in):
    tickers = Summary_df['Symbol_1'].tolist()
    year = year_in
    API_KEY = API_Key_in

    # Define maximum API calls per minute
    MAX_CALLS_PER_MINUTE = 300
    SECONDS_PER_MINUTE = 60

    # Function to get JSON data with error handling
    def get_jsonparsed_data(url):
        try:
            response = requests.get(url)
            if response.ok:
                data = response.json()
                return data
            else:
                print(f"Error retrieving data for {url.split('/')[-1]} ({response.status_code})", end="\r")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error making API request: {e}")
            return []

    # Make a copy of the DataFrame to avoid SettingWithCopyWarning
    Summary_df = Summary_df.copy()

    # Initialize the new column
    Summary_df['cashAndCashEquivalents'] = None
    Summary_df['shortTermInvestments'] = None
    Summary_df['longTermInvestments'] = None
    Summary_df['totalAssets'] = None
    Summary_df['totalLiabilities'] = None
    Summary_df['totalInvestments'] = None
    Summary_df['totalDebt'] = None
    Summary_df['totalEquity'] = None

    calls_made = 0  # Track API calls made

    for i, ticker in enumerate(tickers):
        if ticker == 'unknown':
            # Set financial income data to None for 'unknown' tickers
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'cashAndCashEquivalents'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'shortTermInvestments'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'longTermInvestments'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalAssets'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalLiabilities'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalInvestments'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalDebt'] = None
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalEquity'] = None

            print(f"Skipping ticker {i+1}/{len(tickers)}: 'unknown'", end='\r')
        elif pd.isna(Summary_df.loc[Summary_df['Symbol_1'] == ticker, ['cashAndCashEquivalents', 'shortTermInvestments', 'longTermInvestments', 'totalAssets', 'totalLiabilities', 'totalInvestments', 'totalDebt', 'totalEquity']]).all(axis=1).any():
            # Construct URL
            url = f"https://financialmodelingprep.com/api/v3/balance-sheet-statement/{ticker}?limit=15&apikey={API_KEY}"
            # Call the function and handle data
            data = get_jsonparsed_data(url)
            # Extract needed data for the specified year
            
            cashAndCashEquivalents = None
            shortTermInvestments = None
            longTermInvestments = None
            totalAssets = None
            totalLiabilities = None
            totalInvestments = None
            totalDebt = None
            totalEquity = None
            
            for record in data:
                if 'calendarYear' in record and record['calendarYear'].startswith(str(year)):
                    cashAndCashEquivalents = record.get('cashAndCashEquivalents', None)
                    shortTermInvestments = record.get('shortTermInvestments', None)
                    longTermInvestments = record.get('longTermInvestments', None)
                    totalAssets = record.get('totalAssets', None)
                    totalLiabilities = record.get('totalLiabilities', None)
                    totalInvestments = record.get('totalInvestments', None)
                    totalDebt = record.get('totalDebt', None)
                    totalEquity = record.get('totalEquity', None)

                    break

            # Update the DataFrame with the retrieved employee count
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'cashAndCashEquivalents'] = cashAndCashEquivalents
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'shortTermInvestments'] = shortTermInvestments
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'longTermInvestments'] = longTermInvestments
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalAssets'] = totalAssets
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalLiabilities'] = totalLiabilities
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalInvestments'] = totalInvestments
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalDebt'] = totalDebt
            Summary_df.loc[Summary_df['Symbol_1'] == ticker, 'totalEquity'] = totalEquity
            

            # Print progress and wait if call limit reached
            calls_made += 1
            print(f"Processing ticker for {year} balance sheet data {i+1}/{len(tickers)} ({calls_made} calls made)                                                    ", end='\r')  # Overwrite previous line
            if calls_made >= MAX_CALLS_PER_MINUTE:
                sleep(SECONDS_PER_MINUTE / MAX_CALLS_PER_MINUTE)  # Wait for a minute before next call
                calls_made = 0  # Reset counter

    # Print completion message
    print(f"\nBalance Sheet data retrieval complete. Retrieved data for {len(tickers)} tickers.")
    
    return Summary_df

####below are dictionaries used to replace categorical data with numerics for ML processing
primary_sector_replacement_dict = {
    'Financial services': 1,
    'IT & software development': 2,
    'Construction': 3,
    'Commercial & consumer services': 4,
    'Medical equipment & supplies': 5,
    'Specialized professional services': 6,
    'Thermal power generation': 7,
    'Wood & paper materials': 8,
    'Cement & concrete': 9,
    'Electrical & electronic equipment': 10,
    'Bars, hotels & restaurants': 11,
    'Metal smelting, refining & forming': 12,
    'Industrial support services': 13,
    'Chemicals': 14,
    'Web & marketing services': 15,
    'Convenience retail': 16,
    'Marine transport': 17,
    'Discretionary retail': 18,
    'Energy utility networks': 19,
    'Metallic mineral mining': 20,
    'Other services': 21,
    'Trading, wholesale, distribution, rental & leasing': 22,
    'Food & beverage processing': 23,
    'Air transport': 24,
    'Renewable power generation': 25,
    'Biotech & pharma': 26,
    'Intermodal transport & logistics': 27,
    'Print & publishing services': 28,
    'Tobacco': 29,
    'Transportation equipment': 30,
    'Plastic product manufacturing': 31,
    'Leisure & home manufacturing': 32,
    'Metal products manufacturing': 33,
    '0': 34,
    'Oil & gas extraction & production': 35,
    'Other mineral mining': 36,
    'Oil & gas storage & transportation': 37,
    'Other materials': 38,
    'Media, telecommunications & data center services': 39,
    'Powered machinery': 40,
    'Light manufacturing': 41,
    'Oil & gas processing': 42,
    'Wood & rubber products': 43,
    'Textiles & fabric goods': 44,
    'Rail transport': 45,
    'Non-energy utilities': 46,
    'Paper products & packaging': 47,
    'Oil & gas retailing': 48,
    'Land & property ownership & development': 49,
    'Accessories': 50,
    'Nuclear power generation': 51,
    'Coal mining': 52,
    'Renewable energy equipment': 53,
    'Entertainment facilities': 54,
    'Apparel design': 55,
    'Fish & animal farming': 56,
    'Health care provision': 57,
    'Road transport': 58,
    'Logging & rubber tapping': 59,
    'Crop farming': 60,
    'Questionnaire sector': 61,
    'Government agencies': 62,
    'International bodies': 63,
    'Government bodies': 64,
    '': 0,
    None: 0,  # Replace NaN with 0
}

primary_industry_replacement_dict = {
    'Services': 1,
    'Infrastructure': 2,
    'Biotech, health care & pharma': 3,
    'Power generation': 4,
    'Materials': 5,
    'Manufacturing': 6,
    'Hospitality': 7,
    'Retail': 8,
    'Transportation services': 9,
    'Food, beverage & agriculture': 10,
    '0': 11,
    'Fossil Fuels': 12,
    'Apparel': 13,
    'Corporate Tags': 14,
    'International bodies': 15,
    '': 0,
    None: 0,  # Replace NaN with 0
}

country_replacement_dict = {
    'United Kingdom': 1, 'Brazil': 2, 'Spain': 3, 'USA': 4, 'Italy': 5, 'Canada': 6, 'South Africa': 7,
    'India': 8, 'Taiwan': 9, 'France': 10, 'Switzerland': 11, 'Germany': 12, 'Japan': 13, 'Netherlands': 14,
    'Denmark': 15, 'Finland': 16, 'New Zealand': 17, 'Turkey': 18, 'Norway': 19, 'Ireland': 20, 'Australia': 21,
    'Austria': 22, 'Mexico': 23, 'South Korea': 24, 'Singapore': 25, 'Luxembourg': 26, 'China': 27,
    'Sweden': 28, 'Portugal': 29, 'Argentina': 30, 'Thailand': 31, 'Belgium': 32, 'Hong Kong': 33, 'Chile': 34,
    'Malaysia': 35, 'Israel': 36, 'Philippines': 37, 'Russia': 38, 'Greece': 39, 'Colombia': 40, 'Hungary': 41,
    'Cyprus': 42, 'Venezuela': 43, 'Jordan': 44, 'Liechtenstein': 45, 'Poland': 46, 'Guernsey': 47,
    'Bermuda': 48, 'United Arab Emirates': 49, 'Peru': 50, 'Pakistan': 51, 'Nigeria': 52, 'Costa Rica': 53,
    'Swaziland': 54, 'Indonesia': 55, 'Qatar': 56, 'Guatemala': 57, 'Belarus': 58, 'Sri Lanka': 59, 'Kenya': 60,
    'United Republic of Tanzania': 61, 'Vietnam': 62, 'Zimbabwe': 63, 'Oman': 64, 'Paraguay': 65,
    'Egypt': 66, 'Bahamas': 67, 'British Virgin Islands': 68, 'Slovakia': 69, 'Slovenia': 70,
    'Lithuania': 71, 'Monaco': 72, 'Czech Republic': 73, 'Bulgaria': 74, 'Croatia': 75, 'Cameroon': 76,
    'Romania': 77, 'Iceland': 78, 'El Salvador': 79, 'Dominican Republic': 80, 'Bangladesh': 81,
    'Kuwait': 82, 'Ghana': 83, 'Malta': 84, 'Jamaica': 85, 'Honduras': 86, 'Ecuador': 87, 'Panama': 88,
    'Uruguay': 89, 'Fiji': 90, 'Mongolia': 91, 'Trinidad': 92, 'Kazakhstan': 93, 'Mauritius': 94,
    'Netherlands Antilles': 95, 'Saudi Arabia': 96, 'Albania': 97, 'Belize': 98, 'Guyana': 99,
    'United Kingdom of Great Britain and Northern Ireland': 100, 'Republic of Korea': 101,
    'China, Hong Kong Special Administrative Region': 102, 'Russian Federation': 103, 'Cayman Islands': 104,
    'China, Macao Special Administrative Region': 105, 'Czechia': 106, 'Venezuela (Bolivarian Republic of)': 107,
    'Viet Nam': 108, 'United States Minor Outlying Islands': 109, 'Trinidad and Tobago': 110, 'Aruba': 111,
    'Ukraine': 112, 'Jersey': 113, 'Mozambique': 114, 'Cambodia': 115, 'Åland Islands': 116, 'San Marino': 117,
    'Afghanistan': 118, 'Bolivia (Plurinational State of)': 119, 'Morocco': 120, 'Serbia': 121,
    'Madagascar': 122, 'Puerto Rico': 123, 'Isle of Man': 124, 'Latvia': 125, 'Marshall Islands': 126,
    'Estonia': 127, 'Bahrain': 128, 'Iraq': 129, 'Libya': 130, 'Norfolk Island': 131, 'Mauritania': 132,
    'Algeria': 133, 'Georgia': 134, 'Tunisia': 135, 'Cocos (Keeling) Islands': 136, 'Greenland': 137,
    'Angola': 138, 'Myanmar': 139, 'Azerbaijan': 140, 'Equatorial Guinea': 141, 'Lebanon': 142,
    'Faroe Islands': 143, 'Bosnia & Herzegovina': 144, 'North Macedonia': 145
}