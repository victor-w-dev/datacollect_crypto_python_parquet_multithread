import pandas as pd
import time
import os
import ccxt
import re

from functools import wraps
from threading import Thread

from notificationv1 import send_email
import psutil
import math
from dotenv import load_dotenv

import warnings
# Ignore FutureWarning from pyarrow.pandas_compat
warnings.filterwarnings("ignore", category=FutureWarning, module="pyarrow.pandas_compat")

from datetime import datetime, timezone

pd.set_option('expand_frame_repr', False)

def timer(func):
    """
       A decorator that prints the time a function takes to execute.
    
       This decorator measures the time taken for the wrapped function to execute
       and prints the duration in a human-readable format (seconds, minutes, or hours).
    
       Args:
           func (callable): The function to be decorated.
    
       Returns:
           callable: The wrapped function with execution time measurement.
   """
    @wraps(func)
    def wrapper(*args):
        start_time = time.time();
        retval = func(*args)
        
        
        time_in_seconds = time.time()-start_time
        
        if time_in_seconds > 3600:  # More than 60 minutes
            time_in_hours = time_in_seconds / 3600
            time_used = f"{time_in_hours:.2f} hours"
        elif time_in_seconds > 60:  # More than 1 minute, but less than 60 minutes
            time_in_minutes = time_in_seconds / 60
            time_used = f"{time_in_minutes:.2f} minutes"
        else:  # 1 minute or less
            time_used = f"{time_in_seconds:.2f} seconds"
        
        print("the function ends in ", time_used, "secs\n")
        return retval
    return wrapper

class ohlcv_datacollector():
    def __init__(self, exchange, start, end, mdm):
        """
        Initializes the ohlcv_datacollector instance.

        Args:
            exchange (str): The exchange from which to collect data.
            start (str): The start time for data collection.
            end (str): The end time for data collection.
            mdm (str): The directory for saving data.
        """
        
        self.running = True
        self.exchange = exchange
        self.start = start
        self.end = end
        self.threads = []
        self.mdm = mdm
        
    @staticmethod
    def get_cpu_info():
        """
        Static method to get the number of physical cores and logical processors.
        
        Returns:
            dict: A dictionary with the number of physical cores and logical processors.
        """
        # Get the number of physical cores
        physical_cores = psutil.cpu_count(logical=False)
        # Get the number of logical processors
        logical_processors = psutil.cpu_count(logical=True)
        
        return {
            'physical_cores': physical_cores,
            'logical_processors': logical_processors
        }
    
    def stop_thread(self):
        self.running = False 
        
    def start_thread(self, symbols, intervals):    
        """
        Starts a new thread for data collection.

        Args:
            symbols (list): List of symbols to collect data for.
            intervals (list): List of intervals for data collection.
        """
        t = Thread(target = self.get_history_threads, args = (symbols, intervals))
        t.start()
        self.threads.append(t)
        
    @timer
    def start_collection_all_threads(self, symbols, intervals): 
        """
        Starts data collection using multiple threads.

        Args:
            symbols (list): List of symbols to collect data for.
            intervals (list): List of intervals for data collection.
        """
        start_time = time.time()
        # calculate available threads for cpu
        num_threads = self.get_cpu_info()['logical_processors']
        
        chunk_size = math.ceil(len(symbols)/num_threads)
        symbol_ls = [symbols[i:i + chunk_size] for i in range(0, len(symbols), chunk_size)]
        print(symbol_ls)
        
        for ls in symbol_ls:
            self.start_thread(ls, intervals)
        
        self.wait_for_completion()
        
        end_time = time.time()
        
        time_in_seconds = end_time-start_time  
        #print(time_in_seconds) 
        
        if time_in_seconds > 3600:  # More than 60 minutes
            time_in_hours = time_in_seconds / 3600
            time_used = f"Time used: {time_in_hours:.2f} hours"
        elif time_in_seconds > 60:  # More than 1 minute, but less than 60 minutes
            time_in_minutes = time_in_seconds / 60
            time_used = f"Time used: {time_in_minutes:.2f} minutes"
        else:  # 1 minute or less
            time_used = f"Time used: {time_in_seconds:.2f} seconds"
            
        #notificaion when begin run
        subject = f"Data ohlcv collection for {self.exchange} completed"
        body = f"Data ohlcv collection for {self.exchange}\n{self.start} to {self.end}\n"\
                + time_used

        print(subject)
        print(body)
        
        load_dotenv("email.env")
        report_email = os.getenv("REPORT_EMAIL")
        report_email_pw = os.getenv("REPORT_EMAIL_PASSWORD")
        recipient_email = os.getenv("RECIPIENT_EMAIL")

        send_email(user=report_email, 
                      pwd=report_email_pw,
                      recipient=recipient_email,
                      subject=subject, body=body)
        
    def wait_for_completion(self):
        """
        Waits for all threads to complete their execution.
        """
        for thread in self.threads:
            thread.join()  # Wait for the thread to finish
    
    def get_history_threads(self, symbols, intervals):
        """
        Collects historical data for the given symbols and intervals in a threaded manner.

        Args:
            symbols (list): List of symbols to collect data for.
            intervals (list): List of intervals for data collection.
        """
        self.error_symbols_interval=[]
        for interval in intervals:
            for s in symbols:
                if self.running == False:
                    return
                try:                                     
                    self.get_history(s, interval, self.start, self.end, 1000, True, mdm)
                  
                except Exception as e:
                    print("Exception caught:", str(e))           
                    self.error_symbols_interval.append(tuple((s, interval)))
                    continue

    @timer
    def get_history(self, symbol, interval, start_time=None, end_time=None, limit =1000, savefile=False, mdm=None):
        """
        Returns a DataFrame of historical data for a cryptocurrency symbol.

        Args:
            symbol (str): The symbol to collect data for (e.g., 'BTC/USDT').
            interval (str): The time interval for data collection (e.g., '1m', '1h').
            start_time (str, optional): The start time for data collection (e.g., '2022-01-01').
            end_time (str, optional): The end time for data collection (e.g., '2023-01-01').
            limit (int, optional): The maximum number of data points to fetch per request.
            savefile (bool, optional): Whether to save the data to a file.
            mdm (str, optional): The directory to save the data.

        Returns:
            pandas.DataFrame: A DataFrame containing the historical data.
        """
        
        if self.exchange == 'Bybit':
            exchange = ccxt.bybit()
        elif self.exchange == 'Binance':
            exchange = ccxt.binance()

        print(exchange)
        print(f"symbol: {symbol}, interval: {interval}")

        if start_time:
            #start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
            start_unix = exchange.parse8601(str(start_time))

        if end_time:
            #end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d %H:%M:%S')
            end_unix = exchange.parse8601(str(end_time))
            last_bar_target = end_unix
        else:
            # timestamp of current bar 
            last_bar_target = exchange.fetchOHLCV(symbol = symbol, timeframe = interval, limit = 2)[-1][0]

            # timestamp of last second current bar as last bar may not be completed
            #last_bar_target = exchange.fetchOHLCV(symbol = symbol, timeframe = interval, limit = 3)[-2][0]

        print(f"start: {start_time}")
        #print(f"start: {start_unix}")
        last_bar_target_time = f"{exchange.iso8601(last_bar_target)[:10]} {exchange.iso8601(last_bar_target)[11:19]}"
        print(f"target 1 bar before: {last_bar_target_time}")         
        #print(f"target end: {last_bar_target}")      

        try:
            data = exchange.fetchOHLCV(symbol = symbol, timeframe = interval, since = start_unix, limit = limit)
            
            # check have recent data
            #print(data)
            if len(data)==0: 
                raise Exception("Sorry, no recent data")
            
            last_bar_actual = data[-1][0] # timestamp of last loaded bar
            #print(data)
        except Exception as error:
            print("Something error when get data")
            print(error)
            
        # will run below only to extract data only self.running = True
        self.running = True          
                    
        while last_bar_target != last_bar_actual:
            if self.running == False:
                return
                   
            try:
                data_add = exchange.fetchOHLCV(symbol=symbol, timeframe=interval, 
                                               since=last_bar_actual, limit=limit)

                if data_add[0][0]>last_bar_target:
                    break

                add_bar_date = exchange.iso8601(data_add[0][0])[:10]
                add_bar_time = exchange.iso8601(data_add[0][0])[11:19]

                print("\rAdd bar starting from: {} {}\n".format(add_bar_date,
                                                 add_bar_time),
                                                 #data_add[0][0]),
                                                 end='',
                                                 flush=True)

                data += data_add[1:] # remove 1st row here
                last_bar_actual = data[-1][0]

                time.sleep(0.2)

            except Exception as error:
                print(error)
                time.sleep(10)

        # remove last bar as may not be completed
        df = pd.DataFrame(data[:-1])
        df.columns = ["Date", "Open", "High", "Low", "Close", "Volume"]
        # remove the records less than (before) last_bar_target
        df = df[df.Date<last_bar_target]
        
        # check have data
        #print(df)
        if len(df)==0: 
            raise Exception("Sorry, no data for this period")
        
        df.Date = pd.to_datetime(df.Date, unit = "ms")
        df.set_index("Date", inplace = True)
        print("\nData Collection Completed.")

        if not df.index.is_unique:
            raise Exception("Sorry, Index is not unique, need to study the program and data")

        print(f"data row count: {len(df)}")

        if savefile:
            replace_str = {'/': '',
                       ':': '_'}

            symbol_adjusted = symbol
            for k, v in replace_str.items():
                symbol_adjusted = symbol_adjusted.replace(k, v)
            #print(f"folder_name: {folder_name}")

            if not mdm:
                mdm = os.getcwd()

            file_dir = os.path.join(mdm, interval, self.exchange, symbol_adjusted)
            # special handling for the interval: 1M, i.e. 1mth
            file_dir = file_dir.replace('1M', '1mth')

            if not os.path.exists(file_dir):
                os.makedirs(file_dir)

            #file_name_parquet = f"{symbol_adjusted}_{df.index[0]}_{df.index[-1]}.parquet"\
            file_name_parquet = f"{symbol_adjusted}_{df.index[-1].strftime('%Y%m%d')}.parquet"\
                        .replace(':', '')\
                        .replace(' ', '')\
                        .replace('-', '')     

            #file_name_csv = f"{symbol_adjusted}_{df.index[0]}_{df.index[-1]}.csv"\
            file_name_csv = f"{symbol_adjusted}_{df.index[-1].strftime('%Y%m%d')}.csv"\
                        .replace(':', '')\
                        .replace(' ', '')\
                        .replace('-', '') 

            file_path_parquet = os.path.join(file_dir, file_name_parquet)
            file_path_csv = os.path.join(file_dir, file_name_csv)
            
            # adjust date format
            #df.index = pd.to_datetime(df.index)

            # Convert the date format
            #df.index = df.index.strftime('%d/%m/%Y')

            #if len(df)>=100:
            df.to_parquet(file_path_parquet)
            print(f'Saved to: {file_path_parquet}')
            df.to_csv(file_path_csv)
            print(f'Saved to: {file_path_csv}')
            
def get_ohlcv_data(start, end, mdm, exchange_to_use: str, symbol_filter: list = None, interval_filter: list = None):
    
    end_dt = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    #print(end_dt)
    # Get the current UTC time
    now_utc = datetime.now(timezone.utc)
    #print(now_utc)
    # Check if the current UTC time is less than the end time
    if now_utc <= end_dt.replace(tzinfo=timezone.utc):
        raise ValueError("The current UTC time must be less than the end time.")
    
    if exchange_to_use == 'Bybit':
        #from Bybit exchange
        exchange = ccxt.bybit()
        
        tfs = exchange.timeframes     
        
        tfs['1d']=24*60
        tfs['1w']=7*24*60
        tfs['1M']=30*24*60
        del tfs['1M'], tfs['1w']
        for k, v in exchange.timeframes.items():
            tfs[k] = int(v)        

    elif exchange_to_use == 'Binance':
        #from Binance exchange
        exchange = ccxt.binance()
    
        tfs = exchange.timeframes
        tfs['1s']=0
        tfs['1m']=1
        tfs['3m']=3
        tfs['5m']=5
        tfs['15m']=15
        tfs['30m']=30
        tfs['1h']=60
        tfs['2h']=120
        tfs['4h']=240
        tfs['6h']=360
        tfs['8h']=120*4
        tfs['12h']=720
        tfs['1d']=24*60
        tfs['3d']=3*24*60
        tfs['1w']=7*24*60
        tfs['1M']=30*24*60
        del tfs['1s'], tfs['1M'], tfs['1w'], tfs['3d']
    
    tfs = dict(sorted(tfs.items(), key=lambda x: x[1], reverse=True))
    
    tfs_key = [k for k in tfs]
    
    print(f"Using Exchange: {exchange_to_use}")
    print(f"Available time frame: {tfs}")
    
    #tfs_key = ['1d']
    if interval_filter:
        tfs_key = list(set(tfs_key).intersection(interval_filter))
    
        print(f"Using time frame: {tfs_key}")
    
    markets = exchange.load_markets()
    
    symbols=[]
    for k, v in markets.items():
        if v['contract']==True and v['settle']=='USDT':
            symbols.append(k)
            #print(k)
            
    if symbol_filter:
        symbols = list(set(symbols).intersection(symbol_filter))
    symbols = sorted(symbols)
    print(symbols)
    print(f"Available USDT Perpetual Contract Symbols: {len(symbols)}")
    
    #symbols= ['RUNE/USDT:USDT', 'ADA/USDT:USDT', 'BNB/USDT:USDT', 'MATIC/USDT:USDT']
    
    datacollector = ohlcv_datacollector(exchange=exchange_to_use, start = start, end = end, mdm = mdm)
    
    datacollector.start_collection_all_threads(symbols, tfs_key)
    
    return datacollector.error_symbols_interval
    
def is_month_end(date_str):
    """Check if the given date string is the last day of the month."""
    date = pd.to_datetime(date_str, format='%Y%m%d')
    return date.is_month_end

def get_date_from_filename(filename):
    """Extract date from filename using regex."""
    match = re.search(r'_(\d{8})\.', filename)
    if match:
        return match.group(1)
    return None

def delete_non_month_end_files(directory):
    """Delete non-month-end .csv and .parquet files in the specified directory."""
    for filename in os.listdir(directory):
        filepath = os.path.join(directory, filename)
        if os.path.isfile(filepath):
            # Check if the file is .csv or .parquet
            if filename.endswith('.csv') or filename.endswith('.parquet'):
                date_str = get_date_from_filename(filename)
                if date_str and not is_month_end(date_str):
                    print(f"Deleting file: {filepath}")
                    os.remove(filepath)

def delete_duplicated_ohlcv(base_directory):
    """delete_duplicated_ohlcv function to iterate through directories and delete non-month-end files."""
    
    for root, dirs, files in os.walk(base_directory):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            #print(dir_path)
            delete_non_month_end_files(dir_path)

def list_all_folders(base_directory):
    """List all folders within the specified base directory."""
    for root, dirs, files in os.walk(base_directory):
        for dir_name in dirs:
            dir_path = os.path.join(root, dir_name)
            print(dir_path)

if __name__ == "__main__":
    # define storing location
    mdm = r"D:\GitHub\CryptoAlgoTrade\cryptotrader\mdm\ohlcv"
    # define variables
    interval_ls = ['1d', '1h', '1m', '2h', '3m', '4h', '5m', '6h', '8h', '12h', '15m', '30m']
    exchange_ls = ['Binance', 'Bybit', ]
    
    # Delete duplicated ohlcv first
    print("Deleting duplicated ohlcv")
    for interval in interval_ls:
        for exchange in exchange_ls:
            base_directory = f'{mdm}\{interval}\{exchange}'
            #base_directory = r'D:\GitHub\CryptoAlgoTrade\cryptotrader\mdm'    
            delete_duplicated_ohlcv(base_directory)
    print("Deleting duplicated ohlcv Completed")
    
    # Get data
    # input UTC 0 time
    start = '2024-05-01 00:00:00'
    end = '2024-06-01 00:00:00'
    
    #exchange_to_use = 'Bybit'
    #symbols = ['RUNE/USDT:USDT', 'ADA/USDT:USDT', 'BNB/USDT:USDT', 'MATIC/USDT:USDT']
    #interval_filter = ['1d']
    error_ls = []
    for ex in exchange_ls:
        err = get_ohlcv_data(start=start, 
                             end=end, 
                             mdm=mdm, 
                             exchange_to_use=ex,
                             symbol_filter=None, 
                             interval_filter=None)
        if err:
            error_ls.extend([err, ex])
    
    

    