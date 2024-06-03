# datacollect_crypto_python_parquet_multithread
- This Python program demonstrates how to collect OHLCV (Open, High, Low, Close, Volume) data from cryptocurrency exchanges using Python, `multithreading`, and the `ccxt` library. 
- The collected data is stored in Parquet format for efficient storage and retrieval as well as CSV format for easy exploration. 
- The program is specifically designed to support OHLCV data collection from Binance and Bybit for crypto USDT perpetual contract data.

## Requirements
- **Python 3.9+**: The programming language used to write and run the scripts.
- **Required Python packages**:
- **[ccxt](https://github.com/ccxt/ccxt)**: a library to interact with cryptocurrency exchanges to collect OHLCV data.
- **[pandas](https://github.com/pandas-dev/pandas)**: a data manipulation and analysis library, used for handling the collected data.
- **[pyarrow](https://arrow.apache.org/docs/python/)**: a library that provides tools for working with the Parquet file format, which is used to store the collected data.
- **[python-dotenv](https://github.com/theskumar/python-dotenv)**: a library to read key-value pairs from a `.env` file and set them as environment variables, used for configuration (e.g., email settings).
- **[threading](https://docs.python.org/3/library/threading.html)**: a module for running multiple threads (tasks, function calls) at once, used for concurrent data collection.

## Storage Hierarchy
The collected OHLCV data is organized in a structured directory hierarchy to ensure easy access and management. The hierarchy is designed from low cardinality (fewer unique values) to high cardinality (more unique values):<br>
```
ohlcv/
├── [interval]/
│ ├── [exchange]/
│ │ ├── [symbol]/
│ │ │ ├── [symbol]_[YYYYMMDD].parquet
│ │ │ ├── [symbol]_[YYYYMMDD].csv
```
### Explanation
- **Low Cardinality to High Cardinality**: The hierarchy starts with attributes that have fewer unique values and progresses to attributes with more unique values.
  - **[interval]/**: Subdirectory specifying the time interval of the data (e.g., `1d` for 1-day interval data, `1h` for 1-hour interval data). Intervals are generally fixed and have fewer unique values, so that's why put in the beginning of the hierarchy
  - **[exchange]/**: Subdirectory for each supported exchange (e.g., `Binance` or `Bybit`). Exchanges may be fixed initially but can be expanded to include more exchanges in the future, so that's why after interval in the hierarchy
  - **[symbol].parquet**: The trading pair, stored as a Parquet file (e.g., `BTCUSDT_USDT.parquet`). Trading symbols have the highest cardinality as there can be many different trading pairs.
 
### Example
For instance, if you are collecting 1-day interval data for the `BTCUSDT_USDT` trading pair from the Binance exchange, the data would be stored as:<br>
```
ohlcv/
├── 1d/
│ ├── Binance/
│ │ ├── BTCUSDT_USDT/
│ │ │ ├── BTCUSDT_USDT_20240531.parquet
│ │ │ ├── BTCUSDT_USDT_20240531.csv
```
<br>
<img src="https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/img/storing_hierarchy_lv1.PNG" width="60%" height="60%"><br>
<img src="https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/img/storing_hierarchy_lv2.PNG" width="60%" height="60%"><br>
<img src="https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/img/storing_hierarchy_lv3.PNG" width="60%" height="60%"><br>
<img src="https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/img/storing_hierarchy_lv4.PNG" width="60%" height="60%"><br>

## Demo
- Using Spyder as IDE to run the program, collecting ohlcv data from Crypto Exchange: Binance
- [datacollection_class_ohlcv_v7_20240603.py](https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/datacollection_class_ohlcv_v7_20240603.py)
<br>
<img src="https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/img/demo_collectingdata_spyder.PNG" width="100%" height="100%"><br>
