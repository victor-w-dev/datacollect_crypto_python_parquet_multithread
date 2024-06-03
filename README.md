# datacollect_crypto_python_parquet_multithread
This Python program demonstrates how to collect OHLCV (Open, High, Low, Close, Volume) data from cryptocurrency exchanges using 
Python, multithreading, and the `ccxt` library. 
The collected data is stored in Parquet format for efficient storage and retrieval as well as CSV format for easy exploration. 
The program is specifically designed to support OHLCV collection from Binance and Bybit.

## Requirements
- **Python 3.9+**: The programming language used to write and run the scripts.
- **Required Python packages**:
  - **ccxt**: a library to interact with cryptocurrency exchanges to collect OHLCV data.
  - **pandas**: a data manipulation and analysis library, used for handling the collected data.
  - **pyarrow**: a library that provides tools for working with the Parquet file format, which is used to store the collected data.
  - **python-dotenv**: A library to read key-value pairs from a `.env` file and set them as environment variables, used for configuration (e.g., email settings).
  - **threading**: a module for running multiple threads (tasks, function calls) at once, used for concurrent data collection.

## Demo
- Using Spyder as IDE to run the program, collecting ohlcv data from Crypto Exchange: Binance
<img src="https://github.com/victor-w-dev/datacollect_crypto_python_parquet_multithread/blob/main/img/demo_collectingdata_spyder.PNG" width="100%" height="100%"><br>
