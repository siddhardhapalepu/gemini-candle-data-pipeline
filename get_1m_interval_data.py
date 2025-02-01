import requests
import pandas as pd
import logging
import datetime
import pytz
import aws_utils

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_data(url, params=None):
    """Fetch data from the API and return a JSON response."""
    try:
        response = requests.get(url, params=params)
        logging.debug(f"fetching data from {url}")
        response.raise_for_status() # Will raise an HTTPError if the HTTP request returned an unsuccessful status code
        logging.debug(f"{url} - Successful")
        return response.json()
    except requests.RequestException as e:
        logging.error(f"Request failed: {e}")
        return None

def prepare_candle_data(data, columns=None, time_window=None):
    """This function prepares candle data for the given time window

    Args:
        data (list): input data is list of lists containing candle data
        columns (list, optional): This is list of columns for candle data. Defaults to None.
        time_window (int, optional): this defines how many minutes is the time window. Defaults to None.

    Returns:
        pandas dataframe: returns processed candle dataframe
    """
    if data is None:
        logging.info("No data in candle dataframe")
        return None
    if columns is None:
        candle_columns = [ "candle_open_time_epoch", "open_price", "high_price", "low_price", "close_price", "btc_volume"]
    df_candle = pd.DataFrame(data, columns=candle_columns)
    df_candle["trading_pair"] = "BTCUSD"
    df_candle["usd_volume"] = df_candle["btc_volume"] * df_candle["close_price"]
    df_candle["no_of_trades"] = None  # Placeholder
    df_candle["candle_close_time_epoch"] = df_candle["candle_open_time_epoch"] + 60000
    df_candle['candle_open_time_utc'] = pd.to_datetime(df_candle['candle_open_time_epoch'], unit='ms')
    df_candle['candle_open_time_est'] = df_candle['candle_open_time_utc'].dt.tz_localize('UTC').dt.tz_convert('US/Eastern')
    logging.debug("First 5 rows of df_candle data")
    logging.debug(f"{df_candle.head(5)}")
    return df_candle.head(time_window)

def prepare_trade_data(trade_url, start_time, end_time, api_limit):
    """
    This function prepares trades data for the input candle minute data
    Args:
        trade_url (string): Gemini trade api endpoint
        start_time (int): start window of  candle data
        end_time (int): end window of candle data
        api_limit (int): api output limit value

    Returns:
        pandas dataframe: returns set of trades for the given candle minutes
    """
    params = {
        "timestamp": start_time
    }
    trade_data = fetch_data(trade_url, params=params)
    
    # Using tid because timestamps are duplicate
    tid = trade_data[0]["tid"]
    current_data = trade_data
    
    while end_time > start_time and len(current_data)!=0:
        params = {
            "since_tid":tid,
            "limit_trades": api_limit
        }

        current_data = fetch_data(trade_url, params=params)
        trade_data.extend(current_data)        
        
        if(len(current_data)!=0):
            tid = current_data[0]["tid"]
            end_time = current_data[0]["timestampms"]
            
    df_trades = pd.DataFrame(trade_data, columns=["timestamp", "timestampms", "tid", "price", "amount", "exchange", "type"])
    logging.debug("First 5 rows of trade data")
    logging.debug(f"{df_trades.head(5)}")
    return df_trades

def prepare_one_min_candle_data(candle_data, trades_data):
    """This function prepares the final data which combines candle and trades data

    Args:
        candle_data (dataframe): processed candle data
        trades_data (dataframe): processed trades data

    Returns:
        dataframe: processed/aggregated candle/trades data
    """
    df_candle = candle_data
    df_trades = trades_data
    for idx, candle in df_candle.iterrows():
        trades_in_interval = df_trades[
            (df_trades['timestampms'] >= candle['candle_open_time_epoch']) &
            (df_trades['timestampms'] < candle['candle_close_time_epoch'])
        ]
        df_candle.at[idx, 'no_of_trades'] = len(trades_in_interval)
    df_final = df_candle[['trading_pair', 'open_price', 'close_price', 'high_price', 'low_price', 'btc_volume', 'usd_volume', 'no_of_trades', 'candle_open_time_epoch', 'candle_close_time_epoch']]
    df_final = df_final.rename(columns={
        'trading_pair': 'Trading Pair',
        'open_price' : 'Open Price',
        'close_price': 'Close Price',
        'high_price' : 'High Price',
        'low_price' : 'Low Price',
        'btc_volume' : 'BTC Volume',
        'usd_volume' : 'USD Volume',
        'no_of_trades': 'No of Trades',
        'candle_open_time_epoch': 'Candle Open Time',
        'candle_close_time_epoch': 'Candle Close Time'
    })
    logging.debug("First 5 rows of data")
    logging.debug(f"{df_final.head(5)}")

    return df_final
    


def main():
    time_window = 10 # process data for last time_window mins from this minute
    trades_api_limit = 500

    # Building candle data
    logging.info(f"Processing candle data for time window of {time_window} minutes")
    one_min_candle_url = "https://api.gemini.com/v2/candles/BTCUSD/1m" 
    candle_data = fetch_data(one_min_candle_url)
    df_candle = prepare_candle_data(data=candle_data, time_window=time_window)
    
    # extracting latest and oldest epoch time from candle data 
    #candle_oldest_open_time = df_candle.loc[len(df_candle)-1, "candle_open_time_epoch"] - To process 24 hour data
    candle_oldest_open_time = df_candle.loc[time_window-1, "candle_open_time_epoch"]
    candle_recent_open_time = df_candle.loc[0, "candle_open_time_epoch"]
    
    # Building trades data
    logging.info(f"Processing trades data for time window of {time_window} minutes")
    trade_url = "https://api.gemini.com/v1/trades/BTCUSD"
    df_trade = prepare_trade_data(trade_url, candle_oldest_open_time, candle_recent_open_time, trades_api_limit)
    
    logging.info("preparing final candle_1m data")
    df_final = prepare_one_min_candle_data(df_candle, df_trade)
    df_final.to_csv('candle_min_final.csv',index=False)
    bucket = 'gemini-data-landing'
    aws_utils.upload_file_to_s3(file_name='candle_min_final.csv', bucket=bucket)

if __name__ == "__main__":
    main()
