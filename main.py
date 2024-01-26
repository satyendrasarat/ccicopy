import sqlite3

#from datetime import datetime, time
from datetime import datetime, time

import pandas as pd
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from flask import Flask, render_template
from flask_socketio import SocketIO, emit
from tradingview_ta import Interval, TA_Handler

from option_chain_fetcher import OptionChainFetcher

app = Flask(__name__)
socketio = SocketIO(app)
BOT_TOKEN = '5511964084:AAHV7mNKO35XLcNcEpnt4Pm_bfWF7yNag4k'
CHAT_ID = '-1001655282245'
# your_script.py
nifty_option_chain_fetcher = OptionChainFetcher("nifty")
expiry_date_nifty = "2023-12-14"
nifty_strike = 2090000

banknifty_option_chain_fetcher = OptionChainFetcher("nifty-bank")
expiry_date_banknifty = "2023-12-13"
banknifty_strike = 4700000

#call_ltp_value_banknifty = banknifty_option_chain_fetcher.call_ltp(expiry_date_banknifty, banknifty_strike)
#put_ltp_value_banknifty = banknifty_option_chain_fetcher.put_ltp(expiry_date_banknifty, banknifty_strike)

#print(f"Call LTP for strike {banknifty_strike} on {expiry_date_banknifty}: {call_ltp_value_banknifty}")
#print(f"Put LTP for strike {banknifty_strike} on {expiry_date_banknifty}: {put_ltp_value_banknifty}")

#nifty_call_ltp_value = nifty_option_chain_fetcher.call_ltp(expiry_date_nifty, nifty_strike)
#nifty_put_ltp_value = nifty_option_chain_fetcher.put_ltp(expiry_date_nifty, nifty_strike)

#print(f"Call LTP for strike {nifty_strike} on {expiry_date_nifty}: {nifty_call_ltp_value}")
#print(f"Put LTP for strike {nifty_strike} on {expiry_date_nifty}: {nifty_put_ltp_value}")

#call_ltp_value = nifty_option_chain_fetcher.call_ltp(expiry_date, nifty_strike)
#put_ltp_value = nifty_option_chain_fetcher.put_ltp(expiry_date, nifty_strike)

#print(f"Call LTP for strike {nifty_strike}: {call_ltp_value}")
#print(f"Put LTP for strike {nifty_strike}: {put_ltp_value}")

# ... (other configurations)

# Initialize lists for NIFTY and BANKNIFTY data
nifty_data = []
banknifty_data = []


def send_telegram_message(message):
  url = f'https://api.telegram.org/bot{BOT_TOKEN}/sendMessage'
  params = {'chat_id': CHAT_ID, 'text': message}

  try:
    response = requests.post(url, params=params)
    if response.status_code == 200:
      print("Message sent successfully!")
    else:
      print(f"Failed to send message. Status code: {response.status_code}")
  except Exception as e:
    print(f"Error sending message: {e}")


# Function to convert data into pandas DataFrame
def create_dataframe(data, symbol):
  if not data:
    return pd.DataFrame()

  df = pd.DataFrame(data)
  df['timestamp'] = pd.to_datetime(df['timestamp']) + pd.Timedelta(hours=5,
                                                                   minutes=30)
  df['timestamp'] = df['timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
  df.set_index('timestamp', inplace=True)
  df.columns = [f"{symbol}_CCI"]
  df[f"{symbol}_CCI"] = df[f"{symbol}_CCI"].round().astype(
      int)  # Round and convert to a whole number
  return df


def get_cci_data(symbol, exchange, screener):
  handler = TA_Handler(symbol=symbol,
                       exchange=exchange,
                       screener=screener,
                       interval=Interval.INTERVAL_5_MINUTES,
                       timeout=None)

  analysis = handler.get_analysis()
  cci_value = analysis.indicators['CCI20']
  timestamp = datetime.now()

  return {'timestamp': timestamp, 'cci_value': cci_value}


def generate_buy_signal(data, symbol):
  if len(data) < 310:
    # Not enough data points for comparison
    return ''

  latest_entry = data[-1]
  cci_values = [entry['cci_value']
                for entry in data[:-1]]  # Exclude the latest entry

  if latest_entry['cci_value'] > 0:
    # Find the index where the first CCI value is less than -150, starting from data[-2]
    index_less_than_minus_150 = None
    for i in range(len(cci_values) - 2, max(len(cci_values) - 301, 0), -1):
      if cci_values[i] < -150:
        index_less_than_minus_150 = i
        break

    if index_less_than_minus_150 is not None and all(
        value < 0 for value in cci_values[index_less_than_minus_150 + 1:-1]):
      message = f"Generate BUY signal for {symbol}! Latest CCI: {latest_entry['cci_value']}"
      send_telegram_message(message)

      if symbol == 'nifty':
        call_ltp_value = nifty_option_chain_fetcher.call_ltp(
            expiry_date_nifty, nifty_strike)
        send_telegram_message(call_ltp_value)
      elif symbol == 'banknifty':
        call_ltp_value_banknifty = banknifty_option_chain_fetcher.call_ltp(
            expiry_date_banknifty, banknifty_strike)
        send_telegram_message(call_ltp_value_banknifty)

      return f'BUY {symbol}'

  return ''


def generate_sell_signal(data, symbol):
  if len(data) < 310:
    # Not enough data points for comparison
    return ''

  latest_entry = data[-1]
  cci_values = [entry['cci_value']
                for entry in data[:-1]]  # Exclude the latest entry

  if latest_entry['cci_value'] < 0:
    # Find the index where the first CCI value is greater than +150, starting from data[-2]
    index_greater_than_plus_150 = None
    for i in range(len(cci_values) - 2, max(len(cci_values) - 301, 0), -1):
      if cci_values[i] > 150:
        index_greater_than_plus_150 = i
        break

    if index_greater_than_plus_150 is not None and all(
        value > 0 for value in cci_values[index_greater_than_plus_150 + 1:-1]):
      message = f"Generate SELL signal for {symbol}! Latest CCI: {latest_entry['cci_value']}"
      send_telegram_message(message)

      if symbol == 'nifty':
        put_ltp_value = nifty_option_chain_fetcher.put_ltp(
            expiry_date_nifty, nifty_strike)
        send_telegram_message(put_ltp_value)
      elif symbol == 'banknifty':
        put_ltp_value_banknifty = banknifty_option_chain_fetcher.put_ltp(
            expiry_date_banknifty, banknifty_strike)
        send_telegram_message(put_ltp_value_banknifty)

      return f'SELL {symbol}'

  return ''


# Initialize SQLite3 database connection and create tables
conn = sqlite3.connect('market_data.db')
cursor = conn.cursor()

# Create tables if not exists
cursor.execute('''
    CREATE TABLE IF NOT EXISTS nifty_data (
        timestamp TEXT PRIMARY KEY,
        cci_value INTEGER
    )
''')

cursor.execute('''
    CREATE TABLE IF NOT EXISTS banknifty_data (
        timestamp TEXT PRIMARY KEY,
        cci_value INTEGER
    )
''')

# Commit changes and close the connection
conn.commit()
conn.close()


def store_data_in_db(table_name, data_entry):
  conn = sqlite3.connect('market_data.db')
  cursor = conn.cursor()
  cursor.execute(f"INSERT OR REPLACE INTO {table_name} VALUES (?, ?)",
                 (data_entry['timestamp'], data_entry['cci_value']))
  conn.commit()
  conn.close()


def load_data_from_db(table_name, append_length):
  conn = sqlite3.connect('market_data.db')
  cursor = conn.cursor()
  cursor.execute(
      f"SELECT * FROM {table_name} ORDER BY timestamp DESC LIMIT {append_length}"
  )
  data = cursor.fetchall()
  conn.close()
  return [{
      'timestamp': entry[0],
      'cci_value': entry[1]
  } for entry in reversed(data)]


append_length = 350


def update_data():
  global nifty_data
  global banknifty_data
  now = datetime.now().time()
  today = datetime.now().strftime("%d-%b-%Y")
  today_weekday = datetime.now().weekday()
  market_open = time(3, 45)
  market_close = time(10, 00)

  holiday_dates_2024 = [
    "26-Jan-2024", "08-Mar-2024", "25-Mar-2024", "29-Mar-2024",
    "11-Apr-2024", "17-Apr-2024", "01-May-2024", "17-Jun-2024",
    "17-Jul-2024", "15-Aug-2024", "02-Oct-2024", "01-Nov-2024",
    "15-Nov-2024", "25-Dec-2024"
]


  # Check if it's a weekday, not a holiday, and within market hours
  if 0 <= today_weekday <= 4 and today not in holiday_dates and market_open <= now <= market_close:
    # ... (rest of the code)

    # Fetch and record data every minute for NIFTY
    nifty_entry = get_cci_data(symbol="NIFTY",
                               exchange="NSE",
                               screener="india")
    nifty_data.append(nifty_entry)
    nifty_data = nifty_data[
        -append_length:]  # Keep the latest 'append_length' values
    generate_buy_signal(nifty_data, "NIFTY") or generate_sell_signal(
        nifty_data, "NIFTY")

    # Store data in SQLite3 database for NIFTY
    store_data_in_db('nifty_data', nifty_entry)

    # Fetch and record data every minute for BANKNIFTY
    banknifty_entry = get_cci_data(symbol="BANKNIFTY",
                                   exchange="NSE",
                                   screener="india")
    banknifty_data.append(banknifty_entry)
    banknifty_data = banknifty_data[
        -append_length:]  # Keep the latest 'append_length' values
    generate_buy_signal(banknifty_data, "BANKNIFTY") or generate_sell_signal(
        banknifty_data, "BANKNIFTY")

    # Store data in SQLite3 database for BANKNIFTY
    store_data_in_db('banknifty_data', banknifty_entry)


# ... (rest of the code)

# ... (rest of the code)

# Load data from database when the program restarts
nifty_data = load_data_from_db('nifty_data', append_length)
banknifty_data = load_data_from_db('banknifty_data', append_length)

# ... (rest of the code)

# Schedule the background job to update data every 60 seconds
scheduler = BackgroundScheduler()
scheduler.add_job(update_data, 'interval', seconds=60)
scheduler.start()

# ... (rest of the code)

# ... (rest of the code)


@app.route('/')
def display_data():
  return render_template(
      'index.html',
      nifty_data=create_dataframe(nifty_data, "NIFTY").to_html(),
      banknifty_data=create_dataframe(banknifty_data, "BANKNIFTY").to_html(),
      nifty_signal=generate_buy_signal(nifty_data, "NIFTY")
      or generate_sell_signal(nifty_data, "NIFTY"),
      banknifty_signal=generate_buy_signal(banknifty_data, "BANKNIFTY")
      or generate_sell_signal(banknifty_data, "BANKNIFTY"))


@socketio.on('connect')
def handle_connect():
  emit(
      'update_data', {
          'nifty_df':
          create_dataframe(nifty_data, "NIFTY").to_html(),
          'banknifty_df':
          create_dataframe(banknifty_data, "BANKNIFTY").to_html(),
          'nifty_signal':
          generate_buy_signal(nifty_data, "NIFTY")
          or generate_sell_signal(nifty_data, "NIFTY"),
          'banknifty_signal':
          generate_buy_signal(banknifty_data, "BANKNIFTY")
          or generate_sell_signal(banknifty_data, "BANKNIFTY")
      })


if __name__ == '__main__':
  socketio.run(app, host='0.0.0.0', port=8080, debug=True)
