import requests


class OptionChainFetcher:

  def __init__(
      self,
      symbol,
      url="https://groww.in/v1/api/option_chain_service/v1/option_chain"):
    self.symbol = symbol
    self.url = f"{url}/{symbol}"
    self.latest_data = None

  def fetch_data(self, expiry_date):
    full_url = f"{self.url}?expiry={expiry_date}"
    response = requests.get(full_url)
    if response.status_code == 200:
      self.latest_data = response.json()

  def call_ltp(self, expiry_date, user_input_strike):
    if self.latest_data is None:
      self.fetch_data(expiry_date)

    option_chain_entry = next(
        (option for option in self.latest_data['optionChains']
         if option['strikePrice'] == user_input_strike), None)

    return option_chain_entry['callOption'][
        'ltp'] if option_chain_entry else None

  def put_ltp(self, expiry_date, user_input_strike):
    if self.latest_data is None:
      self.fetch_data(expiry_date)

    option_chain_entry = next(
        (option for option in self.latest_data['optionChains']
         if option['strikePrice'] == user_input_strike), None)

    return option_chain_entry['putOption'][
        'ltp'] if option_chain_entry else None
