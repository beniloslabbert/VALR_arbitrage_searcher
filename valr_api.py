import json
import time
import hashlib
import hmac
import requests
import pandas as pd

from key import valr_key as my_keys


class ValrAPI:
    def __init__(self):
        self.pub_key = my_keys.public
        self.sec_key = my_keys.secret

    def sign_request(self, timestamp, verb, path, body=""):
        """Signs the request payload using the api key secret
        api_key_secret - the api key secret
        timestamp - the unix timestamp of this request e.g. int(time.time()*1000)
        verb - Http verb - GET, POST, PUT or DELETE
        path - path excluding host name, e.g. '/api/v1/withdraw
        body - http request body as a string, optional
        """
        payload = "{}{}{}{}".format(timestamp, verb.upper(), path, body)
        message = bytearray(payload, 'utf-8')
        self.signature = hmac.new(bytearray(self.sec_key, 'utf-8'), message, digestmod=hashlib.sha512).hexdigest()
        return self.signature

    def get_order_book(self, curr_pair):

        self.order_book_url = "https://api.valr.com/v1/public/{}/orderbook".format(str(curr_pair))
        payload = {}
        headers = {}

        response = requests.request("GET", self.order_book_url, headers=headers, data=payload)

        return dict(json.loads(response.text))

    def market_summary(self):
        url = "https://api.valr.com/v1/public/marketsummary"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        return json.loads(response.text)

    def market_summary_pair(self, curr_pair):
        url = f"https://api.valr.com/v1/public/{curr_pair}/marketsummary"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        return dict(json.loads(response.text))

    def extract_ask_bid(self, curr_pair):
        pair = self.market_summary_pair(curr_pair)

        return float(pair['askPrice']), float(pair['bidPrice'])

    def get_currencies(self):

        url = "https://api.valr.com/v1/public/currencies"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        return json.loads(response.text)

    def get_currency_pairs(self):

        url = "https://api.valr.com/v1/public/pairs"

        payload = {}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)

        return json.loads(response.text)

    def concat_OB(self):
        # get_market_data
        market_data = ValrAPI()
        book = market_data.get_order_book('BTCZAR')

        asks = pd.DataFrame(book['Asks'])
        bids = pd.DataFrame(book['Bids'])

        full_book = pd.concat([asks, bids]).reset_index(drop=True)

        full_book['Timestamp'] = book['LastChange']

        print(full_book)
