from pycoingecko import CoinGeckoAPI
import json
import time 
import csv
import os
import datetime

cg = CoinGeckoAPI()
bitcoin_id = 'bitcoin'
usd_id = 'usd'
DIR_PATH = '/home/pi/Desktop/crypto-scraping/exchange_data/'
date = str(datetime.datetime.now())
list_ex = cg.get_exchanges_list() #returns a list of the top exchanges with additional info

#writes exchange list in a JSON file
with open(f"/home/pi/Desktop/crypto-scraping/exchange_lists/{date}.json", "w+") as json_file:
    json_file.write(json.dumps(list_ex))

#get current BTC price in USD
btc_price_dict = cg.get_price(ids=bitcoin_id, vs_currencies='usd')[bitcoin_id]
btc_price = btc_price_dict[usd_id]

for exchange in list_ex:
	exchange_id = exchange['id'] #id used internally in the CoinGecko API (e.g. 'binance')
	path_file = DIR_PATH+exchange_id+".csv"

	try:
		with open(path_file,"a", newline="") as csv_file:
			writer = csv.writer(csv_file)
			if os.path.getsize(path_file) == 0: #write header in case file is empty
				print("Writing header")
				header = ["Date","Volume (USD)","Normalized Volume (USD)"]
				writer.writerow(header)
			print(f"Writing row for {exchange_id}..")
			row = [date, exchange['trade_volume_24h_btc'] * btc_price, exchange['trade_volume_24h_btc_normalized'] * btc_price]
			writer.writerow(row)
	except Exception as ex:
		telegram_bot_sendtext("Something went wrong.. Please check")
		print(ex)
		
print("Writing was successful!")




