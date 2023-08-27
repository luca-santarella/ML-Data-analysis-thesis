import asyncio
import websockets
import json
import csv
import os

async def gather(pairs):
    while True:
        async with websockets.connect('wss://ws.kraken.com') as websocket:
            try:    
                response = await websocket.recv()
                print(f"first resp: {response}")
                
                msg_json = {
                  "event": "subscribe",
                  "pair": pairs,
                  "subscription": {
                    "name": "trade"
                  }
                }
                
                await websocket.send(json.dumps(msg_json))
                response = await websocket.recv()

                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    print(data)
                    if "trade" in data:
                        trades = data[1]
                        pair = data[-1]
                        with open(os.path.join("kraken","kraken_"+pair.replace('/','_')+".csv"),"a", newline="") as csv_file:
                            writer = csv.writer(csv_file)
                            for trade in trades:
                                trade_type = "null"
                                if trade[3] == 'b':
                                    trade_type = "buy"
                                else:
                                    trade_type = "sell"
                                time = trade[2]
                                volume = trade[1]
                                price = trade[0]
                                writer.writerow([time, 
                                    volume,
                                    price,
                                    trade_type])
            except websockets.ConnectionClosed:
                continue




    
def main():

    pairs = ["XBT/USD","XBT/USDT","XBT/USDC",
        "ETH/USDT", "ETH/USDC", "ETH/USD",
        "ETH/XBT", "ADA/USDT", "ADA/USD",
        "XRP/USDT", "XRP/USD"]
          
    # for pair in pairs:
        # with open(os.path.join("kraken","kraken_"+pair.replace('/','_')+".csv"),"w+", newline="") as csv_file:
            # writer = csv.writer(csv_file)
            # writer.writerow(["date_ms","amount", "price", "type"]) 
        

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(gather(pairs))
    except Exception as e:
        print(e)
        loop.close()
        #finally:

if __name__ == "__main__":
    main()
    