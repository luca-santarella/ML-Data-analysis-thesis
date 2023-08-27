import asyncio
import websockets
import json
import csv
import os
import gzip

async def gather(pairs):
    while True:
        async with websockets.connect('wss://api.pro.changelly.com/api/3/ws/public') as websocket:
            try:

                msg_json = {
                    "method": "subscribe",
                    "ch": "trades",                         
                    "params": {
                        "symbols": pairs,
                        "limit": 1
                    },
                    "id": 123
                }
                await websocket.send(json.dumps(msg_json))
                resp = await websocket.recv()
                data = json.loads(resp)
                print("\n")
                print(data)            
                    
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    if "ch" and "update" in data:
                        trade = data["update"]
                        pair = list(trade.keys())[0]
                        trade = trade[pair]
                        trade = trade[0]
                        print("\n")
                        print(trade)
                        with open(os.path.join("changelly","changelly_"+pair+".csv"),"a", newline="") as csv_file:
                            writer = csv.writer(csv_file)
                            writer.writerow([trade["t"], 
                                trade["q"],
                                trade["p"],
                                trade["s"],
                                trade["i"]])
                    
            except websockets.ConnectionClosed:
                continue
            




    
def main():

    pairs = ["BTCUSDT","BTCUSDC",
            "ETHUSDT","ETHUSDT",
            "ETHBTC","ADAUSDT","ADAUSDC",
            "XRPUSDT","XRPUSDC"]
          
    # for pair in pairs:
        # with open(os.path.join("changelly","changelly_"+pair+".csv"),"w+", newline="") as csv_file:
            # writer = csv.writer(csv_file)
            # writer.writerow(["date_ms","amount", "price", "type", "tid"]) 
        

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(gather(pairs))
    except Exception as e:
        print(e)
        loop.close()
        #finally:

if __name__ == "__main__":
    main()
    