import asyncio
import websockets
import json
import csv
import os

async def gather(pairs):
    while True:
        async with websockets.connect('wss://data-stream.binance.com/ws') as websocket:
            try:
            
                msg_json = {
                    "method": "SUBSCRIBE",
                    "params":
                    [
                    "btcusdt@trade",
                    "btcusdc@trade",
                    "ethusdt@trade",
                    "ethusdc@trade",
                    "ethbtc@trade",
                    "xrpusdt@trade",
                    "xrpusdc@trade",
                    "adausdt@trade",
                    "adausdc@trade",
                    ],
                    "id": 1
                }
                
                await websocket.send(json.dumps(msg_json))
                resp = await websocket.recv()
                print(resp)
                    
                while True:
                    resp = await websocket.recv()
                    data = json.loads(resp)
                    
                    print("\n")
                    print(data)   

                    if data["e"] == "trade":
                        pair = data["s"]
                        with open(os.path.join("binance","binance_"+pair+".csv"),"a", newline="") as csv_file:
                            writer = csv.writer(csv_file)
                            writer.writerow([data["T"], 
                                data["q"],
                                data["p"],
                                data["t"]])
                    
            except websockets.ConnectionClosed:
                continue
            




    
def main():

    pairs = ["BTCUSDT","BTCUSDC","ETHUSDT","ETHUSDT",
            "ETHBTC","ADAUSDT","ADAUSDC","XRPUSDT","XRPUSDC",]
          
    # for pair in pairs:
        # with open(os.path.join("binance","binance_"+pair+".csv"),"w+", newline="") as csv_file:
            # writer = csv.writer(csv_file)
            # writer.writerow(["date_ms","amount", "price", "tid"]) 
        

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(gather(pairs))
    except Exception as e:
        print(e)
        loop.close()
        #finally:

if __name__ == "__main__":
    main()
    