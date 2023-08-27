import asyncio
import websockets
import json
import csv
import os
import gzip

async def gather(pairs):
    while True:
        async with websockets.connect('wss://ws.hotbit.io') as websocket:
            try:
                
                msg_json = {
                    "method":"deals.subscribe", 
                    "params":pairs, 
                    "id":100
                }
                

                await websocket.send(json.dumps(msg_json))
                    
                while True:
                    resp = await websocket.recv()
                    resp = gzip.decompress(resp)
                    
                    print("\n")
                    data = resp.decode("utf-8")  
                    data = json.loads(data)
                    if "params" in data:
                        data = data["params"]
                        pair = data[0]
                        print("\n")
                        print(data)
                        trades = data[1]
                        for trade in trades:
                            with open(os.path.join("hotbit","hotbit_"+pair+".csv"),"a", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                writer.writerow([trade["time"], 
                                    trade["amount"],
                                    trade["price"],
                                    trade["type"],
                                    trade["id"]])
                    
            except websockets.ConnectionClosed:
                continue
            




    
def main():

    pairs = ["BTCUSDT","BTCUSDC",
            "ETHUSDT","ETHUSDT",
            "ETHBTC","ADAUSDT","ADAUSDC",
            "XRPUSDT","XRPUSDC"]
          
    # for pair in pairs:
        # with open(os.path.join("hotbit","hotbit_"+pair+".csv"),"w+", newline="") as csv_file:
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
    