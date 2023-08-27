import asyncio
import websockets
import json
import csv
import os

async def gather(pairs):
    while True:
        async with websockets.connect('wss://ws.bitmex.com/realtime') as websocket:
            try:    
                response = await websocket.recv()
                print(f"first resp: {response}")
                
                args =  ["trade:"+pair for pair in pairs]
                
                await websocket.send(json.dumps({"op": "subscribe", "args": args}))
                response = await websocket.recv()
                

                while True:
                    resp = await websocket.recv()
                    print("\n\n")
                    print(resp)
                    if "data" in json.loads(resp):
                        data = json.loads(resp)["data"]
                        if len(data) > 0:
                            pair = data[0]["symbol"]
                            
                            with open(os.path.join("bitmex","bitmex_"+pair+".csv"),"a", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                for trade in json.loads(resp)["data"]:
                                    writer.writerow([trade["timestamp"], 
                                        trade["size"],
                                        trade["price"],
                                        trade["side"],
                                        trade["trdMatchID"]])
            except websockets.ConnectionClosed:
                continue
            
            
            




    
def main():

    pairs = ["XBTUSD","XBTUSDT","ETHUSDT", "ETHUSD",
        "ETHXBT", "ADAUSDT", "ADAUSD",
        "XRPUSDT", "XRPUSD"]
          
    # for pair in pairs:
        # with open(os.path.join("bitmex","bitmex_"+pair+".csv"),"w+", newline="") as csv_file:
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
    

