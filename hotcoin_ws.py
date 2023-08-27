import asyncio
import websockets
import json
import csv
import os
import gzip

async def gather(pairs):
    while True:
        async with websockets.connect('wss://wss.hotcoinfin.com/trade/multiple') as websocket:
            try:
                
                for pair in pairs:
                    msg_json = {"sub":"market."+pair+".trade.detail"}
                    await websocket.send(json.dumps(msg_json))
                    resp = await websocket.recv()
                    resp = gzip.decompress(resp)
                    

                    data = resp.decode("utf-8")  
                    data = json.loads(data)
                    print("\n")
                    print(resp)
                    
                while True:
                    resp = await websocket.recv()
                    resp = gzip.decompress(resp)

                    data = resp.decode("utf-8")  
                    data = json.loads(data)
                    print("\n")
                    print(resp)
                    if "data" in data:
                        pair = data["ch"].replace("market.","")
                        pair = pair.replace(".trade.detail","")
                        data = data["data"]
                        
                        print("\n")
                        print(data)
                        for trade in data:
                            with open(os.path.join("hotcoin","hotcoin_"+pair+".csv"),"a", newline="") as csv_file:
                                writer = csv.writer(csv_file)
                                writer.writerow([trade["ts"], 
                                    trade["amount"],
                                    trade["price"],
                                    trade["direction"],
                                    trade["tradeId"]])
                    
            except websockets.ConnectionClosed:
                continue
            




    
def main():

    pairs = ["btc_usdt","btc_usdc",
            "eth_usdt","eth_usdc",
            "eth_btc","ada_usdt","ada_usdc",
            "xrp_usdt","xrp_usdc"]
          
    # for pair in pairs:
        # with open(os.path.join("hotcoin","hotcoin_"+pair+".csv"),"w+", newline="") as csv_file:
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
    