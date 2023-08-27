import asyncio
import websockets
import json
import csv
import os

async def gather(pairs):
    while True:
        async with websockets.connect('wss://ws.btse.com/ws/spot') as websocket:
            try:
            
                msg_json = {
                    "op":"subscribe",
                    "args":["tradeHistoryApi:BTC-USD","tradeHistoryApi:BTC-USDT"
                        "tradeHistoryApi:BTC-USDC","tradeHistoryApi:ETH-BTC",
                        "tradeHistoryApi:ETH-USDT", "tradeHistoryApi:ETH-USD",
                        "tradeHistoryApi:ETH-USDC", "tradeHistoryApi:XRP-USD",
                        "tradeHistoryApi:XRP-USDT", "tradeHistoryApi:XRP-USDC",
                        "tradeHistoryApi:ADA-USD", "tradeHistoryApi:ADA-USDT",
                        "tradeHistoryApi:ADA-USDC"]
                }
                
                await websocket.send(json.dumps(msg_json))
                resp = await websocket.recv()
                print(resp)
                    
                while True:
                    resp = await websocket.recv()
                    resp = json.loads(resp)
                    trades = resp["data"]
                    
                    print("\n")
                    print(trades)   

                    pair = trades[0]["symbol"]
                    with open(os.path.join("btse","btse_"+pair+".csv"),"a", newline="") as csv_file:
                        writer = csv.writer(csv_file)
                        for trade in trades:
                            writer.writerow([trade["timestamp"], 
                                trade["size"],
                                trade["price"],
                                trade["side"],
                                trade["tradeId"]])
                    
            except websockets.ConnectionClosed:
                continue
            




    
def main():

    pairs = ["BTC-USD","BTC-USDT","BTC-USDC","ETH-USDT","ETH-USDC", "ETH-USD",
            "ETH-BTC","ADA-USDT","ADA-USD", "ADA-USDC","XRP-USDT","XRP-USDC", "XRP-USD"]
          
    # for pair in pairs:
        # with open(os.path.join("btse","btse_"+pair+".csv"),"w+", newline="") as csv_file:
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
    