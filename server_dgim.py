import asyncio
import websockets
import random
import time

async def send_data(websocket, path):
    print("Websocket connection established ")
    position =1
    while True:
        # Generate random timestamp and bit (0 or 1)
        
      #   timestamp = int(time.time())
        bit = random.choice([0, 1])  
        data =f"{position},{bit}"
        print(f"Sending data : {data}")
        # Send the data to the client
        await websocket.send(f"{position},{bit}")
        position+=1
        # Wait for some time before sending the next data
        await asyncio.sleep(1)

# Start the WebSocket server
start_server = websockets.serve(send_data, "localhost", 9000)

# Run the server indefinitely
asyncio.get_event_loop().run_until_complete(start_server)
asyncio.get_event_loop().run_forever()
