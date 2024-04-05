# from pyspark.sql import SparkSession
# from pyspark.sql.functions import explode, split
# from collections import deque

# class Bucket:
#     def __init__(self, size, timestamp):
#         self.size = size
#         self.timestamp = timestamp

# def update_window(window, bit, current_timestamp, window_size):
#     window.append(Bucket(bit, current_timestamp))
#     while len(window) > 2 and window[-1].timestamp - window[0].timestamp > window_size:
#         window.popleft()

# def estimate(window, current_timestamp, window_size):
#     count = 0
#     for bucket in window:
#         if current_timestamp - bucket.timestamp <= window_size and bucket.size == 1:
#             count += 1
#     return count

# def process_batch(df, current_timestamp, window, window_size):
#     batch_data = df.toPandas()
#     for row in batch_data.itertuples(index=False):
#         update_window(window, row.bit, current_timestamp, window_size)
#         count = estimate(window, current_timestamp, window_size)
#         print(f"Timestamp: {current_timestamp}, Bit: {row.bit}, Estimated Count: {count}")
#         current_timestamp += 1

# if __name__ == "__main__":
#     spark = SparkSession.builder \
#         .appName("DGIM Algorithm with PySpark and Websockets") \
#         .getOrCreate()

#     sc = spark.sparkContext
#     sc.setLogLevel("ERROR")

#     window_size = 18
#     current_timestamp = 0
#     window = deque()

#     lines = spark \
#         .readStream \
#         .format("socket") \
#         .option("host", "localhost") \
#         .option("port", 9000) \
#         .load()

#     bits = lines.select(
#         explode(split(lines.value, ",")) \
#             .alias("bit"))

#     bits = bits.select(bits.bit.cast("int").alias("bit"))

#     query = bits.writeStream \
#         .foreachBatch(lambda df, epoch_id: process_batch(df, current_timestamp, window, window_size)) \
#         .start()

#     query.awaitTermination()


import tkinter as tk
from collections import deque
import websocket
import threading

class Bucket:
    def __init__(self, size, timestamp):
        self.size = size
        self.timestamp = timestamp

class DGIM:
    def __init__(self, window_size):
        self.window_size = window_size
        self.dgim = deque(maxlen=window_size)
        self.current_timestamp = 0

    def updation(self):
        while self.dgim and self.current_timestamp - self.dgim[0].timestamp >= self.window_size:
            self.dgim.popleft()

    def estimate(self):
        count = 0
        for i, bucket in enumerate(self.dgim):
            if self.current_timestamp - bucket.timestamp <= self.window_size and bucket.size == 1:
                count += 1
        return count

    def update_window(self, bit):
        if bit == 1:
            self.dgim.append(Bucket(1, self.current_timestamp))
        else:
            self.dgim.append(Bucket(0, self.current_timestamp))
        self.updation()

    def on_message(self, position, bit):
        self.current_timestamp = position
        self.update_window(bit)
        count = self.estimate()
        return position, bit, count

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    print("### opened ###")

def main():
    window_size = 18
    dgim = DGIM(window_size)
    # websocket.enableTrace(True)
    ws = websocket.WebSocketApp("ws://localhost:9000/",
                                on_message=lambda ws, message: on_message(ws, message, dgim),
                                on_error=on_error,
                                on_close=on_close)
    ws.on_open = on_open

    # Start websocket in a separate thread to prevent blocking the GUI
    websocket_thread = threading.Thread(target=ws.run_forever)
    websocket_thread.daemon = True
    websocket_thread.start()

    # Create the GUI
    root = tk.Tk()
    root.title("DGIM Algorithm Client")

    label = tk.Label(root, text="DGIM Algorithm Client", font=("Helvetica", 16))
    label.pack(pady=10)

    status_label = tk.Label(root, text="Waiting for data...", font=("Helvetica", 12))
    status_label.pack(pady=5)

    bits_label = tk.Label(root, text="Last 18 bits: [0, 0, 0, ..., 0]", font=("Helvetica", 10))
    bits_label.pack(pady=10)

    # Define the function to handle message updates from DGIM and update GUI status
    def handle_message(position, bit, dgim):
        position, bit, count = dgim.on_message(position, bit)
        status_label.config(text=f"Received bit at {position}: {bit}\nEstimated count: {count}")

        # Update bits label with the last 18 bits
        bits = [bucket.size for bucket in dgim.dgim]
        bits_label.config(text=f"Last 18 bits: {bits}")

    def on_message(ws, message, dgim=dgim):
        data = message.split(",")
        position = int(data[0])
        bit = int(data[1])
        handle_message(position, bit, dgim)

    root.mainloop()

if __name__ == "__main__":
    main()

# from collections import deque
# import websocket
# import tkinter as tk
# import threading
# class Bucket:
#     def __init__(self, size, timestamp):
#         self.size = size
#         self.timestamp = timestamp

# class DGIM:
#     def __init__(self, window_size):
#         self.window_size = window_size
#         self.dgim = deque()
#         self.current_timestamp = 0

#     def updation(self):
#         while self.dgim and self.current_timestamp - self.dgim[0].timestamp >= self.window_size:
#             self.dgim.popleft()

#     def estimate(self):
#         count = 0
#         for i, bucket in enumerate(self.dgim):
#             if self.current_timestamp - bucket.timestamp <= self.window_size:
#                 count += 1
#         return count

#     def update_window(self, bit):
#         if bit == 1:
#             self.dgim.append(Bucket(1, self.current_timestamp))
#         self.updation()

#     def on_message(self, position, bit):
#         print(f"Received bit at {position}: {bit}")
#         self.current_timestamp = position
#         if bit == 1:
#             self.update_window(bit)
#         count = self.estimate()
#         print(f"Estimated count at {position}: {count}\n")

# def on_error(ws, error):
#     print(error)

# def on_close(ws):
#     print("### closed ###")

# def on_open(ws):
#     print("### opened ###")

# def main():
#     window_size = 32
#     dgim = DGIM(window_size)
#     # websocket.enableTrace(True)
#     ws = websocket.WebSocketApp("ws://localhost:9000/",
#                                 on_message=lambda ws, message: on_message(ws, message, dgim),
#                                 on_error=on_error,
#                                 on_close=on_close)
#     ws.on_open = on_open
#     websocket_thread = threading.Thread(target=ws.run_forever)
#     websocket_thread.daemon = True
#     websocket_thread.start()

#     root = tk.Tk()
#     root.title("DGIM Algorithm CLient")
#     label = tk.Label(root, text="DGIM Algorithm Client", font=("Helvetica", 16))
#     label.pack(pady=100)

#     status_label = tk.Label(root, text="Waiting for data...", font=("Helvetica", 12))
#     status_label.pack(pady=50)

#     def handle_message(position, bit):
#         position, bit, count = dgim.on_message(position, bit)
#         status_label.config(text=f"Received bit at {position}: {bit}\nEstimated count: {count}")

#     def on_message(ws, message):
#         data = message.split(",")
#         position = int(data[0])
#         bit = int(data[1])
#         handle_message(position, bit)


#     root.mainloop()
#    #  ws.run_forever()

# # def on_message(ws, message, dgim):
# #     data = message.split(",")
# #     position = int(data[0])
# #     bit = int(data[1])
# #     dgim.on_message(position, bit)

# if __name__ == "__main__":
#     main()

# import websockets
# from pyspark import SparkConf
# from pyspark.streaming import StreamingContext
# from collections import deque

# # Constants for the DGIM algorithm
# BUCKET_SIZE = 1000
# WINDOW_SIZE = 10000

# # DGIM algorithm function
# def dgim_algorithm(bit_stream):
#     buckets = deque()
#     count_1s = 0
#     for bit in bit_stream:
#         if bit == '1':
#             count_1s += 1
#             if len(buckets) > 0 and len(buckets) < BUCKET_SIZE:
#                 buckets.append(1)
#             else:
#                 buckets.append(1)
#                 if len(buckets) > BUCKET_SIZE:
#                     buckets.popleft()
#         else:
#             if len(buckets) > 0 and len(buckets) < BUCKET_SIZE:
#                 buckets.append(0)
#             else:
#                 buckets.append(0)
#                 if len(buckets) > BUCKET_SIZE:
#                     buckets.popleft()
#         if len(buckets) > BUCKET_SIZE:
#             count_1s -= buckets.popleft()
#     estimate = 0
#     while len(buckets) > 1:
#         estimate += 1
#         buckets.pop()
#     estimate = estimate + (0.5 if len(buckets) == 1 else 0)
#     return int(estimate)

# def receive_data(websocket, ssc):
#     print("Connected to the WebSocket server.")
#     while True:
#         data = websocket.recv()
#         position, bit = map(int, data.split(','))
#         print(f"Received data: Position={position}, Bit={bit}")
#         # Implement DGIM algorithm using Spark
#         bit_stream = ssc.sparkContext.parallelize([str(bit)])
#         dstream = ssc.queueStream([bit_stream])
#         windowed_dstream = dstream.window(WINDOW_SIZE, WINDOW_SIZE)
#         estimated_count = windowed_dstream.map(lambda x: dgim_algorithm(x)).reduce(lambda x, y: x + y)
#         print(f"Estimated count of 1s in last {WINDOW_SIZE} bits: {estimated_count}")

# # Initialize SparkContext
# conf = SparkConf().setAppName("DGIM Algorithm")
# ssc = StreamingContext(conf, 1)  # 1 second batch interval

# # Connect to the WebSocket server
# async def connect_to_websocket():
#     async with websockets.connect('ws://localhost:9000') as websocket:
#         receive_data(websocket, ssc)

# ssc.start()
# ssc.awaitTermination()
