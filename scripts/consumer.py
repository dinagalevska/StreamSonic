# from quixstreams import Application
# import json
# import pandas as pd

# app = Application(
#     broker_address="localhost:9092",
#     loglevel="DEBUG",
#     consumer_group="music_events_processor",
#     auto_offset_reset="earliest",
# )

# with app.get_consumer() as consumer:
#     msgs = []
#     consumer.subscribe(topics=["listen_events"])
#     print("Subscribing to topic: listen_events")

#     while True:
#         # Poll for new messages
#         msg = consumer.poll(1)
#         if msg is not None:
#             # Check if the message has value and decode it
#             if msg.value() is not None:
#                 # Append the decoded message to the list
#                 msgs.append(json.loads(msg.value().decode('utf-8')))
#                 print(f"Message received: {msgs[-1]}")  # Log the received message
#             else:
#                 print("Received empty message")
#         else:
#             print("No message received, continuing...")
        
#         # Add a break condition after a certain number of messages
#         if len(msgs) >= 100000:  # Set your own limit
#             break

# # Check if msgs contains data before creating DataFrame
# if msgs:
#     df = pd.DataFrame(msgs)
#     df.to_csv('events.csv', index=False)
#     print(f"Saved {len(msgs)} messages to events.csv")
# else:
#     print("No messages to write to CSV.")
from quixstreams import Application
import json
import pandas as pd

list_of_topics = [
    "auth_events",
    "listen_events",
    "page_view_events",
    "status_change_events",
]

app = Application(
    broker_address="localhost:9092",
    loglevel="DEBUG",
    consumer_group="music_events_processor",
    auto_offset_reset="earliest",
)

data_dict = {k: [] for k in list_of_topics}

with app.get_consumer() as consumer:
    consumer.subscribe(topics=list_of_topics)

    while len(data_dict["listen_events"]) < 100000:
        msg = consumer.poll(1)
        if msg is not None and msg.value() is not None:
            topic = msg.topic()
            data_dict[topic].append(json.loads(msg.value().decode("utf-8")))
            print(f"Message received from {topic}: {data_dict[topic][-1]}")

for topic, data in data_dict.items():
    df = pd.DataFrame(data)
    df.to_csv(f"data/{topic}.csv", index=False)

print("Data collection complete. CSV files have been saved.")
