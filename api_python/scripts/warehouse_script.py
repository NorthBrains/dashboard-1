from kafka import KafkaProducer
import pandas as pd
from datetime import datetime
import json
import time

producer = KafkaProducer(bootstrap_servers='172.30.0.8:9092,172.30.0.9:9092,172.30.0.10:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

df = pd.read_csv('/scripts/data/warehouse_data.csv')

while True:
    df['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df = pd.concat([df.iloc[[-1]], df.iloc[:-1]]).reset_index(drop=True)

    record = df.iloc[0].to_dict()

    print("Wiadomość została wysłana do Kafki ✅")

    producer.send('warehouse', record)
    producer.flush()
    time.sleep(5)

producer.close()