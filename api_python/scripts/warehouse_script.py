import pandas as pd
import numpy as np
import time
import random
import json
from confluent_kafka import Producer
import logging
from datetime import datetime

def create_producer():
    conf = {'bootstrap.servers': "localhost:9092"}
    producer = Producer(**conf)
    return producer

producer = create_producer()

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


def update_data(df):
    updated_df = df.copy()
    for (product, location), group in df.groupby(['Product Name', 'Location']):
        last_row_index = group.index[-1]
        last_row = group.loc[last_row_index] # bierzemy ostatni wiersz z danymi dla danego produktu dla danego miasta

        new_row = last_row.copy()
        for column in ['Quantity in Stock', 'Average Delivery Time (days)', 'Stock Value (USD)', 'Shelf Life (days)', 'Reorder Level', 'Daily Sales (units)']:
            change = random.uniform(0.1, 0.3) * (1 if random.choice([True, False]) else -1)
            new_row[column] = last_row[column] * (1 + change) #randomizujemy dane z tego dnia i tworzymy kopie i wstawiamy 
        
        new_row['timestamp'] = datetime.now().isoformat()

        updated_df = updated_df.drop(last_row_index) #usuwamy ostatnie dane dla tego dnai
        updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)

        enriched_product = new_row.to_dict()
        producer.produce('products_data', key=str(last_row['Product ID']), value=json.dumps(enriched_product), callback=delivery_report)
        producer.poll(0)

        logging.info(f"Updated {product} in {location}: {new_row.to_dict()}")

    return updated_df

def main():
    file_path = 'data/magazyn_data.csv'
    while True:
        data = pd.read_csv(file_path)
        updated_data = update_data(data)
        producer.flush()
        print(updated_data)
        time.sleep(5)

if __name__ == "__main__":
    main()