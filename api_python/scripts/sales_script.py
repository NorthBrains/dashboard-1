import pandas as pd
import time
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
    for (product, location), group in df.groupby(['Item Purchased', 'Location']):
        last_row_index = group.index[-1]
        last_row = group.loc[last_row_index]  # bierzemy ostatni wiersz z danymi dla danego produktu dla danej lokalizacji

        new_row = last_row.copy()
        new_row['timestamp'] = datetime.now().isoformat()

        updated_df = updated_df.drop(last_row_index)  # usuwamy ostatnie dane dla tego dnia
        updated_df = pd.concat([updated_df, pd.DataFrame([new_row])], ignore_index=True)

        enriched_product = new_row.to_dict()
        producer.produce('products_data', key=str(last_row['Product ID']), value=json.dumps(enriched_product), callback=delivery_report)
        producer.poll(0)

        logging.info(f"Updated {product} in {location}: {new_row.to_dict()}")

    return updated_df

# Główna funkcja
def main():
    file_path = 'data/sales_data.csv'
    while True:
        data = pd.read_csv(file_path)
        updated_data = update_data(data)
        #updated_data.to_csv(file_path, index=False)
        producer.flush()
        print(updated_data)
        time.sleep(5)

if __name__ == "__main__":
    main()