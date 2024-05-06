from flask import Flask, jsonify
from kafka import KafkaProducer
import random
from datetime import datetime
import threading
import json
import time

app = Flask(__name__)

products = [
    {"category": "Beverages", "subcategories": [{"name": "Juices", "products": [{"name": "Orange Juice", "id": 1}, {"name": "Apple Juice", "id": 2}, {"name": "Multi-Vegetable Juice", "id": 3}]}, {"name": "Water", "products": [{"name": "Mineral Water", "id": 4}, {"name": "Spring Water", "id": 5}, {"name": "Carbonated Water", "id": 6}]}]},
    {"category": "Fruits and Vegetables", "subcategories": [{"name": "Fruits", "products": [{"name": "Apples", "id": 7}, {"name": "Bananas", "id": 8}, {"name": "Oranges", "id": 9}]}, {"name": "Vegetables", "products": [{"name": "Tomatoes", "id": 10}, {"name": "Cucumbers", "id": 11}, {"name": "Peppers", "id": 12}]}]},
    {"category": "Meat and Fish", "subcategories": [{"name": "Meat", "products": [{"name": "Chicken", "id": 13}, {"name": "Beef", "id": 14}, {"name": "Pork", "id": 15}]}, {"name": "Fish", "products": [{"name": "Salmon", "id": 16}, {"name": "Trout", "id": 17}, {"name": "Mackerel", "id": 18}]}]},
    {"category": "Dairy Products", "subcategories": [{"name": "Milk and Yogurts", "products": [{"name": "Milk", "id": 19}, {"name": "Plain Yogurt", "id": 20}, {"name": "Fruit Yogurt", "id": 21}]}, {"name": "Cheeses", "products": [{"name": "Yellow Cheese", "id": 22}, {"name": "White Cheese", "id": 23}, {"name": "Blue Cheese", "id": 24}]}]}
]

#inicjowanie producera Kafki
producer = KafkaProducer(
    bootstrap_servers='172.20.0.4:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_data_continuously():
    while True:
        new_rows = []
        for category in products:
            for subcategory in category["subcategories"]:
                for product in subcategory["products"]:
                    quantity = random.randint(1, 100)
                    new_rows.append({
                        "id": product["id"],
                        "quantity": quantity
                    })
        #wysłanie danych do topicu 'products_data'
        for row in new_rows:
            producer.send('products_data', value=row)
        
        producer.flush()
        
        time.sleep(3)
        print(new_rows)

@app.route('/start')
def start_data_generation():
    data_thread = threading.Thread(target=generate_data_continuously)
    data_thread.start()
    return jsonify({"status": "Data generation thread started"})


if __name__ == '__main__':
    data_thread = threading.Thread(target=generate_data_continuously)
    data_thread.start()
    app.run(debug=True, port=5001, threaded=True)