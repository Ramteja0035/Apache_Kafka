from kafka import KafkaConsumer
import json
import pandas as pd
import csv
import os
from collections import Counter

# Kafka Consumer setup
consumer = KafkaConsumer(
    'demo-topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    group_id='demo-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

file_path = "submissions.csv"

# Ensure CSV file exists
if not os.path.exists(file_path):
    with open(file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Name", "Age", "Gender", "Country"])

print("Waiting for messages...")

for message in consumer:
    data = message.value
    print(f"Received: {data}")

    # Save to CSV
    with open(file_path, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow([data['name'], data['age'], data['gender'], data['country']])

    # Load all data for analysis
    df = pd.read_csv(file_path)

    # 1️⃣ Gender stats
    gender_counts = df['Gender'].value_counts().to_dict()

    # 2️⃣ Country stats
    country_counts = df['Country'].value_counts().to_dict()

    # 3️⃣ Age group stats
    bins = [0, 18, 25, 35, 50, 100]
    labels = ['<18', '18–25', '26–35', '36–50', '50+']
    df['AgeGroup'] = pd.cut(df['Age'], bins=bins, labels=labels, right=False)
    age_group_percentage = (df['AgeGroup'].value_counts(normalize=True) * 100).round(2).to_dict()

    # Print live analytics
    print("\n📊 --- Live Statistics ---")
    print(f"Total submissions: {len(df)}")
    print()
    print("Gender counts:")
    for i in gender_counts:
        print(i," : ", gender_counts[i])
    print()
    print("Country counts:")
    for i in country_counts:
        print(i," : ", country_counts[i])
    print()
    print("Age group :")
    for i in age_group_percentage:
        print(i," : ", age_group_percentage[i],"%")
    print("-----------------------------\n")
