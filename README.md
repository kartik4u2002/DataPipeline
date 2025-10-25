# 🧩 Real-Time Data Pipeline — Kafka + Python + MongoDB Atlas

This project is a **real-time data pipeline** built using **Kafka (Python API)** and **MongoDB Atlas**.  
It continuously fetches live data from **NewsAPI**, processes and transforms it using Python, and then streams the refined data to **MongoDB Atlas** for storage and analysis.

---

## 🚀 Overview

This pipeline demonstrates how to:
1. Fetch **real-time data** from an external API (NewsAPI).
2. Perform **data transformation and cleaning** using Python.
3. Stream the transformed data to a **Kafka topic**.
4. Consume the data from Kafka and store it in **MongoDB Atlas**.

---

## ⚙️ Architecture

NewsAPI → Python Producer → Kafka Topic → Python Consumer → MongoDB Atlas


Each stage of the pipeline is modular and can be scaled or modified independently.

---

## 🧠 Features

- Real-time news ingestion from [NewsAPI](https://newsapi.org/).  
- Lightweight data transformation in Python.  
- Asynchronous message passing via Kafka topics.  
- Persistent storage in MongoDB Atlas.  
- Environment-based configuration for flexibility.  
- Clean, modular code structure.

---

## 🧰 Tech Stack

- **Language:** Python  
- **Message Broker:** Apache Kafka  
- **Database:** MongoDB Atlas  
- **API Source:** NewsAPI  
- **Libraries:** `kafka-python`, `requests`, `pymongo`, `python-dotenv`

---

## 🧩 Project Structure

├── README.md
├── requirements.txt
├── .env
├── src/
│ ├── producer.py # Fetches and publishes data to Kafka
│ ├── consumer.py # Consumes data and stores in MongoDB
│ ├── transformer.py # Handles data transformation
│ ├── config.py # Loads environment variables
│ └── utils.py # Helper functions (optional)
└── docker-compose.yml # For running Kafka locally (optional)


