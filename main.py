import os
import time
import requests
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_db():
    return psycopg2.connect(
        host='db',
        database='weather_data',
        user='ria_admin',
        password='ahjduii8wuheBNye!@',
        port=5432
    )
def db_setup():
    conn = None
    while True:
        try:
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather (
                    id SERIAL PRIMARY KEY,
                        city VARCHAR(50),
                        degree FLOAT,
                        humidity FLOAT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    """)
        except Exception as e:
            print(f"Error setting up database: {e}")
        finally:
            if conn:
                conn.close()
                
def fetch_weather_data(API_key, city):
    conn = None
    while True:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_key}"
            response = requests.get(url).json()
            degree = response['main']['temp']
            humidity = response['main']['humidity']

            cur = conn.cursor()
            cur.execute(
                "INSERT INTO weather (city, degree, humidity) VALUES (%s, %s)",
                (city, degree, humidity)
            )
            print(f"Reported weather data for {city}: Degree={degree}, Humidity={humidity}")
        except Exception as e:
            print(f"Error fetching or inserting weather data: {e}")


            conn.commit()
            cur.close()
            conn.close()

if __name__ == "__main__":
    db_setup()
    API_key = os.getenv("API_KEY")
    city = "New York"
    lat = "40.7128"
    lon = "-74.0060"
    while True:
        fetch_weather_data(API_key, city)
        time.sleep(300)  # Fetch data every 5 minutes
