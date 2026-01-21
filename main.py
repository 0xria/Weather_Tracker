import os
import time
import requests
import psycopg2 #standard bridge b/w python & postgresql2
from dotenv import load_dotenv

load_dotenv()
WEATHER_API = os.getenv("WEATHER_API")
DB_PASSWORD = os.getenv("DB_PASSWORD")

def get_db():
    """Builds and returdn a connection to the PostgreSQL DB..."""
    try:
        return psycopg2.connect(
            host='127.0.0.1',
            database='weather_data',
            user='ria_admin',
            password=('DB_PASSWORD'),
            port=5432
        )
    except Exception as e:
        print(f"Connection failed: {e}")
        return  None
    
def db_setup():
    """ Creates the weather table if it's not in existence..."""
    conn = get_db()
    if conn:
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS weather (
                    id SERIAL PRIMARY KEY,
                        city VARCHAR(50),
                        degree FLOAT,
                        humidity FLOAT,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);
                    """)
            conn.commit() #save table creation
            print("Db setup complete...")
        except Exception as e:
            print(f"Error setting up database: {e}")
        finally:
                conn.close()
                
#weather track tools....
def fetch_weather_data(city_name):
    input("Enter City Name: ")
    url = f"https://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit=1&appid={WEATHER_API}"
    try:
        response = requests.get(url).json()
        if response:
             return response[0]['lat'], response[0]['lon']
        return None, None
    except Exception as e:
        print(f"Geocoding Error ❌: {e}")
        return None, None
    
def fetch_and_save_weather(city, lat, lon):
    """Gets weather from API and saves it to Postgres."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_API}&units=metric"
    conn = get_db()
    
    if not conn: return

    try:
        data = requests.get(url).json()
        temp = data['main']['temp']
        hum = data['main']['humidity']

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO weather (city, degree, humidity) VALUES (%s, %s, %s)",
            (city, temp, hum)
        )
        conn.commit()
        print(f"☁️  {city.capitalize()}: {temp}°C | {hum}% Humidity (Saved)")
    except Exception as e:
        print(f"❌ Error during weather cycle: {e}")
    finally:
        conn.close()

def get_coords(city_name):
    """Translates a city name into Latitude and Longitude."""
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name}&limit=1&appid={WEATHER_API}"
    try:
        response = requests.get(url).json()
        if response:
            return response[0]['lat'], response[0]['lon']
        return None, None
    except Exception as e:
        print(f"❌ Geocoding Error: {e}")
        return None, None

def fetch_and_save_weather(city, lat, lon):
    """Gets weather from API and saves it to Postgres."""
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={WEATHER_API}&units=metric"
    conn = get_db()
    
    if not conn: return

    try:
        data = requests.get(url).json()
        temp = data['main']['temp']
        hum = data['main']['humidity']

        cur = conn.cursor()
        cur.execute(
            "INSERT INTO weather (city, degree, humidity) VALUES (%s, %s, %s)",
            (city, temp, hum)
        )
        conn.commit()
        print(f"☁️  {city.capitalize()}: {temp}°C | {hum}% Humidity (Saved)")
    except Exception as e:
        print(f"❌ Error during weather cycle: {e}")
    finally:
        conn.close()

#main
if __name__ == "__main__":
    db_setup()
    print("\n-- 🌦️ Weather Tracker System... ---")
    target_city = input("Enter the city name you want to track: ").strip()

    lat, lon = get_coords(target_city)

    if lat and lon:
        print(f" Target locked: {target_city} ({lat}, {lon})")
        print("Press Ctrl+C to terminate tracker... \n")
        try:
            while True:
                fetch_and_save_weather(target_city, lat, lon)
                time.sleep(300) #during 5 minjute intervals
        except KeyboardInterrupt:
            print("\n System Shut down successfully...")
    else:
        print("Critical: Couldn't find that location. Please check the spelling...")