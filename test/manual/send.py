import json
import redis
import sys
import os

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def send_to_redis_from_file(json_file_path, stream_name):
    try:
        # Read JSON file
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        # Convert JSON to string
        message = json.dumps(json_data)

        # Send message to Redis stream
        redis_client.xadd(stream_name, {'done': message})

        print("Data sent to Redis stream successfully.")

    except Exception as e:
        print("Error:", e)

def send_to_redis(data, stream_name):
    try:
        # Convert JSON to string
        message = json.dumps(data)

        # Send message to Redis stream
        redis_client.xadd(stream_name, {'done': message})

        print("Data sent to Redis stream successfully.")

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
 
    # file = sys.argv[1]

    stream = os.getenv("REDIS_STREAM_NAME", "scraper_articles")
    # send_to_redis_from_file(file, stream)

    d = [
    {
        "id": "38cde5a8f2284b4390c8cf0f121ff6a8", 
        "url": "https://abcnews.go.com/Business/boeing-ceo-dave-calhoun-step/story?id=108465621", 
        "scrape_time": "2024-03-26T19:23:29.071263"
    }, 
    {
        "id": "63606bc548fb4154bd34e9aec31cfacc", 
        "url": "https://abcnews.go.com/Business/cargo-ship-bridge-crash-baltimore-harbor-means-economy/story?id=108506177", 
        "scrape_time": "2024-03-26T19:23:29.770078"
    }, 
    {
        "id": "65eb3a76a6a446f588fa3b7b3998dc00", 
        "url": "https://abcnews.go.com/Business/massive-ship-bali-crashed-baltimores-francis-scott-key/story?id=108505848",
        "scrape_time": "2024-03-26T19:23:30.189892"
    }]

    send_to_redis(d, stream)

