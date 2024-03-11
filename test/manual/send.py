import json
import redis
import sys
import os

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

def send_to_redis(json_file_path, stream_name):
    try:
        # Read JSON file
        with open(json_file_path, 'r') as file:
            json_data = json.load(file)

        # Convert JSON to string
        message = json.dumps(json_data)

        # Send message to Redis stream
        redis_client.xadd(stream_name, {'article': message})

        print("Data sent to Redis stream successfully.")

    except Exception as e:
        print("Error:", e)

if __name__ == "__main__":
 
    file = sys.argv[1]

    stream = os.getenv("REDIS_STREAM_NAME", "scraped_articles")

    send_to_redis(file, stream)
