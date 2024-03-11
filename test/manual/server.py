import json
from flask import Flask, request, jsonify
from flasgger import Swagger
import redis

app = Flask(__name__)
swagger = Swagger(app)

# Connect to Redis
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Endpoint to accept JSON and send it to Redis stream
@app.route('/send_to_redis', methods=['POST'])
def send_to_redis():
    """
    Endpoint to send JSON data to a Redis stream.
    ---
    tags:
      - Redis Stream
    parameters:
      - in: body
        name: body
        description: JSON data to be sent to Redis stream
        required: true
        schema:
          type: object
    responses:
      200:
        description: Successfully sent data to Redis stream
      400:
        description: Bad request - No JSON data provided
      500:
        description: Internal Server Error
    """
    try:
        data = request.json
        if not data:
            return jsonify({'error': 'No JSON data provided'}), 400

        # Example stream name
        stream_name = 'example_stream'

        # Convert JSON to string
        message = json.dumps(data)

        # Send message to Redis stream
        redis_client.xadd(stream_name, {
            'url': 'test_url',
            'article': message,
        })

        return jsonify({'success': True}), 200

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
