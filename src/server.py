from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/analyze', methods=['POST'])
def analyze_article():
    article = request.get_json().get('article')

    # Analyze the article here
    analyzed_article = analyze(article)

    return jsonify({'article': analyzed_article})

if __name__ == '__main__':
    app.run()