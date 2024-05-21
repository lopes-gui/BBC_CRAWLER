# import the necessary libraries

from flask import Flask, request, jsonify
from google.cloud import bigquery
import os

app = Flask(__name__)

# Initialize BigQuery 
client = bigquery.Client()

#  table details
PROJECT_ID = 'NEWSDATA'
DATASET_ID = 'newsdata'
TABLE_ID = 'database'


# function to search by keyworss
@app.route('/search', methods=['GET'])
def search_by_keyword():
    keyword = request.args.get('keyword')
    if not keyword:
        return jsonify({"error": "Keyword parameter is required"}), 400
    
    query = f"""
    SELECT title, content, author, url, publish_date
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    WHERE LOWER(content) LIKE '%{keyword.lower()}%'
    """
    
    query_job = client.query(query)
    results = query_job.result()

    articles = []
    for row in results:
        articles.append({
            "title": row.title,
            "content": row.content,
            "author": row.author,
            "url": row.url,
            "publish_date": row.publish_date
        })

    return jsonify(articles), 200

if __name__ == '__main__':
    app.run(debug=True)
