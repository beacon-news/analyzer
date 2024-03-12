from article_store.elasticsearch_store import ElasticsearchStore
from elasticsearch import Elasticsearch
import json
from datetime import datetime
import uuid

if __name__ == '__main__':

  user = "elastic"
  passwd = "password"

  es = Elasticsearch("https://localhost:9200", basic_auth=(user, passwd), 
                     ca_certs="../certs/_data/ca/ca.crt")


  with open("../test/manual/art.json") as f:
    doc = json.load(f)

  titles = []
  paras = []
  authors = []
  publish_date = None
  for c in doc["components"]["article"]:
    if 'title' in c:
      titles.append(c['title'])
    elif 'paragraphs' in c:
      paras.extend(c['paragraphs'])
    elif 'author' in c:
      authors.append(c['author'])
    if 'publish_date' in c:
      publish_date = c['publish_date']
  
  # title = "\n".join(titles)
  # text = "\n".join(paras)

  id = str(uuid.uuid4())

  doc = {
    "analyzer": {
      "categories": ["business", "technology", "entertainment"],
      "entities": ["Donald Trump", "BYD", "Tesla"],
      "embeddings": [-0.234124, 0.23434, -12.002342, 0.343434],
      "title": titles,
      "paragraphs": paras,
    },
    # "scraper": doc,
  }

  print(json.dumps(doc, indent=2)) 
  # exit(0)


  # doc = {
  #   "author": "kimchy",
  #   "text": "Elasticsearch: cool. bonsai cool.",
  #   "timestamp": datetime.now(),
  # }

  # resp = es.index(index="test-index", id=1, document=doc)
  resp = es.index(index="test-index", id=id, document=doc)
  print("================================")
  print(resp)
  print("================================")
  print(resp["result"])
  print("================================")

  print("retrieving document")

  # resp = es.get(index="test-index", id=1)
  resp = es.get(index="test-index", id=id)
  print(resp["_source"])

  es.indices.refresh(index="test-index")

  resp = es.search(index="test-index", query={"match_all": {}})

  print("Full response:")
  print(json.dumps(str(resp), indent=2))

  print("Got {} hits:".format(resp["hits"]["total"]["value"]))
  for hit in resp["hits"]["hits"]:
    print("{timestamp} {author} {text}".format(**hit["_source"]))


    


