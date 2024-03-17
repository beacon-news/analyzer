from pymongo import MongoClient

mc = MongoClient(host="localhost", port=27017)

mc.get_database("scraper").get_collection("scraped_articles").update_many(
  {
    "$or": [
      {"analyzer.processed": {"$exists": False}},
      {"analyzer.processed": {"$eq": False}}
    ]
  },
  {
    "$unset": {
      "analyzer": ""
    }
  }
) 