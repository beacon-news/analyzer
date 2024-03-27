from pymongo import MongoClient

mc = MongoClient(host="localhost", port=27017)

db = mc.get_database("analyzer")

# c = db.get_collection("analyzed_articles")

db.drop_collection("analyzed_articles")

# mc.get_database("scraper").get_collection("scraped_articles").update_many(
#   {
#     "$or": [
#       {"analyzer.processed": {"$exists": False}},
#       {"analyzer.processed": {"$eq": False}}
#     ]
#   },
#   {
#     "$unset": {
#       "analyzer": ""
#     }
#   }
# ) 

