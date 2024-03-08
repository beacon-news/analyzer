import os
from category_classifier import CategoryClassifier

MODEL_PATH = os.environ.get('MODEL_PATH')

if MODEL_PATH is None:
  raise ValueError('MODEL_PATH environment variable is not set')


# 1. load the classification model
cclf = CategoryClassifier.load(MODEL_PATH)

cats = cclf.predict("Donald Trump caught in another scandal while he was on vacation in Canada")

print(cats)

# 2. load the bertopic model
# - load the embedding model
# - load the embeddings