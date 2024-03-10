from ner.entity_recognizer import SpacyEntityRecognizer 


if __name__ == '__main__':

  # ec = EmbeddingsContainer.load('models/embeddings/embeddings_container_2024-03-10.pkl')
  # p = 'models/ner/spacy_en_core_web_sm'

  # try:
  #   nlp = spacy.load(p)
  # except OSError:
  #   print("OSError, trying to download and save en_core_web_sm spacy model")
  #   nlp = spacy.cli.download('en_core_web_sm')
  #   nlp.to_disk(p)


  t = 'Donald Trump caught in another scandal while he was on vacation in Canada'

  # # see spacy.explain() for descriptions
  # entity_types = ['EVENT', 'FAC', 'GPE', 'LAW', 'LOC', 'NORP', 'ORG', 'PERSON', 'WORK_OF_ART', 'PRODUCT']
  # named_entities = []

  # doc = nlp(t)
  # for ent in doc.ents:
  #   if ent.label_ in entity_types:
  #     named_entities.append(ent.text)
  #     # print(ent.text, ent.start_char, ent.end_char, ent.label_)
  
  # print(named_entities)

  r = SpacyEntityRecognizer('en_core_web_sm', 'models/ner/')

  print(r.ner(t))

    


