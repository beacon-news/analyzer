import spacy
import logging
from pathlib import Path
from utils import log_utils


class SpacyEntityRecognizer:

  def __init__(self, model: str, model_path: str):
    self.log = log_utils.create_console_logger(
      name=self.__class__.__name__,
      level=logging.INFO
    )

    path = Path(model_path, model)
    try:
      self.log.info(f"trying to load {model} spacy model from {path}")
      self.nlp = spacy.load(path)
    except OSError:
      print(f"OSError, trying to download and save {model} spacy model at {path}")
      spacy.cli.download(model)
      self.nlp = spacy.load(model)
      self.nlp.to_disk(path)
    
    self.log.info(f"loaded {model} spacy model from {path}")
    
    # named entities we're interested in
    self.entity_types = ['EVENT', 'FAC', 'GPE', 'LAW', 'LOC', 'NORP', 'ORG', 'PERSON', 'WORK_OF_ART', 'PRODUCT']


  def ner(self, text: str) -> list[str]:
    # see spacy.explain() for descriptions
    named_entities = set() 

    doc = self.nlp(text)
    for ent in doc.ents:
      if ent.label_ in self.entity_types:
        named_entities.add(ent.text)

    return list(named_entities)
      


