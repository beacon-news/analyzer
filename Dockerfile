FROM python:3.12-bookworm

WORKDIR /app

COPY requirements.txt requirements.txt

RUN pip3 install -r requirements.txt --no-cache-dir

RUN useradd app --create-home --shell /bin/bash
USER app

COPY --chown=app:app . .

ENV CAT_CLF_MODEL_PATH=models/classification/article_ovr_clfs_2024-03-09.pkl
ENV EMBEDDINGS_MODEL_PATH=models/embeddings/embeddings_container_all_MiniLM_L6_v2.pkl
ENV SPACY_MODEL=en_core_web_sm
ENV SPACY_MODEL_DIR=models/ner/
ENV ELASTIC_PASSWORD=password
ENV MONGO_DB_SCRAPER=scraper
ENV MONGO_COLLECTION_SCRAPER=scraped_articles

ENTRYPOINT ["python3", "src/main.py"]
