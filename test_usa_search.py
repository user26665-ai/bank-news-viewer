#!/usr/bin/env python3
"""
Тест поиска по запросу "США"
"""

import sqlite3
import numpy as np
from news_rag_system import NewsRAGSystem

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

rag = NewsRAGSystem()
print()

query = "США"
query_embedding = rag.get_embedding(query)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Проверим хоккейную новость
cursor.execute('SELECT id, title, description, embedding FROM news WHERE id = 46828')
row = cursor.fetchone()

if row:
    news_id, title, description, emb_blob = row
    news_embedding = np.frombuffer(emb_blob, dtype=np.float32)

    vector_score = np.dot(query_embedding, news_embedding) / (
        np.linalg.norm(query_embedding) * np.linalg.norm(news_embedding)
    )

    print(f"Новость ID {news_id}:")
    print(f"Заголовок: {title[:100]}")
    print(f"Vector score для 'США': {vector_score:.4f}")
    print()

# Топ-5 по векторному скору для "США"
print("Топ-5 по vector score для запроса 'США':")
print("-" * 70)

cursor.execute('SELECT id, title, embedding FROM news')

similarities = []
for row in cursor.fetchall():
    nid, title, emb_blob = row
    if emb_blob:
        nemb = np.frombuffer(emb_blob, dtype=np.float32)
        sim = np.dot(query_embedding, nemb) / (
            np.linalg.norm(query_embedding) * np.linalg.norm(nemb)
        )
        similarities.append({'id': nid, 'title': title, 'similarity': sim})

top_similar = sorted(similarities, key=lambda x: x['similarity'], reverse=True)[:10]

for i, item in enumerate(top_similar, 1):
    print(f"{i}. [Vector: {item['similarity']:.4f}] ID={item['id']}")
    print(f"   {item['title'][:100]}")
    if item['id'] == 46828:
        print("   ⬆️  ХОККЕЙНАЯ НОВОСТЬ")
    print()

conn.close()
