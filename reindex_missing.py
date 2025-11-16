#!/usr/bin/env python3
"""
Переиндексировать новости с неправильной размерностью эмбеддингов
"""

import sqlite3
import numpy as np
from news_rag_system import NewsRAGSystem

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

rag = NewsRAGSystem()
print()

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Найдем все новости с эмбеддингами размерности 768
cursor.execute('SELECT id, title, description, embedding FROM news')

to_reindex = []
for news_id, title, description, emb_blob in cursor.fetchall():
    if emb_blob:
        emb = np.frombuffer(emb_blob, dtype=np.float32)
        if len(emb) == 768:
            to_reindex.append((news_id, title, description))

print(f"Найдено {len(to_reindex)} новостей с неправильной размерностью")
print("Переиндексация...")

for news_id, title, description in to_reindex:
    embed_text = f"{title or ''}\n\n{description or ''}"
    embedding = rag.get_embedding(embed_text)

    if embedding is not None:
        cursor.execute('''
            UPDATE news
            SET embedding = ?
            WHERE id = ?
        ''', (embedding.tobytes(), news_id))

conn.commit()
conn.close()

print(f"✅ Переиндексировано {len(to_reindex)} новостей")
