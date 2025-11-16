#!/usr/bin/env python3
"""
Проверить размерности эмбеддингов в базе
"""

import sqlite3
import numpy as np

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

cursor.execute('SELECT id, embedding FROM news')  # ВСЕ новости

dimensions = {}
for news_id, emb_blob in cursor.fetchall():
    if emb_blob:
        emb = np.frombuffer(emb_blob, dtype=np.float32)
        dim = len(emb)
        if dim not in dimensions:
            dimensions[dim] = []
        dimensions[dim].append(news_id)

print("Размерности эмбеддингов в базе:")
for dim, ids in sorted(dimensions.items()):
    print(f"  {dim}: {len(ids)} новостей (примеры: {ids[:5]})")

conn.close()
