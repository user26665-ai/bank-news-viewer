#!/usr/bin/env python3
"""
Переиндексация всех новостей с BGE-M3 эмбеддингами
"""

import sqlite3
import numpy as np
from news_rag_system import NewsRAGSystem
from tqdm import tqdm

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

def reindex_all_news():
    """Переиндексировать все новости с BGE-M3"""

    print("="*70)
    print("🔄 Переиндексация новостей с BGE-M3")
    print("="*70)
    print()

    # Инициализируем систему (это загрузит BGE-M3)
    rag = NewsRAGSystem()
    print()

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем все новости
    cursor.execute('SELECT COUNT(*) FROM news')
    total_count = cursor.fetchone()[0]

    print(f"Всего новостей для переиндексации: {total_count}")
    print()

    # Получаем новости батчами для эффективности
    cursor.execute('SELECT id, title, description FROM news')
    all_news = cursor.fetchall()

    print("Начинаю переиндексацию...")
    print()

    processed = 0
    errors = 0
    batch_size = 100

    for i in tqdm(range(0, len(all_news), batch_size), desc="Переиндексация"):
        batch = all_news[i:i + batch_size]

        for news_id, title, description in batch:
            try:
                # Создаем текст для эмбеддинга (как в оригинале)
                embed_text = f"{title or ''}\n\n{description or ''}"

                # Получаем новый эмбеддинг через BGE-M3
                embedding = rag.get_embedding(embed_text)

                if embedding is not None:
                    # Обновляем эмбеддинг в БД
                    cursor.execute('''
                        UPDATE news
                        SET embedding = ?
                        WHERE id = ?
                    ''', (embedding.tobytes(), news_id))

                    processed += 1
                else:
                    errors += 1

            except Exception as e:
                errors += 1
                if errors <= 5:  # Показываем только первые 5 ошибок
                    print(f"\n⚠️  Ошибка для новости {news_id}: {e}")

        # Коммитим каждый батч
        conn.commit()

    print()
    print("="*70)
    print("📊 Результаты:")
    print("="*70)
    print(f"✓ Успешно переиндексировано: {processed}/{total_count}")
    if errors > 0:
        print(f"⚠️  Ошибок: {errors}")
    print()

    # Проверим размерность эмбеддингов
    cursor.execute('SELECT embedding FROM news LIMIT 1')
    row = cursor.fetchone()
    if row and row[0]:
        emb = np.frombuffer(row[0], dtype=np.float32)
        print(f"Размерность эмбеддингов: {len(emb)}")
        print()

    conn.close()

    print("="*70)
    print("✅ Переиндексация завершена!")
    print("="*70)


if __name__ == "__main__":
    reindex_all_news()
