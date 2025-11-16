#!/usr/bin/env python3
"""
Скрипт для нормализации существующих NER-сущностей
"""

import sqlite3
from news_ner import NewsNERExtractor
from tqdm import tqdm

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

def normalize_existing_entities():
    """Обновить существующие сущности нормализованными формами"""

    print("="*70)
    print("🔄 Нормализация существующих NER-сущностей")
    print("="*70)
    print()

    # Инициализируем NER экстрактор
    print("Инициализация NER-экстрактора...")
    ner = NewsNERExtractor()
    print("✓ Готово")
    print()

    # Подключаемся к БД
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Получаем все уникальные сущности
    cursor.execute("""
        SELECT DISTINCT entity_text, entity_type
        FROM entities
        WHERE normalized_text IS NULL OR normalized_text = ''
    """)

    entities_to_normalize = cursor.fetchall()
    total = len(entities_to_normalize)

    print(f"Найдено сущностей для нормализации: {total}")
    print("Начинаю нормализацию...")
    print()

    processed = 0
    errors = 0

    # Обрабатываем с прогресс-баром
    for entity_text, entity_type in tqdm(entities_to_normalize, desc="Нормализация"):
        try:
            # Нормализуем сущность
            normalized = ner.normalize_entity(entity_text, entity_type)

            # Обновляем все записи с этой сущностью
            cursor.execute('''
                UPDATE entities
                SET normalized_text = ?
                WHERE entity_text = ? AND entity_type = ?
            ''', (normalized, entity_text, entity_type))

            processed += 1

            # Коммитим каждые 100 записей
            if processed % 100 == 0:
                conn.commit()

        except Exception as e:
            errors += 1
            if errors <= 5:  # Показываем только первые 5 ошибок
                print(f"\n⚠️  Ошибка нормализации '{entity_text}': {e}")

    # Финальный коммит
    conn.commit()

    print()
    print("="*70)
    print("📊 Результаты:")
    print("="*70)
    print(f"✓ Обработано уникальных сущностей: {processed}")
    if errors > 0:
        print(f"⚠️  Ошибок: {errors}")
    print()

    # Примеры нормализации
    print("Примеры нормализации:")
    cursor.execute("""
        SELECT entity_text, normalized_text, COUNT(*) as cnt
        FROM entities
        WHERE entity_type = 'person' AND entity_text != normalized_text
        GROUP BY normalized_text
        HAVING cnt > 1
        ORDER BY cnt DESC
        LIMIT 10
    """)

    results = cursor.fetchall()
    if results:
        for entity, normalized, count in results:
            print(f"  '{entity}' → '{normalized}' ({count} вариантов)")
    else:
        print("  Нет примеров с несколькими вариантами")

    print()

    # Статистика по сокращению
    cursor.execute("""
        SELECT
            COUNT(DISTINCT entity_text) as original_count,
            COUNT(DISTINCT normalized_text) as normalized_count
        FROM entities
        WHERE entity_type = 'person'
    """)

    orig, norm = cursor.fetchone()
    print(f"Персоны: {orig} оригинальных форм → {norm} нормализованных")
    if orig > 0:
        reduction = (orig - norm) / orig * 100
        print(f"Сокращение: {reduction:.1f}%")

    conn.close()

    print()
    print("="*70)
    print("✅ Нормализация завершена!")
    print("="*70)


if __name__ == "__main__":
    normalize_existing_entities()
