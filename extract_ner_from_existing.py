#!/usr/bin/env python3
"""
Скрипт для извлечения NER-сущностей из существующих новостей
"""

import sqlite3
from news_ner import NewsNERExtractor
from tqdm import tqdm

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

def extract_ner_from_existing_news():
    """Извлечь NER-сущности из всех существующих новостей"""

    print("="*70)
    print("🔍 Извлечение NER-сущностей из существующих новостей")
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

    # Очищаем старые сущности (если есть)
    print("Очистка старых данных...")
    cursor.execute("DELETE FROM entities")
    conn.commit()
    print("✓ Таблица entities очищена")
    print()

    # Получаем все новости
    cursor.execute("SELECT COUNT(*) FROM news")
    total_news = cursor.fetchone()[0]

    print(f"Найдено новостей: {total_news}")
    print("Начинаю извлечение сущностей...")
    print()

    cursor.execute("SELECT id, title, description FROM news")

    processed = 0
    entities_count = 0
    errors = 0

    # Обрабатываем с прогресс-баром
    for row in tqdm(cursor.fetchall(), total=total_news, desc="Обработка новостей"):
        news_id, title, description = row

        try:
            # Извлекаем сущности
            result = ner.extract_from_news(title, description or "")

            # Сохраняем каждую сущность
            for idx, entity in enumerate(result['all']):
                is_banking = ner.is_banking_entity(entity['text'])

                cursor.execute('''
                    INSERT INTO entities (news_id, entity_text, entity_type, position, is_banking)
                    VALUES (?, ?, ?, ?, ?)
                ''', (news_id, entity['text'], entity['type'], idx, is_banking))

                entities_count += 1

            processed += 1

            # Коммитим каждые 100 новостей
            if processed % 100 == 0:
                conn.commit()

        except Exception as e:
            errors += 1
            if errors <= 5:  # Показываем только первые 5 ошибок
                print(f"\n⚠️  Ошибка обработки новости {news_id}: {e}")

    # Финальный коммит
    conn.commit()

    print()
    print("="*70)
    print("📊 Результаты:")
    print("="*70)
    print(f"✓ Обработано новостей: {processed}")
    print(f"✓ Извлечено сущностей: {entities_count}")
    if errors > 0:
        print(f"⚠️  Ошибок: {errors}")
    print()

    # Статистика по типам
    print("Статистика по типам сущностей:")
    cursor.execute('''
        SELECT entity_type, COUNT(*) as cnt
        FROM entities
        GROUP BY entity_type
        ORDER BY cnt DESC
    ''')

    for entity_type, count in cursor.fetchall():
        print(f"  • {entity_type}: {count}")

    print()

    # Топ сущностей
    print("Топ-10 самых упоминаемых сущностей:")
    cursor.execute('''
        SELECT entity_text, entity_type, COUNT(*) as cnt
        FROM entities
        GROUP BY entity_text, entity_type
        ORDER BY cnt DESC
        LIMIT 10
    ''')

    for entity, etype, count in cursor.fetchall():
        banking_mark = "🏦" if etype == "organization" else "  "
        print(f"  {banking_mark} {entity} ({etype}): {count} упоминаний")

    print()

    # Банковские сущности
    cursor.execute('''
        SELECT entity_text, COUNT(*) as cnt
        FROM entities
        WHERE is_banking = 1
        GROUP BY entity_text
        ORDER BY cnt DESC
        LIMIT 10
    ''')

    banking = cursor.fetchall()
    if banking:
        print("Топ-10 банковских сущностей:")
        for entity, count in banking:
            print(f"  🏦 {entity}: {count} упоминаний")

    conn.close()

    print()
    print("="*70)
    print("✅ Извлечение завершено!")
    print("="*70)


if __name__ == "__main__":
    extract_ner_from_existing_news()
