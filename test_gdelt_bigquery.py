#!/usr/bin/env python3
"""
Тестовый скрипт для проверки доступа к GDELT BigQuery
Проверяет возможность запроса публичных датасетов без аутентификации
"""

from google.cloud import bigquery
import os

def test_gdelt_access():
    """Проверка доступа к GDELT Article List в BigQuery"""

    print("🔍 Проверяю доступ к GDELT BigQuery...")

    # Создаем клиент без явной аутентификации (для публичных датасетов)
    try:
        client = bigquery.Client()
        print("✅ BigQuery клиент создан")
    except Exception as e:
        print(f"⚠️  Не удалось создать аутентифицированный клиент: {e}")
        print("⏩ Пробую без аутентификации...")

        # Для публичных датасетов можно попробовать без credentials
        os.environ['GOOGLE_CLOUD_PROJECT'] = 'gdelt-bq'
        try:
            client = bigquery.Client(project='gdelt-bq')
            print("✅ BigQuery клиент создан (публичный режим)")
        except Exception as e2:
            print(f"❌ Ошибка создания клиента: {e2}")
            return False

    # Тестовый запрос - получить 5 записей за последний день
    query = """
    SELECT
        url,
        title,
        domain,
        language,
        date
    FROM
        `gdelt-bq.gdeltv2.gal`
    WHERE
        DATE(date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
        AND language = 'Russian'
    LIMIT 5
    """

    print("\n📊 Выполняю тестовый запрос к gdelt-bq.gdeltv2.gal...")
    print(f"Запрос: {query[:200]}...")

    try:
        query_job = client.query(query)
        results = query_job.result()

        print(f"\n✅ Запрос выполнен успешно!")
        print(f"Количество записей: {results.total_rows}")

        print("\n📰 Примеры статей:")
        for i, row in enumerate(results, 1):
            print(f"\n{i}. {row.title}")
            print(f"   URL: {row.url}")
            print(f"   Домен: {row.domain}")
            print(f"   Дата: {row.date}")

        return True

    except Exception as e:
        print(f"\n❌ Ошибка выполнения запроса: {e}")
        print(f"\nТип ошибки: {type(e).__name__}")

        # Проверяем, требуется ли аутентификация
        if "credentials" in str(e).lower() or "authentication" in str(e).lower():
            print("\n⚠️  Требуется аутентификация Google Cloud")
            print("\nДля работы с BigQuery нужно:")
            print("1. Создать проект в Google Cloud Console")
            print("2. Включить BigQuery API")
            print("3. Создать Service Account и скачать ключ JSON")
            print("4. Установить переменную GOOGLE_APPLICATION_CREDENTIALS")

        return False

if __name__ == "__main__":
    success = test_gdelt_access()

    if success:
        print("\n" + "="*70)
        print("✅ Доступ к GDELT BigQuery работает!")
        print("="*70)
    else:
        print("\n" + "="*70)
        print("❌ Требуется настройка аутентификации")
        print("="*70)
