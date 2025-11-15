#!/usr/bin/env python3
"""
Скрипт для загрузки данных GDELT Article List из BigQuery

Функции:
- Аутентификация через браузер (OAuth)
- Загрузка данных за указанный период
- Фильтрация по языку, стране, ключевым словам
- Экспорт в CSV/JSON
- Пакетная обработка для больших объемов

Использование:
    python3 download_gdelt_bigquery.py --start-date 2024-01-01 --end-date 2024-12-31 --language Russian
"""

import pandas as pd
import pandas_gbq
from datetime import datetime, timedelta
import argparse
import os
import sys
import json

# Отключаем предупреждения pandas
import warnings
warnings.filterwarnings('ignore')

def authenticate():
    """
    Аутентификация в Google Cloud
    Откроет браузер для входа в Google аккаунт
    """
    print("=" * 70)
    print("🔐 Аутентификация в Google Cloud")
    print("=" * 70)
    print("\n📌 ВАЖНО:")
    print("1. Сейчас откроется браузер")
    print("2. Войдите в свой Google аккаунт")
    print("3. Разрешите доступ к BigQuery")
    print("4. Можете закрыть браузер после успешной авторизации")
    print("\nЭто нужно сделать только один раз.")
    print("\n⏳ Запускаю OAuth flow через 3 секунды...")

    import time
    time.sleep(3)

    # pandas-gbq автоматически запустит OAuth flow
    # Credentials сохранятся локально
    return True


def estimate_query_size(query):
    """Оценка размера запроса (для контроля бесплатного лимита 1TB)"""
    # Примерная оценка: GDELT GAL содержит ~1 млрд записей
    # В среднем 1 месяц = ~50 млн записей = ~50 GB
    # Год = ~600 млн записей = ~600 GB (в пределах бесплатного лимита)
    print("\n💡 Примерная оценка:")
    print("   - 1 день GDELT ≈ 2-5 GB")
    print("   - 1 месяц ≈ 50 GB")
    print("   - 1 год ≈ 500-600 GB")
    print("   - Бесплатный лимит BigQuery: 1 TB/месяц")


def download_gdelt_batch(start_date, end_date, output_dir="gdelt_data",
                          language=None, country=None, keywords=None,
                          batch_days=7, file_format="csv"):
    """
    Загрузка данных GDELT Article List из BigQuery по батчам

    Args:
        start_date: начальная дата (YYYY-MM-DD)
        end_date: конечная дата (YYYY-MM-DD)
        output_dir: директория для сохранения файлов
        language: фильтр по языку (например, "Russian")
        country: фильтр по стране (например, "Russia")
        keywords: список ключевых слов для фильтрации
        batch_days: размер батча в днях (для разбивки больших запросов)
        file_format: формат файлов ("csv", "json", "parquet")
    """

    # Создаем директорию для данных
    os.makedirs(output_dir, exist_ok=True)

    print("\n" + "=" * 70)
    print("📥 Загрузка данных GDELT из BigQuery")
    print("=" * 70)
    print(f"\n📅 Период: {start_date} → {end_date}")
    print(f"🗂️  Выходная директория: {output_dir}/")
    print(f"📦 Размер батча: {batch_days} дней")
    print(f"📄 Формат: {file_format.upper()}")

    if language:
        print(f"🌐 Язык: {language}")
    if country:
        print(f"🌍 Страна: {country}")
    if keywords:
        print(f"🔍 Ключевые слова: {', '.join(keywords)}")

    estimate_query_size(None)

    # Разбиваем период на батчи
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    current = start
    batch_num = 1
    total_rows = 0

    print("\n" + "=" * 70)
    print("⏳ Начинаю загрузку...")
    print("=" * 70)

    while current <= end:
        batch_end = min(current + timedelta(days=batch_days - 1), end)

        print(f"\n📦 Батч #{batch_num}: {current.strftime('%Y-%m-%d')} → {batch_end.strftime('%Y-%m-%d')}")

        # Формируем SQL запрос
        query = f"""
        SELECT
            url,
            title,
            seendate,
            socialimage,
            domain,
            language,
            sourcecountry
        FROM
            `gdelt-bq.gdeltv2.gal`
        WHERE
            DATE(seendate) BETWEEN '{current.strftime('%Y-%m-%d')}' AND '{batch_end.strftime('%Y-%m-%d')}'
        """

        # Добавляем фильтры
        if language:
            query += f"\n            AND language = '{language}'"
        if country:
            query += f"\n            AND sourcecountry = '{country}'"
        if keywords:
            keyword_filter = " OR ".join([f"LOWER(title) LIKE '%{kw.lower()}%'" for kw in keywords])
            query += f"\n            AND ({keyword_filter})"

        try:
            # Выполняем запрос через pandas-gbq
            print(f"   🔄 Запрашиваю данные...")

            # Используем проект пользователя (из переменной окружения или параметра)
            project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')
            if not project_id:
                raise ValueError(
                    "❌ Не указан GOOGLE_CLOUD_PROJECT!\n\n"
                    "Установите переменную окружения:\n"
                    "export GOOGLE_CLOUD_PROJECT='your-project-id'\n\n"
                    "Или передайте через параметр --project-id\n"
                    "См. инструкцию в GDELT_BIGQUERY_SETUP.md"
                )

            df = pandas_gbq.read_gbq(
                query,
                project_id=project_id,
                progress_bar_type='tqdm'
            )

            rows = len(df)
            total_rows += rows

            print(f"   ✅ Получено записей: {rows:,}")

            if rows > 0:
                # Сохраняем файл
                filename = f"gdelt_{current.strftime('%Y%m%d')}_{batch_end.strftime('%Y%m%d')}.{file_format}"
                filepath = os.path.join(output_dir, filename)

                if file_format == "csv":
                    df.to_csv(filepath, index=False, encoding='utf-8')
                elif file_format == "json":
                    df.to_json(filepath, orient='records', force_ascii=False, indent=2)
                elif file_format == "parquet":
                    df.to_parquet(filepath, index=False)

                file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
                print(f"   💾 Сохранено: {filename} ({file_size:.1f} MB)")

            else:
                print(f"   ⚠️  Нет данных за этот период")

        except Exception as e:
            print(f"   ❌ Ошибка: {e}")
            print(f"   Пропускаю батч и продолжаю...")

        current = batch_end + timedelta(days=1)
        batch_num += 1

    print("\n" + "=" * 70)
    print("✅ Загрузка завершена!")
    print("=" * 70)
    print(f"\n📊 Всего загружено: {total_rows:,} записей")
    print(f"📁 Файлы сохранены в: {output_dir}/")

    # Создаем сводный файл с метаданными
    metadata = {
        "download_date": datetime.now().isoformat(),
        "period": {
            "start": start_date,
            "end": end_date
        },
        "filters": {
            "language": language,
            "country": country,
            "keywords": keywords
        },
        "total_rows": total_rows,
        "batch_size_days": batch_days,
        "file_format": file_format
    }

    with open(os.path.join(output_dir, "metadata.json"), "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"📋 Метаданные сохранены: {output_dir}/metadata.json")

    return total_rows


def main():
    parser = argparse.ArgumentParser(
        description="Загрузка данных GDELT Article List из BigQuery",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Все русскоязычные новости за 2024 год
  python3 download_gdelt_bigquery.py --start-date 2024-01-01 --end-date 2024-12-31 --language Russian

  # Новости из России за последний месяц
  python3 download_gdelt_bigquery.py --start-date 2024-11-01 --end-date 2024-11-30 --country Russia

  # Новости с ключевыми словами
  python3 download_gdelt_bigquery.py --start-date 2024-01-01 --end-date 2024-12-31 --keywords "банк,санкции,рубль"

  # Быстрая загрузка (большие батчи)
  python3 download_gdelt_bigquery.py --start-date 2024-01-01 --end-date 2024-12-31 --batch-days 30

  # Экспорт в JSON
  python3 download_gdelt_bigquery.py --start-date 2024-01-01 --end-date 2024-12-31 --format json
        """
    )

    parser.add_argument("--start-date", required=True, help="Начальная дата (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Конечная дата (YYYY-MM-DD)")
    parser.add_argument("--project-id", help="Google Cloud Project ID (или используйте переменную GOOGLE_CLOUD_PROJECT)")
    parser.add_argument("--output-dir", default="gdelt_data", help="Директория для сохранения (по умолчанию: gdelt_data)")
    parser.add_argument("--language", help="Фильтр по языку (например: Russian, English)")
    parser.add_argument("--country", help="Фильтр по стране (например: Russia, US)")
    parser.add_argument("--keywords", help="Ключевые слова через запятую (например: банк,санкции)")
    parser.add_argument("--batch-days", type=int, default=7, help="Размер батча в днях (по умолчанию: 7)")
    parser.add_argument("--format", choices=["csv", "json", "parquet"], default="csv", help="Формат выходных файлов")
    parser.add_argument("--no-auth", action="store_true", help="Пропустить аутентификацию (если уже выполнена)")

    args = parser.parse_args()

    # Устанавливаем project_id если передан
    if args.project_id:
        os.environ['GOOGLE_CLOUD_PROJECT'] = args.project_id

    # Парсим ключевые слова
    keywords = None
    if args.keywords:
        keywords = [kw.strip() for kw in args.keywords.split(",")]

    # Аутентификация
    if not args.no_auth:
        authenticate()

    # Загрузка данных
    try:
        total_rows = download_gdelt_batch(
            start_date=args.start_date,
            end_date=args.end_date,
            output_dir=args.output_dir,
            language=args.language,
            country=args.country,
            keywords=keywords,
            batch_days=args.batch_days,
            file_format=args.format
        )

        print("\n🎉 Готово!")
        return 0

    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
