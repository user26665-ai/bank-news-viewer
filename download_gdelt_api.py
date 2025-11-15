#!/usr/bin/env python3
"""
Загрузка данных GDELT через DOC API (БЕЗ аутентификации!)

Преимущества:
- Не требует Google Cloud аккаунта
- Не требует регистрации
- 100% бесплатно
- Полностью автоматично

Ограничения:
- Максимум 250 результатов на запрос
- Последние 3 месяца данных

Использование:
    python3 download_gdelt_api.py --days 60 --language Russian
"""

import requests
import json
import csv
import time
from datetime import datetime, timedelta
import argparse
import os
import sys
from typing import List, Dict

# GDELT DOC API endpoint
API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

def make_gdelt_request(query: str, max_records: int = 250,
                       start_datetime: str = None, end_datetime: str = None,
                       mode: str = "artlist", max_retries: int = 3) -> List[Dict]:
    """
    Запрос к GDELT DOC API с retry и exponential backoff

    Args:
        query: поисковый запрос (например, "sourcecountry:Russia sourcelang:Russian")
        max_records: максимум результатов (до 250)
        start_datetime: начало периода (YYYYMMDDHHMMSS)
        end_datetime: конец периода (YYYYMMDDHHMMSS)
        mode: режим (artlist, timeline, etc)
        max_retries: максимум попыток при ошибках

    Returns:
        Список статей
    """
    params = {
        "query": query,
        "mode": mode,
        "maxrecords": min(max_records, 250),  # API limit
        "format": "json"
    }

    # Используем точные даты вместо timespan для контроля
    if start_datetime and end_datetime:
        params["startdatetime"] = start_datetime
        params["enddatetime"] = end_datetime

    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, params=params, timeout=30)

            # Обработка rate limiting
            if response.status_code == 429:
                wait_time = (2 ** attempt) * 5  # Exponential backoff: 5, 10, 20 секунд
                print(f"      ⚠️  Rate limit (попытка {attempt + 1}/{max_retries}). Жду {wait_time}с...")
                time.sleep(wait_time)
                continue

            response.raise_for_status()

            data = response.json()

            # API возвращает articles в поле 'articles'
            if 'articles' in data:
                return data['articles']
            else:
                return []

        except requests.exceptions.Timeout:
            print(f"      ⚠️  Timeout (попытка {attempt + 1}/{max_retries})")
            if attempt < max_retries - 1:
                time.sleep(3)
                continue
            return []

        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"      ⚠️  Ошибка (попытка {attempt + 1}/{max_retries}): {e}")
                time.sleep(3)
                continue
            else:
                print(f"      ❌ Ошибка запроса: {e}")
                return []

        except json.JSONDecodeError as e:
            print(f"      ⚠️  Ошибка парсинга JSON: {e}")
            return []

    return []


def download_gdelt_period(start_date: datetime, end_date: datetime,
                          language: str = None, country: str = None,
                          keywords: List[str] = None,
                          output_dir: str = "gdelt_data",
                          window_hours: int = 12) -> int:
    """
    Загрузка данных GDELT за указанный период

    Args:
        start_date: начальная дата
        end_date: конечная дата
        language: язык (Russian, English, etc)
        country: страна (Russia, US, etc)
        keywords: список ключевых слов
        output_dir: директория для сохранения
        window_hours: размер временного окна в часах

    Returns:
        Общее количество загруженных статей
    """

    os.makedirs(output_dir, exist_ok=True)

    # Формируем query для GDELT API
    query_parts = []

    if language:
        query_parts.append(f"sourcelang:{language}")

    if country:
        query_parts.append(f"sourcecountry:{country}")

    if keywords:
        # Для ключевых слов используем OR
        keyword_query = " OR ".join(keywords)
        query_parts.append(f"({keyword_query})")

    # Если ничего не указано - все новости
    query = " ".join(query_parts) if query_parts else "*"

    print("=" * 70)
    print("📥 Загрузка данных GDELT через DOC API")
    print("=" * 70)
    print(f"\n📅 Период: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    print(f"🗂️  Выходная директория: {output_dir}/")
    print(f"⏱️  Временное окно: {window_hours} часов")
    print(f"🔍 Запрос: {query}")

    # Расчет количества запросов
    total_hours = int((end_date - start_date).total_seconds() / 3600)
    num_windows = (total_hours + window_hours - 1) // window_hours

    print(f"\n📊 Планируется запросов: {num_windows}")
    print(f"⏳ Примерное время: {num_windows * 2} секунд (~{num_windows * 2 / 60:.1f} минут)")
    print(f"💡 Ожидается статей: ~{num_windows * 200:,} (по 200 в среднем на окно)")

    print("\n" + "=" * 70)
    print("⏳ Начинаю загрузку...")
    print("=" * 70)

    all_articles = []
    current = start_date
    window_num = 1

    # CSV файл для сохранения
    csv_filename = os.path.join(
        output_dir,
        f"gdelt_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    )

    csv_file = open(csv_filename, 'w', newline='', encoding='utf-8')
    csv_writer = None

    while current < end_date:
        window_end = min(current + timedelta(hours=window_hours), end_date)

        # Форматируем даты для GDELT API (YYYYMMDDHHMMSS)
        start_dt_str = current.strftime("%Y%m%d%H%M%S")
        end_dt_str = window_end.strftime("%Y%m%d%H%M%S")

        print(f"\n📦 Окно #{window_num}/{num_windows}: {current.strftime('%Y-%m-%d %H:%M')} → {window_end.strftime('%Y-%m-%d %H:%M')}")
        print(f"   🔄 Запрашиваю данные...")

        # Запрос к API
        articles = make_gdelt_request(
            query=query,
            max_records=250,
            start_datetime=start_dt_str,
            end_datetime=end_dt_str
        )

        if articles:
            print(f"   ✅ Получено статей: {len(articles)}")

            # Инициализируем CSV writer при первой записи
            if csv_writer is None and articles:
                # Определяем поля из первой статьи
                fieldnames = articles[0].keys()
                csv_writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
                csv_writer.writeheader()

            # Записываем статьи в CSV
            for article in articles:
                csv_writer.writerow(article)

            all_articles.extend(articles)
        else:
            print(f"   ⚠️  Нет данных")

        # Задержка между запросами (чтобы не нагружать API)
        # GDELT API имеет строгие rate limits
        time.sleep(3)

        current = window_end
        window_num += 1

    csv_file.close()

    print("\n" + "=" * 70)
    print("✅ Загрузка завершена!")
    print("=" * 70)
    print(f"\n📊 Всего загружено: {len(all_articles):,} статей")

    if all_articles:
        file_size = os.path.getsize(csv_filename) / (1024 * 1024)  # MB
        print(f"💾 Сохранено в: {csv_filename}")
        print(f"📏 Размер файла: {file_size:.1f} MB")

    # Сохраняем метаданные
    metadata = {
        "download_date": datetime.now().isoformat(),
        "period": {
            "start": start_date.isoformat(),
            "end": end_date.isoformat()
        },
        "query": query,
        "filters": {
            "language": language,
            "country": country,
            "keywords": keywords
        },
        "total_articles": len(all_articles),
        "window_hours": window_hours
    }

    metadata_file = os.path.join(output_dir, "metadata.json")
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    print(f"📋 Метаданные: {metadata_file}")

    return len(all_articles)


def main():
    parser = argparse.ArgumentParser(
        description="Загрузка данных GDELT через DOC API (БЕЗ регистрации!)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Русскоязычные новости за последние 60 дней
  python3 download_gdelt_api.py --days 60 --language Russian

  # Новости из России за последние 30 дней
  python3 download_gdelt_api.py --days 30 --country Russia

  # С точными датами
  python3 download_gdelt_api.py --start-date 2024-10-01 --end-date 2024-11-30 --language Russian

  # С ключевыми словами
  python3 download_gdelt_api.py --days 60 --language Russian --keywords "банк,санкции,рубль"

  # Большое временное окно (быстрее, но меньше точность)
  python3 download_gdelt_api.py --days 60 --language Russian --window-hours 24

  # Маленькое окно (медленнее, но больше данных)
  python3 download_gdelt_api.py --days 60 --language Russian --window-hours 6
        """
    )

    # Период
    date_group = parser.add_mutually_exclusive_group(required=True)
    date_group.add_argument("--days", type=int, help="Количество дней назад от сегодня (1-90)")
    date_group.add_argument("--start-date", help="Начальная дата (YYYY-MM-DD)")

    parser.add_argument("--end-date", help="Конечная дата (YYYY-MM-DD), используется со --start-date")

    # Фильтры
    parser.add_argument("--language", help="Язык статей (Russian, English, etc)")
    parser.add_argument("--country", help="Страна источника (Russia, US, etc)")
    parser.add_argument("--keywords", help="Ключевые слова через запятую")

    # Настройки
    parser.add_argument("--output-dir", default="gdelt_api_data", help="Директория для сохранения")
    parser.add_argument("--window-hours", type=int, default=12,
                       help="Размер временного окна в часах (по умолчанию: 12)")

    args = parser.parse_args()

    # Определяем период
    if args.days:
        if args.days > 90:
            print("⚠️  GDELT DOC API поддерживает максимум 90 дней истории")
            print("   Устанавливаю --days=90")
            args.days = 90

        end_date = datetime.now()
        start_date = end_date - timedelta(days=args.days)
    else:
        start_date = datetime.strptime(args.start_date, "%Y-%m-%d")

        if args.end_date:
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        else:
            end_date = datetime.now()

    # Проверка: не более 90 дней
    if (end_date - start_date).days > 90:
        print("⚠️  GDELT DOC API поддерживает максимум 90 дней")
        print("   Сокращаю период до 90 дней от конечной даты")
        start_date = end_date - timedelta(days=90)

    # Парсим ключевые слова
    keywords = None
    if args.keywords:
        keywords = [kw.strip() for kw in args.keywords.split(",")]

    # Загрузка
    try:
        total = download_gdelt_period(
            start_date=start_date,
            end_date=end_date,
            language=args.language,
            country=args.country,
            keywords=keywords,
            output_dir=args.output_dir,
            window_hours=args.window_hours
        )

        print("\n🎉 Готово!")

        if total == 0:
            print("\n⚠️  Не найдено статей с указанными фильтрами")
            print("   Попробуйте:")
            print("   - Увеличить временное окно (--window-hours)")
            print("   - Убрать некоторые фильтры")
            print("   - Изменить ключевые слова")

        return 0

    except KeyboardInterrupt:
        print("\n\n⚠️  Прервано пользователем")
        return 1
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
