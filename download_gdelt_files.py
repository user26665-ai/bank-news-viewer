#!/usr/bin/env python3
"""
Скачивание сырых GDELT GKG файлов (БЕЗ rate limits!)

GKG (Global Knowledge Graph) содержит:
- URLs статей
- Темы, организации, люди, локации
- Тональность
- Язык и страна источника

Преимущества:
- Нет API rate limits
- Полные данные (не только 250 статей)
- Можно скачать за любой период

Использование:
    python3 download_gdelt_files.py --start-date 2024-09-01 --end-date 2024-10-31
"""

import requests
import zipfile
import shutil
import csv
import os
import sys
from datetime import datetime, timedelta
import argparse
from typing import List
import time

# GDELT v2 master file list
MASTER_LIST_URL = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


def get_available_files(start_date: datetime, end_date: datetime,
                        hours_interval: int = 24) -> List[str]:
    """
    Получить список доступных GDELT файлов за период

    Args:
        start_date: начальная дата
        end_date: конечная дата
        hours_interval: интервал между файлами в часах (24 = 1 файл в день)

    Returns:
        Список URL файлов .gkg.csv.zip
    """
    print("🔍 Получаю список доступных GDELT файлов...")

    try:
        response = requests.get(MASTER_LIST_URL, timeout=30)
        response.raise_for_status()
        lines = response.text.strip().split('\n')
    except Exception as e:
        print(f"❌ Ошибка загрузки masterfilelist: {e}")
        return []

    # Фильтруем GKG файлы за нужный период
    selected_files = []
    current = start_date

    while current <= end_date:
        # Ищем файл для этого дня (берем полдень 12:00)
        target_time = current.replace(hour=12, minute=0, second=0)
        timestamp = target_time.strftime("%Y%m%d%H%M%S")

        # Ищем ближайший файл
        for line in lines:
            if timestamp in line and '.gkg.csv.zip' in line:
                parts = line.split()
                if len(parts) >= 3:
                    url = parts[2]
                    selected_files.append(url)
                    break

        current += timedelta(hours=hours_interval)

    print(f"✅ Найдено {len(selected_files)} файлов")
    return selected_files


def download_and_extract_file(url: str, output_dir: str) -> str:
    """
    Скачать и распаковать GDELT файл

    Args:
        url: URL файла
        output_dir: директория для сохранения

    Returns:
        Путь к распакованному CSV файлу
    """
    filename = url.split('/')[-1]
    zip_path = os.path.join(output_dir, filename)
    csv_path = zip_path.replace('.zip', '')

    # Скачиваем
    try:
        print(f"   📥 Скачиваю {filename}...")
        response = requests.get(url, stream=True, timeout=60)
        response.raise_for_status()

        with open(zip_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        file_size = os.path.getsize(zip_path) / (1024 * 1024)
        print(f"   ✅ Скачано: {file_size:.1f} MB")

    except Exception as e:
        print(f"   ❌ Ошибка скачивания: {e}")
        return None

    # Распаковываем
    try:
        print(f"   📦 Распаковываю...")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            # ZIP содержит один файл - извлекаем его
            names = zip_ref.namelist()
            if names:
                zip_ref.extract(names[0], output_dir)
                extracted_path = os.path.join(output_dir, names[0])

                # Переименовываем в ожидаемое имя
                if extracted_path != csv_path:
                    if os.path.exists(csv_path):
                        os.remove(csv_path)
                    os.rename(extracted_path, csv_path)

        # Удаляем zip
        os.remove(zip_path)

        unzipped_size = os.path.getsize(csv_path) / (1024 * 1024)
        print(f"   ✅ Распаковано: {unzipped_size:.1f} MB")

        return csv_path

    except Exception as e:
        print(f"   ❌ Ошибка распаковки: {e}")
        return None


def filter_gkg_file(input_csv: str, output_csv: str,
                    language: str = None, country: str = None) -> int:
    """
    Фильтровать GKG файл по языку/стране

    GKG формат (27 колонок):
    0: GKGRECORDID
    1: DATE
    2: SourceCollectionIdentifier
    3: SourceCommonName
    4: DocumentIdentifier (URL)
    ...

    Args:
        input_csv: входной CSV файл
        output_csv: выходной CSV файл
        language: фильтр по языку
        country: фильтр по стране

    Returns:
        Количество отфильтрованных записей
    """
    count = 0

    try:
        with open(input_csv, 'r', encoding='utf-8', errors='ignore') as f_in:
            with open(output_csv, 'a', newline='', encoding='utf-8') as f_out:
                reader = csv.reader(f_in, delimiter='\t')
                writer = csv.writer(f_out)

                for row in reader:
                    if len(row) < 5:
                        continue

                    # Колонка 4 - DocumentIdentifier (URL)
                    url = row[4] if len(row) > 4 else ''

                    # Простая фильтрация по языку/стране через URL
                    # (В GKG нет прямых полей language/country, они выводятся из других данных)

                    if language:
                        # Ищем признаки русского языка в URL или домене
                        if language.lower() == 'russian':
                            if not any(domain in url.lower() for domain in ['.ru/', '.рф/', 'russian', 'russia']):
                                continue

                    if country:
                        if country.lower() == 'russia':
                            if not any(domain in url.lower() for domain in ['.ru/', '.рф/']):
                                continue

                    # Упрощенная запись: date, url, source
                    simple_row = [
                        row[1] if len(row) > 1 else '',  # DATE
                        row[4] if len(row) > 4 else '',  # URL
                        row[3] if len(row) > 3 else '',  # SourceCommonName
                    ]

                    writer.writerow(simple_row)
                    count += 1

        # Удаляем исходный файл
        os.remove(input_csv)

    except Exception as e:
        print(f"   ⚠️  Ошибка фильтрации: {e}")

    return count


def download_gdelt_period(start_date: datetime, end_date: datetime,
                          language: str = None, country: str = None,
                          output_dir: str = "gdelt_raw_data",
                          hours_interval: int = 24) -> int:
    """
    Скачать GDELT файлы за период

    Args:
        start_date: начальная дата
        end_date: конечная дата
        language: фильтр по языку
        country: фильтр по стране
        output_dir: директория для сохранения
        hours_interval: интервал между файлами в часах

    Returns:
        Общее количество записей
    """
    os.makedirs(output_dir, exist_ok=True)

    print("=" * 70)
    print("📥 Скачивание сырых GDELT файлов")
    print("=" * 70)
    print(f"\n📅 Период: {start_date.strftime('%Y-%m-%d')} → {end_date.strftime('%Y-%m-%d')}")
    print(f"🗂️  Выходная директория: {output_dir}/")
    print(f"⏱️  Интервал: {hours_interval} часов ({24 // hours_interval} файлов в день)")

    if language:
        print(f"🌐 Фильтр языка: {language}")
    if country:
        print(f"🌍 Фильтр страны: {country}")

    # Получаем список файлов
    file_urls = get_available_files(start_date, end_date, hours_interval)

    if not file_urls:
        print("\n❌ Не найдено файлов для скачивания")
        return 0

    print(f"\n📊 Всего файлов к скачиванию: {len(file_urls)}")

    # Оценка объема
    avg_file_size = 10  # MB в среднем (сжатый)
    total_size = len(file_urls) * avg_file_size
    print(f"📏 Примерный объем: ~{total_size:.0f} MB сжатых, ~{total_size * 10:.0f} MB распакованных")
    print(f"⏳ Примерное время: ~{len(file_urls) * 0.5:.0f} минут")

    print("\n" + "=" * 70)
    print("⏳ Начинаю скачивание...")
    print("=" * 70)

    # Итоговый CSV файл
    output_csv = os.path.join(
        output_dir,
        f"gdelt_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.csv"
    )

    # Заголовок
    with open(output_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['date', 'url', 'source'])

    total_records = 0

    for i, url in enumerate(file_urls, 1):
        print(f"\n📦 Файл {i}/{len(file_urls)}: {url.split('/')[-1]}")

        # Скачиваем и распаковываем
        csv_path = download_and_extract_file(url, output_dir)

        if not csv_path:
            continue

        # Фильтруем и добавляем в итоговый файл
        print(f"   🔍 Фильтрую...")
        count = filter_gkg_file(csv_path, output_csv, language, country)
        total_records += count
        print(f"   ✅ Добавлено записей: {count:,}")

        # Небольшая задержка между файлами
        if i < len(file_urls):
            time.sleep(1)

    print("\n" + "=" * 70)
    print("✅ Скачивание завершено!")
    print("=" * 70)
    print(f"\n📊 Всего записей: {total_records:,}")

    if total_records > 0:
        file_size = os.path.getsize(output_csv) / (1024 * 1024)
        print(f"💾 Сохранено в: {output_csv}")
        print(f"📏 Размер файла: {file_size:.1f} MB")

    return total_records


def main():
    parser = argparse.ArgumentParser(
        description="Скачивание сырых GDELT GKG файлов",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # За 2 месяца (по 1 файлу в день)
  python3 download_gdelt_files.py --start-date 2024-09-01 --end-date 2024-10-31

  # С фильтром по России
  python3 download_gdelt_files.py --start-date 2024-09-01 --end-date 2024-10-31 --language Russian

  # Больше файлов (каждые 6 часов)
  python3 download_gdelt_files.py --start-date 2024-10-01 --end-date 2024-10-31 --interval 6
        """
    )

    parser.add_argument("--start-date", required=True, help="Начальная дата (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="Конечная дата (YYYY-MM-DD)")
    parser.add_argument("--language", help="Фильтр по языку (Russian, English, etc)")
    parser.add_argument("--country", help="Фильтр по стране (Russia, US, etc)")
    parser.add_argument("--output-dir", default="gdelt_raw_data", help="Директория для сохранения")
    parser.add_argument("--interval", type=int, default=24,
                       help="Интервал между файлами в часах (по умолчанию: 24)")

    args = parser.parse_args()

    # Парсим даты
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    # Скачиваем
    try:
        total = download_gdelt_period(
            start_date=start_date,
            end_date=end_date,
            language=args.language,
            country=args.country,
            output_dir=args.output_dir,
            hours_interval=args.interval
        )

        print("\n🎉 Готово!")

        if total == 0:
            print("\n⚠️  Не найдено записей с указанными фильтрами")

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
