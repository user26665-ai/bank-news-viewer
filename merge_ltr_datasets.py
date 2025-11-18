#!/usr/bin/env python3
"""
Склейка нескольких LTR датасетов в один

Использование:
    python3 merge_ltr_datasets.py dataset1.json dataset2.json dataset3.json -o merged_dataset.json
"""

import json
import argparse
from pathlib import Path
from typing import List, Dict


def merge_datasets(input_files: List[str], output_file: str, remove_duplicates: bool = True):
    """
    Склеивает несколько JSON датасетов в один

    Args:
        input_files: Список путей к входным JSON файлам
        output_file: Путь к выходному JSON файлу
        remove_duplicates: Удалять ли дубликаты (по query + news_id)
    """

    all_data = []
    seen_pairs = set()  # (query, news_id) для удаления дубликатов

    print("=" * 70)
    print("🔗 Склейка LTR датасетов")
    print("=" * 70)
    print()

    # Читаем все файлы
    for i, file_path in enumerate(input_files, 1):
        print(f"{i}. Загружаю {file_path}...", end=" ")

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            if not isinstance(data, list):
                print(f"❌ Неверный формат (ожидался список)")
                continue

            # Фильтруем дубликаты
            added = 0
            for item in data:
                pair_key = (item.get('query', ''), item.get('news_id', -1))

                if remove_duplicates:
                    if pair_key not in seen_pairs:
                        all_data.append(item)
                        seen_pairs.add(pair_key)
                        added += 1
                else:
                    all_data.append(item)
                    added += 1

            print(f"✓ Загружено {len(data)} примеров, добавлено {added}")

        except FileNotFoundError:
            print(f"❌ Файл не найден")
        except json.JSONDecodeError:
            print(f"❌ Ошибка парсинга JSON")
        except Exception as e:
            print(f"❌ Ошибка: {e}")

    print()
    print("=" * 70)
    print(f"📊 Статистика:")
    print(f"   Всего примеров: {len(all_data)}")

    # Статистика по label
    labeled = [item for item in all_data if item.get('label') is not None]
    print(f"   Размечено: {len(labeled)}")

    if labeled:
        label_counts = {}
        for item in labeled:
            label = item.get('label', -1)
            label_counts[label] = label_counts.get(label, 0) + 1

        print(f"   Распределение:")
        for label in sorted(label_counts.keys()):
            emoji = {0: "❌", 1: "🤔", 2: "✅", 3: "⭐"}.get(label, "?")
            count = label_counts[label]
            bar = "█" * int(count / len(labeled) * 30)
            print(f"      {emoji} {label}: {count:4d} ({count/len(labeled)*100:.1f}%) {bar}")

    # Статистика по запросам
    unique_queries = len(set(item.get('query', '') for item in all_data))
    print(f"   Уникальных запросов: {unique_queries}")

    print("=" * 70)
    print()

    # Сохраняем результат
    print(f"💾 Сохраняю в {output_file}...", end=" ")
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        print("✓ Готово!")
        print()
        print(f"✅ Успешно склеено {len(input_files)} датасетов → {len(all_data)} примеров")
        return True
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Склеивает несколько LTR датасетов в один",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Склеить 2 файла
  python3 merge_ltr_datasets.py day1.json day2.json -o merged.json

  # Склеить все JSON файлы в папке
  python3 merge_ltr_datasets.py datasets/*.json -o all_data.json

  # Склеить с сохранением дубликатов
  python3 merge_ltr_datasets.py file1.json file2.json -o merged.json --keep-duplicates
        """
    )

    parser.add_argument('inputs', nargs='+', help='Входные JSON файлы с датасетами')
    parser.add_argument('-o', '--output', required=True, help='Выходной JSON файл')
    parser.add_argument('--keep-duplicates', action='store_true',
                        help='Не удалять дубликаты (по query + news_id)')

    args = parser.parse_args()

    success = merge_datasets(
        input_files=args.inputs,
        output_file=args.output,
        remove_duplicates=not args.keep_duplicates
    )

    exit(0 if success else 1)


if __name__ == "__main__":
    main()
