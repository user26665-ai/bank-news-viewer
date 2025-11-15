#!/usr/bin/env python3
"""
Helper скрипт для проверки настройки BigQuery

Проверяет:
- Наличие необходимых библиотек
- Project ID
- Доступ к BigQuery
- Доступ к GDELT публичным датасетам
"""

import sys
import os

def check_libraries():
    """Проверка установленных библиотек"""
    print("=" * 70)
    print("📚 Проверка библиотек")
    print("=" * 70)

    required_libs = {
        'pandas': 'pandas',
        'pandas_gbq': 'pandas-gbq',
        'google.cloud.bigquery': 'google-cloud-bigquery'
    }

    all_ok = True
    for module, package in required_libs.items():
        try:
            __import__(module)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} - НЕ УСТАНОВЛЕН")
            print(f"   Установите: pip3 install {package}")
            all_ok = False

    return all_ok


def check_project_id():
    """Проверка наличия Project ID"""
    print("\n" + "=" * 70)
    print("🔑 Проверка Project ID")
    print("=" * 70)

    project_id = os.environ.get('GOOGLE_CLOUD_PROJECT')

    if project_id:
        print(f"✅ GOOGLE_CLOUD_PROJECT = '{project_id}'")
        return True, project_id
    else:
        print("❌ GOOGLE_CLOUD_PROJECT не установлен")
        print("\n📝 Что нужно сделать:")
        print("1. Создайте проект в console.cloud.google.com")
        print("2. Скопируйте Project ID")
        print("3. Установите переменную:")
        print("   export GOOGLE_CLOUD_PROJECT='your-project-id'")
        print("\nПодробная инструкция: GDELT_BIGQUERY_SETUP.md")
        return False, None


def check_bigquery_access(project_id):
    """Проверка доступа к BigQuery"""
    print("\n" + "=" * 70)
    print("🔌 Проверка доступа к BigQuery")
    print("=" * 70)

    try:
        import pandas_gbq

        # Простейший запрос к GDELT публичному датасету
        query = """
        SELECT
            url,
            title,
            language
        FROM
            `gdelt-bq.gdeltv2.gal`
        WHERE
            DATE(seendate) = CURRENT_DATE()
            AND language = 'Russian'
        LIMIT 3
        """

        print("🔄 Выполняю тестовый запрос к GDELT...")
        print(f"   Project ID: {project_id}")

        df = pandas_gbq.read_gbq(
            query,
            project_id=project_id,
            progress_bar_type=None
        )

        print(f"\n✅ Запрос выполнен успешно!")
        print(f"   Получено записей: {len(df)}")

        if len(df) > 0:
            print("\n📰 Примеры новостей:")
            for i, row in df.iterrows():
                print(f"\n{i+1}. {row['title']}")
                print(f"   URL: {row['url'][:80]}...")

        return True

    except Exception as e:
        print(f"\n❌ Ошибка: {e}")

        error_msg = str(e).lower()

        if "credentials" in error_msg or "authentication" in error_msg:
            print("\n⚠️  Требуется аутентификация")
            print("\n📝 Что нужно сделать:")
            print("1. Запустите скрипт download_gdelt_bigquery.py")
            print("2. При первом запуске откроется браузер")
            print("3. Войдите в Google аккаунт и разрешите доступ")

        elif "permission" in error_msg or "access denied" in error_msg:
            print("\n⚠️  Нет прав доступа")
            print("\n📝 Что нужно сделать:")
            print("1. Убедитесь, что BigQuery API включен в вашем проекте:")
            print("   https://console.cloud.google.com/apis/library/bigquery.googleapis.com")
            print("2. Включите биллинг (бесплатный tier):")
            print("   https://console.cloud.google.com/billing")

        elif "quota" in error_msg:
            print("\n⚠️  Превышена квота")
            print("\n📝 Вы превысили бесплатный лимит 1 TB/месяц")

        return False


def main():
    print("\n🔍 Проверка настройки GDELT BigQuery")
    print("=" * 70)

    # Проверка библиотек
    if not check_libraries():
        print("\n❌ Установите недостающие библиотеки и запустите снова")
        return 1

    # Проверка Project ID
    has_project, project_id = check_project_id()
    if not has_project:
        print("\n❌ Настройте Project ID и запустите снова")
        return 1

    # Проверка доступа к BigQuery
    if not check_bigquery_access(project_id):
        print("\n❌ Настройте доступ к BigQuery и запустите снова")
        return 1

    # Все ОК!
    print("\n" + "=" * 70)
    print("✅ ВСЕ ПРОВЕРКИ ПРОЙДЕНЫ!")
    print("=" * 70)
    print("\n🎉 Вы готовы к загрузке данных GDELT!")
    print("\n📝 Следующие шаги:")
    print("\n1. Тестовая загрузка (1 день):")
    print(f"   python3 download_gdelt_bigquery.py \\")
    print(f"     --start-date 2024-11-13 --end-date 2024-11-13 \\")
    print(f"     --language Russian --output-dir gdelt_test\n")
    print("2. Полная загрузка за 2024 год:")
    print(f"   python3 download_gdelt_bigquery.py \\")
    print(f"     --start-date 2024-01-01 --end-date 2024-12-31 \\")
    print(f"     --language Russian --output-dir gdelt_2024\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
