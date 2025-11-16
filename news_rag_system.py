#!/usr/bin/env python3
"""
Масштабируемая RAG-система для работы с новостями
Поддерживает множество источников, векторный поиск и автоматическую индексацию
Использует легковесные RSS-фиды без web scraping
"""

import sqlite3
import json
import feedparser
import requests
import hashlib
from datetime import datetime, timedelta
import numpy as np
from typing import List, Dict, Optional
import time
import asyncio
import httpx
import re
from html import unescape
from news_ner import NewsNERExtractor
from sentence_transformers import SentenceTransformer

# Настройки
DB_PATH = "/Users/david/bank_news_agent/news_database.db"
SOURCES_PATH = "/Users/david/bank_news_agent/news_sources.json"
LM_STUDIO_API = "http://localhost:1234/v1"
EMBEDDING_MODEL = "BAAI/bge-m3"  # Изменено на BGE-M3

class NewsRAGSystem:
    def __init__(self):
        self.db_path = DB_PATH
        self.ner_extractor = NewsNERExtractor()

        # Инициализация BGE-M3 модели
        print("Загрузка BGE-M3 модели...")
        self.embedding_model = SentenceTransformer(EMBEDDING_MODEL)
        print("✓ BGE-M3 загружена")

        self.init_database()

    def init_database(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Таблица новостей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                hash TEXT UNIQUE,
                source TEXT,
                category TEXT,
                title TEXT,
                description TEXT,
                link TEXT,
                published TEXT,
                fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                embedding BLOB
            )
        ''')

        # Добавляем поле full_text если его нет
        cursor.execute("PRAGMA table_info(news)")
        columns = [col[1] for col in cursor.fetchall()]

        if 'full_text' not in columns:
            cursor.execute('ALTER TABLE news ADD COLUMN full_text TEXT')

        if 'content_hash' not in columns:
            cursor.execute('ALTER TABLE news ADD COLUMN content_hash TEXT')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_content_hash ON news(content_hash)')

        # Индекс для быстрого поиска
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_source ON news(source)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_category ON news(category)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_published ON news(published)')

        # Таблица NER-сущностей
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                news_id INTEGER,
                entity_text TEXT,
                entity_type TEXT,
                position INTEGER,
                is_banking BOOLEAN DEFAULT 0,
                FOREIGN KEY (news_id) REFERENCES news(id) ON DELETE CASCADE
            )
        ''')

        # Добавляем поле normalized_text если его нет
        cursor.execute("PRAGMA table_info(entities)")
        entity_columns = [col[1] for col in cursor.fetchall()]

        if 'normalized_text' not in entity_columns:
            cursor.execute('ALTER TABLE entities ADD COLUMN normalized_text TEXT')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_normalized_text ON entities(normalized_text)')

        # Индексы для быстрого поиска по сущностям
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_entity_text ON entities(entity_text)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_entity_type ON entities(entity_type)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_news_id ON entities(news_id)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_is_banking ON entities(is_banking)')

        conn.commit()
        conn.close()

    def load_sources(self) -> List[Dict]:
        """Загрузить список источников"""
        try:
            with open(SOURCES_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return [s for s in data['sources'] if s.get('enabled', True)]
        except Exception as e:
            print(f"Ошибка загрузки источников: {e}")
            return []

    def generate_hash(self, title: str, link: str) -> str:
        """Генерация хеша для дедупликации"""
        content = f"{title}:{link}"
        return hashlib.md5(content.encode()).hexdigest()

    def clean_html(self, text: str) -> str:
        """Очистка HTML тегов и лишних пробелов из текста"""
        if not text:
            return ""

        # Удаляем HTML теги
        text = re.sub(r'<[^>]+>', ' ', text)

        # Декодируем HTML entities
        text = unescape(text)

        # Удаляем множественные пробелы и переносы
        text = re.sub(r'\s+', ' ', text)

        # Убираем пробелы в начале и конце
        text = text.strip()

        return text

    def extract_rss_content(self, entry) -> dict:
        """
        Улучшенное извлечение контента из RSS entry
        Пытается получить максимум информации из различных полей RSS
        """
        # Заголовок
        title = entry.get('title', 'Без заголовка')
        title = self.clean_html(title)

        # Извлекаем РЕАЛЬНЫЙ источник (СМИ) из RSS
        real_source = None

        # Способ 1: Поле 'source' в RSS (предпочтительно)
        if 'source' in entry and entry['source'] and 'title' in entry['source']:
            real_source = entry['source']['title']

        # Способ 2: Из заголовка новости (после последнего " - ")
        if not real_source and ' - ' in title:
            parts = title.split(' - ')
            if len(parts) >= 2:
                # Последняя часть - это источник
                real_source = parts[-1].strip()
                # Убираем источник из заголовка
                title = ' - '.join(parts[:-1]).strip()

        # Описание - пробуем разные поля
        description = ""
        if 'content' in entry and entry['content']:
            # content:encoded - обычно содержит полный текст
            description = entry['content'][0].get('value', '')
        elif 'summary' in entry:
            description = entry.get('summary', '')
        elif 'description' in entry:
            description = entry.get('description', '')

        description = self.clean_html(description)

        # Если описание слишком короткое, попробуем summary_detail
        if len(description) < 50 and 'summary_detail' in entry:
            summary_detail = entry['summary_detail'].get('value', '')
            if len(summary_detail) > len(description):
                description = self.clean_html(summary_detail)

        # Ссылка
        link = entry.get('link', '')

        # Дата публикации - пробуем разные форматы
        published = entry.get('published', '')
        if not published and 'updated' in entry:
            published = entry.get('updated', '')
        if not published and 'created' in entry:
            published = entry.get('created', '')

        # Автор
        author = entry.get('author', '')
        if not author and 'authors' in entry and entry['authors']:
            author = entry['authors'][0].get('name', '')

        # Категории/теги
        tags = []
        if 'tags' in entry:
            tags = [tag.get('term', '') for tag in entry['tags']]

        return {
            'title': title,
            'description': description,
            'link': link,
            'published': published,
            'author': author,
            'tags': tags,
            'real_source': real_source  # Реальный источник (СМИ)
        }

    def save_entities(self, news_id: int, title: str, description: str, conn=None):
        """
        Извлечь и сохранить NER-сущности для новости

        Args:
            news_id: ID новости в БД
            title: заголовок новости
            description: описание новости
            conn: существующее соединение с БД (опционально)
        """
        close_conn = False
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            close_conn = True

        try:
            cursor = conn.cursor()

            # Извлекаем сущности
            result = self.ner_extractor.extract_from_news(title, description)

            # Сохраняем каждую сущность
            for idx, entity in enumerate(result['all']):
                is_banking = self.ner_extractor.is_banking_entity(entity['text'])
                normalized = entity.get('normalized', entity['text'])

                cursor.execute('''
                    INSERT INTO entities (news_id, entity_text, entity_type, position, is_banking, normalized_text)
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', (news_id, entity['text'], entity['type'], idx, is_banking, normalized))

            conn.commit()

        except Exception as e:
            print(f"Ошибка сохранения entities: {e}")

        finally:
            if close_conn:
                conn.close()

    def get_embedding(self, text: str) -> np.ndarray:
        """Получить embedding через BGE-M3"""
        try:
            # Ограничиваем длину текста
            text = text[:8000]

            # Используем BGE-M3 для создания эмбеддинга
            embedding = self.embedding_model.encode(text, normalize_embeddings=True)
            return np.array(embedding, dtype=np.float32)
        except Exception as e:
            print(f"Ошибка получения embedding: {e}")
        return None


    async def get_embedding_async(self, text: str, timeout: int = 30) -> Optional[np.ndarray]:
        """Асинхронное получение embedding через BGE-M3"""
        try:
            # Запускаем синхронный метод в executor для асинхронности
            loop = asyncio.get_event_loop()
            embedding = await loop.run_in_executor(None, self.get_embedding, text[:8000])
            return embedding
        except Exception as e:
            pass
        return None

    def fetch_and_index_news(self, limit_per_source: int = 200):
        """Загрузить и проиндексировать новости из всех источников"""
        sources = self.load_sources()
        total_new = 0
        total_updated = 0

        print(f"📡 Загрузка новостей из {len(sources)} источников...\n")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        for source in sources:
            print(f"  • {source['name']}...", end=" ")
            try:
                feed = feedparser.parse(source['url'], agent='Mozilla/5.0')
                new_count = 0

                for entry in feed.entries[:limit_per_source]:
                    # Улучшенное извлечение данных из RSS
                    content = self.extract_rss_content(entry)

                    title = content['title']
                    description = content['description']
                    link = content['link']
                    published = content['published']

                    if not title or not link:
                        continue

                    # Генерируем хеш для дедупликации
                    content_hash = hashlib.md5(f"{title}{link}".encode()).hexdigest()

                    # Проверяем, есть ли уже эта новость
                    cursor.execute('SELECT id FROM news WHERE content_hash = ?', (content_hash,))
                    if cursor.fetchone():
                        continue

                    # Используем очищенные данные из RSS
                    full_text = description
                    # Если описание достаточно длинное, используем его полностью
                    embed_text = f"{title}\n\n{description}"

                    # Получаем embedding
                    embedding = self.get_embedding(embed_text)
                    if embedding is None:
                        # Если не удалось получить embedding, пропускаем
                        continue

                    # Сохраняем в базу с полным текстом
                    try:
                        cursor.execute('''
                            INSERT INTO news (
                                hash, source, category, title, description, link,
                                published, embedding, content_hash, full_text
                            )
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            content_hash,  # используем content_hash для обоих полей
                            source['name'],
                            source.get('category', 'general'),
                            title,
                            description,
                            link,
                            published,
                            embedding.tobytes(),
                            content_hash,
                            full_text
                        ))
                    except sqlite3.IntegrityError:
                        # Дубликат - пропускаем
                        continue
                    new_count += 1
                    total_new += 1

                    # Небольшая пауза между запросами
                    time.sleep(0.3)

                print(f"✓ {new_count} новых")
                time.sleep(0.5)  # Небольшая пауза между источниками

            except Exception as e:
                print(f"✗ Ошибка: {e}")

        conn.commit()
        conn.close()

        print(f"\n✅ Всего добавлено: {total_new} новых новостей")
        return total_new

    async def fetch_and_index_news_async(self, limit_per_source: int = 200, max_concurrent: int = 5, max_age_days: int = 0):
        """
        Асинхронная загрузка и индексация новостей (не блокирует FastAPI)

        Args:
            limit_per_source: максимум новостей с одного источника
            max_concurrent: максимум параллельных запросов для embeddings
            max_age_days: загружать только новости за последние N дней (0 = все новости)
        """
        sources = self.load_sources()
        total_new = 0

        print(f"\n{'='*70}")
        print(f"📡 [ASYNC] Загрузка новостей из {len(sources)} источников...")
        print(f"⚡ Параллельных запросов: {max_concurrent}")
        if max_age_days > 0:
            print(f"📅 Фильтр: только новости за последние {max_age_days} дня(дней)")
        print(f"{'='*70}\n")

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        async def process_single_article(entry, source_name, category):
            """Обработка одной статьи: улучшенный парсинг RSS + embedding"""
            # Улучшенное извлечение данных из RSS
            content = self.extract_rss_content(entry)

            title = content['title']
            description = content['description']
            link = content['link']
            published = content['published']

            # Используем РЕАЛЬНЫЙ источник (СМИ) если есть, иначе - название из конфига
            real_source = content.get('real_source')
            final_source = real_source if real_source else source_name

            if not title or not link:
                return None

            # Генерируем хеш для дедупликации
            content_hash = hashlib.md5(f"{title}{link}".encode()).hexdigest()

            # Проверяем дубликаты
            cursor.execute('SELECT id FROM news WHERE content_hash = ?', (content_hash,))
            if cursor.fetchone():
                return None  # Уже есть

            # Используем очищенные данные из RSS
            full_text = description
            embed_text = f"{title}\n\n{description}"

            # Создаем embedding асинхронно
            embedding = await self.get_embedding_async(embed_text, timeout=30)

            if embedding is None:
                return None

            # Возвращаем данные для вставки
            return (
                content_hash,  # hash
                final_source,  # source - РЕАЛЬНЫЙ источник (СМИ)!
                category,  # category
                title,
                description,
                link,
                published,
                embedding.tobytes(),
                content_hash,  # content_hash
                full_text  # full_text
            )

        for source in sources:
            source_name = source['name']
            source_url = source['url']
            category = source.get('category', 'general')

            print(f"  • {source_name}...", end=" ", flush=True)

            try:
                # Загрузка RSS (в отдельном треде, чтобы не блокировать)
                loop = asyncio.get_event_loop()
                feed = await loop.run_in_executor(None, lambda: feedparser.parse(source_url, agent='Mozilla/5.0'))

                # Фильтрация по дате (только свежие новости за последние N дней)
                if max_age_days > 0:
                    cutoff_time = datetime.now() - timedelta(days=max_age_days)
                    filtered_entries = []

                    for entry in feed.entries[:limit_per_source]:
                        # Проверяем дату публикации
                        if hasattr(entry, 'published_parsed') and entry.published_parsed:
                            try:
                                entry_time = datetime(*entry.published_parsed[:6])
                                if entry_time >= cutoff_time:
                                    filtered_entries.append(entry)
                            except:
                                # Если не удалось распарсить дату - добавляем новость
                                filtered_entries.append(entry)
                        else:
                            # Если нет даты - добавляем новость (чтобы не потерять)
                            filtered_entries.append(entry)

                    entries = filtered_entries
                else:
                    entries = feed.entries[:limit_per_source]

                # Обрабатываем статьи параллельно (создаем embeddings одновременно)
                semaphore = asyncio.Semaphore(max_concurrent)

                async def process_with_semaphore(entry):
                    async with semaphore:
                        return await process_single_article(entry, source_name, category)

                # Запускаем параллельную обработку embeddings
                tasks = [process_with_semaphore(entry) for entry in entries]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # Сохраняем в БД
                new_count = 0
                for result in results:
                    if result and not isinstance(result, Exception):
                        try:
                            cursor.execute('''
                                INSERT INTO news (
                                    hash, source, category, title, description, link,
                                    published, embedding, content_hash, full_text
                                )
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', result)

                            # Получаем ID новой записи
                            news_id = cursor.lastrowid

                            # Извлекаем и сохраняем NER-сущности
                            # result[3] = title, result[4] = description
                            self.save_entities(news_id, result[3], result[4], conn)

                            new_count += 1
                            total_new += 1
                        except sqlite3.IntegrityError:
                            # Дубликат - пропускаем
                            pass

                conn.commit()
                print(f"✓ {new_count} новых")

            except Exception as e:
                print(f"✗ Ошибка: {e}")

        conn.close()

        print(f"\n✅ [ASYNC] Всего добавлено: {total_new} новых новостей")
        return total_new

    def search_similar(self, query: str, top_k: int = 10, category: str = None) -> List[Dict]:
        """Поиск похожих новостей с использованием векторного поиска"""
        # Получаем embedding запроса
        query_embedding = self.get_embedding(query)
        if query_embedding is None:
            print("Не удалось получить embedding для запроса")
            return []

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Загружаем все новости (с фильтром по категории если нужно)
        if category:
            cursor.execute('SELECT id, title, description, link, source, published, embedding FROM news WHERE category = ?', (category,))
        else:
            cursor.execute('SELECT id, title, description, link, source, published, embedding FROM news')

        results = []
        for row in cursor.fetchall():
            news_id, title, description, link, source, published, embedding_blob = row

            # Восстанавливаем embedding
            news_embedding = np.frombuffer(embedding_blob, dtype=np.float32)

            # Вычисляем косинусное сходство
            similarity = np.dot(query_embedding, news_embedding) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(news_embedding)
            )

            results.append({
                'id': news_id,
                'title': title,
                'description': description,
                'link': link,
                'source': source,
                'published': published,
                'similarity': float(similarity)
            })

        # Сортируем по схожести
        results.sort(key=lambda x: x['similarity'], reverse=True)

        conn.close()
        return results[:top_k]

    def get_stats(self) -> Dict:
        """Получить статистику по базе"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT COUNT(*) FROM news')
        total = cursor.fetchone()[0]

        cursor.execute('SELECT source, COUNT(*) FROM news GROUP BY source')
        by_source = dict(cursor.fetchall())

        cursor.execute('SELECT category, COUNT(*) FROM news GROUP BY category')
        by_category = dict(cursor.fetchall())

        conn.close()

        return {
            'total': total,
            'by_source': by_source,
            'by_category': by_category
        }

def main():
    print("=" * 70)
    print("🚀 RAG-система для новостей (масштабируемая версия)")
    print("=" * 70)
    print()

    system = NewsRAGSystem()

    # Показываем текущую статистику
    stats = system.get_stats()
    print(f"📊 Текущая статистика:")
    print(f"   Всего новостей: {stats['total']}")
    if stats['by_source']:
        print(f"   По источникам:")
        for source, count in stats['by_source'].items():
            print(f"     • {source}: {count}")
    print()

    # Индексируем новости
    new_count = system.fetch_and_index_news()

    # Обновленная статистика
    if new_count > 0:
        stats = system.get_stats()
        print(f"\n📊 Обновленная статистика:")
        print(f"   Всего новостей: {stats['total']}")

if __name__ == "__main__":
    main()
