#!/usr/bin/env python3
"""
Генератор датасета для Learning to Rank
Создает пары (запрос, новость) с фичами для ручной разметки
"""

import sqlite3
import json
import numpy as np
from news_rag_system import NewsRAGSystem
from typing import List, Dict
import re

DB_PATH = "/Users/david/bank_news_agent/news_database.db"

# Типичные запросы для банковской тематики
SAMPLE_QUERIES = [
    # Банки
    "Сбербанк",
    "ВТБ",
    "Альфа-Банк",
    "Тинькофф",
    "Газпромбанк",

    # Регуляторы
    "Центральный банк",
    "ЦБ РФ",
    "Банк России",

    # Продукты
    "ипотека",
    "кредит",
    "вклады",
    "ключевая ставка",

    # События
    "санкции",
    "курс доллара",
    "курс евро",
    "инфляция",

    # Комбинированные
    "Сбербанк ипотека",
    "ЦБ повысил ставку",
    "ВТБ кредиты",
    "санкции против банков",
    "доллар растет",
]


class LTRDatasetGenerator:
    def __init__(self):
        self.rag = NewsRAGSystem()
        self.conn = sqlite3.connect(DB_PATH)
        self.cursor = self.conn.cursor()

    def calculate_features(self, query: str, news: Dict) -> Dict:
        """Вычисляет все фичи для пары (запрос, новость)"""

        features = {}

        # 1. Embedding similarity (уже есть)
        features['embedding_score'] = news.get('similarity', 0.0)

        # 2. BM25 score (аппроксимация через TF-IDF)
        features['bm25_score'] = self._calculate_bm25_approx(query, news)

        # 3. NER overlap
        features['ner_overlap'] = self._calculate_ner_overlap(query, news['id'])

        # 4. Morphological match
        features['morpho_match'] = self._calculate_morpho_match(query, news)

        # 5. Title match
        features['title_match'] = self._calculate_title_match(query, news['title'])

        # 6. Exact match
        features['exact_match'] = 1.0 if query.lower() in news['title'].lower() else 0.0

        # 7. Date recency (дней назад)
        features['days_ago'] = self._calculate_days_ago(news.get('published', ''))

        # 8. Source authority (пока простая эвристика)
        features['source_authority'] = self._get_source_authority(news.get('source', ''))

        # 9. Text length
        title_len = len(news.get('title', ''))
        desc_len = len(news.get('description', ''))
        features['text_length'] = title_len + desc_len

        return features

    def _calculate_bm25_approx(self, query: str, news: Dict) -> float:
        """Упрощенная аппроксимация BM25 через подсчет совпадающих слов"""
        query_words = set(query.lower().split())
        text = f"{news.get('title', '')} {news.get('description', '')}".lower()

        matches = sum(1 for word in query_words if word in text)
        return matches / len(query_words) if query_words else 0.0

    def _calculate_ner_overlap(self, query: str, news_id: int) -> float:
        """Процент совпадающих NER-сущностей"""
        # Извлекаем сущности из запроса
        query_entities = self.rag.ner_extractor.extract_from_news(query, "")
        query_ner_set = set(e['normalized'].lower() for e in query_entities['all'])

        if not query_ner_set:
            return 0.0

        # Извлекаем сущности из новости
        self.cursor.execute('''
            SELECT normalized_text FROM entities WHERE news_id = ?
        ''', (news_id,))

        news_ner_set = set(row[0].lower() for row in self.cursor.fetchall() if row[0])

        if not news_ner_set:
            return 0.0

        # Jaccard similarity
        intersection = query_ner_set & news_ner_set
        union = query_ner_set | news_ner_set

        return len(intersection) / len(union) if union else 0.0

    def _calculate_morpho_match(self, query: str, news: Dict) -> float:
        """Морфологическое совпадение"""
        import pymorphy2
        morph = pymorphy2.MorphAnalyzer()

        query_words = query.lower().split()
        text = f"{news.get('title', '')} {news.get('description', '')}".lower()

        matches = 0
        for word in query_words:
            parsed = morph.parse(word)[0]
            normal_form = parsed.normal_form

            if normal_form in text or word in text:
                matches += 1

        return matches / len(query_words) if query_words else 0.0

    def _calculate_title_match(self, query: str, title: str) -> float:
        """Совпадение с заголовком"""
        query_words = set(query.lower().split())
        title_words = set(title.lower().split())

        if not query_words:
            return 0.0

        matches = query_words & title_words
        return len(matches) / len(query_words)

    def _calculate_days_ago(self, published: str) -> float:
        """Количество дней назад (чем меньше, тем свежее)"""
        if not published:
            return 999.0  # очень старая

        try:
            from datetime import datetime
            from email.utils import parsedate_to_datetime

            if published.startswith(('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')):
                pub_date = parsedate_to_datetime(published)
            else:
                pub_date = datetime.fromisoformat(published.replace('Z', '+00:00'))

            now = datetime.now(pub_date.tzinfo)
            delta = (now - pub_date).days
            return max(0, delta)
        except:
            return 999.0

    def _get_source_authority(self, source: str) -> float:
        """Оценка авторитетности источника"""
        high_authority = ['РБК', 'Коммерсантъ', 'Ведомости', 'Интерфакс', 'ТАСС']
        medium_authority = ['Известия', 'Российская газета', 'Banki.ru']

        for auth_source in high_authority:
            if auth_source.lower() in source.lower():
                return 1.0

        for auth_source in medium_authority:
            if auth_source.lower() in source.lower():
                return 0.5

        return 0.0

    def generate_candidates(self, query: str, top_k: int = 20) -> List[Dict]:
        """Генерирует топ-K кандидатов для запроса"""
        print(f"\n🔍 Запрос: '{query}'")

        # Получаем топ-K из текущей системы
        results = self.rag.search_similar(query, top_k=top_k)

        candidates = []
        for rank, news in enumerate(results, 1):
            features = self.calculate_features(query, news)

            candidate = {
                'query': query,
                'rank': rank,
                'news_id': news['id'],
                'title': news['title'],
                'description': news.get('description', ''),
                'source': news.get('source', ''),
                'published': news.get('published', ''),
                'link': news.get('link', ''),
                'features': features,
                'label': None  # Заполнится при разметке
            }

            candidates.append(candidate)

        print(f"  ✓ Сгенерировано {len(candidates)} кандидатов")
        return candidates

    def generate_dataset(self, queries: List[str] = None, output_file: str = "ltr_dataset.json"):
        """Генерирует полный датасет для разметки"""
        if queries is None:
            queries = SAMPLE_QUERIES

        print("=" * 70)
        print("🎯 Генерация датасета для Learning to Rank")
        print("=" * 70)

        dataset = []

        for query in queries:
            candidates = self.generate_candidates(query, top_k=20)
            dataset.extend(candidates)

        # Сохраняем
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(dataset, f, ensure_ascii=False, indent=2)

        print(f"\n✅ Датасет сохранен: {output_file}")
        print(f"   Всего пар (запрос, новость): {len(dataset)}")
        print(f"   Уникальных запросов: {len(queries)}")
        print(f"\n💡 Следующий шаг: откройте ltr_annotator.html для разметки")

        self.conn.close()
        return dataset


if __name__ == "__main__":
    generator = LTRDatasetGenerator()
    dataset = generator.generate_dataset()
