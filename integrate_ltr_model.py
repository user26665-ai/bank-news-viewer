#!/usr/bin/env python3
"""
Интеграция LTR модели в NewsRAGSystem
"""

import pickle
import numpy as np
from news_rag_system import NewsRAGSystem
from typing import List, Dict


class LTRNewsRAGSystem(NewsRAGSystem):
    """RAG система с переранжированием через LTR модель"""

    def __init__(self, ltr_model_path='ltr_model.pkl'):
        super().__init__()

        # Подключение к БД (для доступа к NER-сущностям)
        import sqlite3
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        # Загружаем LTR модель
        with open(ltr_model_path, 'rb') as f:
            model_data = pickle.load(f)

        self.ltr_model = model_data['model']
        self.feature_columns = model_data['feature_columns']

        print(f"✓ LTR модель загружена: {ltr_model_path}")
        print(f"  Фичи: {', '.join(self.feature_columns)}")

    def calculate_features(self, query: str, news: Dict) -> Dict:
        """Вычисляет фичи для одной новости (аналогично ltr_dataset_generator)"""

        features = {}

        # 1. Embedding similarity
        features['embedding_score'] = news.get('similarity', 0.0)

        # 2. BM25 score (аппроксимация)
        features['bm25_score'] = self._calculate_bm25_approx(query, news)

        # 3. NER overlap
        features['ner_overlap'] = self._calculate_ner_overlap(query, news['id'])

        # 4. Morphological match
        features['morpho_match'] = self._calculate_morpho_match(query, news)

        # 5. Title match
        features['title_match'] = self._calculate_title_match(query, news['title'])

        # 6. Exact match
        features['exact_match'] = 1.0 if query.lower() in news['title'].lower() else 0.0

        # 7. Date recency
        features['days_ago'] = self._calculate_days_ago(news.get('published', ''))

        # 8. Source authority
        features['source_authority'] = self._get_source_authority(news.get('source', ''))

        # 9. Text length
        title_len = len(news.get('title', ''))
        desc_len = len(news.get('description', ''))
        features['text_length'] = title_len + desc_len

        return features

    def _calculate_bm25_approx(self, query: str, news: Dict) -> float:
        """Упрощенная аппроксимация BM25"""
        query_words = set(query.lower().split())
        text = f"{news.get('title', '')} {news.get('description', '')}".lower()

        matches = sum(1 for word in query_words if word in text)
        return matches / len(query_words) if query_words else 0.0

    def _calculate_ner_overlap(self, query: str, news_id: int) -> float:
        """Процент совпадающих NER-сущностей"""
        query_entities = self.ner_extractor.extract_from_news(query, "")
        query_ner_set = set(e['normalized'].lower() for e in query_entities['all'])

        if not query_ner_set:
            return 0.0

        self.cursor.execute('''
            SELECT normalized_text FROM entities WHERE news_id = ?
        ''', (news_id,))

        news_ner_set = set(row[0].lower() for row in self.cursor.fetchall() if row[0])

        if not news_ner_set:
            return 0.0

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
        """Количество дней назад"""
        if not published:
            return 999.0

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

    def search_similar(self, query: str, top_k: int = 10) -> List[Dict]:
        """
        Поиск с переранжированием через LTR модель

        1. Получаем топ-100 кандидатов через embedding
        2. Вычисляем все фичи для каждого кандидата
        3. Переранжируем через LTR модель
        4. Возвращаем топ-K
        """

        # Шаг 1: Получаем больше кандидатов (top-100)
        candidates = super().search_similar(query, top_k=min(100, top_k * 10))

        if not candidates:
            return []

        # Шаг 2: Вычисляем фичи для всех кандидатов
        features_list = []
        for news in candidates:
            features = self.calculate_features(query, news)
            # Сохраняем в том же порядке, что и при обучении
            features_list.append([features[col] for col in self.feature_columns])

        # Шаг 3: Предсказываем релевантность через LTR модель
        X = np.array(features_list)
        ltr_scores = self.ltr_model.predict(X)

        # Шаг 4: Сортируем по LTR-скору
        for i, news in enumerate(candidates):
            news['ltr_score'] = float(ltr_scores[i])

        candidates.sort(key=lambda x: x['ltr_score'], reverse=True)

        # Возвращаем топ-K
        return candidates[:top_k]


def test_ltr_search():
    """Тестирует поиск с LTR"""

    print("=" * 70)
    print("🧪 Тестирование LTR поиска")
    print("=" * 70)
    print()

    # Создаем систему с LTR
    rag_ltr = LTRNewsRAGSystem()

    # Тестовые запросы
    test_queries = [
        "Сбербанк",
        "ЦБ повысил ставку",
        "ипотека",
    ]

    for query in test_queries:
        print(f"\n🔍 Запрос: '{query}'")
        print("-" * 70)

        results = rag_ltr.search_similar(query, top_k=5)

        for i, news in enumerate(results, 1):
            print(f"\n{i}. {news['title'][:80]}...")
            print(f"   LTR Score: {news['ltr_score']:.4f}")
            print(f"   Embedding: {news.get('similarity', 0):.4f}")
            print(f"   Источник: {news.get('source', 'N/A')}")

    print("\n" + "=" * 70)
    print("✅ Тестирование завершено!")
    print("=" * 70)


if __name__ == "__main__":
    test_ltr_search()
