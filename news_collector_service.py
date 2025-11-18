#!/usr/bin/env python3
"""
News Collector Service (с интернетом)
- Загружает RSS с периодичностью
- Создает embeddings
- Предоставляет REST API для поиска новостей

Запуск: python3 news_collector_service.py
API: http://localhost:8001
"""

from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional, Dict
import uvicorn
import asyncio
from datetime import datetime, timedelta
import json
import sqlite3

from news_rag_system import NewsRAGSystem
import os

app = FastAPI(title="News Collector API", version="1.0")

# Добавляем CORS middleware для веб-интерфейса
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешаем все источники (для локальной разработки)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальный экземпляр RAG системы (с LTR если модель существует)
LTR_MODEL_PATH = "/Users/david/bank_news_agent/ltr_model.pkl"

if os.path.exists(LTR_MODEL_PATH):
    from integrate_ltr_model import LTRNewsRAGSystem
    rag = LTRNewsRAGSystem(ltr_model_path=LTR_MODEL_PATH)
    print("✓ Используется LTR-ранжирование")
else:
    rag = NewsRAGSystem()
    print("ℹ Используется стандартный поиск (LTR модель не найдена)")

# Конфигурация
UPDATE_INTERVAL_SECONDS = 3600  # 1 час

class SearchRequest(BaseModel):
    query: str
    top_k: int = 20
    category: Optional[str] = None

class EntityTag(BaseModel):
    text: str
    type: str
    is_banking: bool = False

class NewsItem(BaseModel):
    id: int
    title: str
    description: str
    link: str
    source: str
    published: str
    similarity: float
    keyword_score: Optional[float] = 0
    vector_score: Optional[float] = 0
    bank_boost: Optional[float] = 1.0
    critical_keywords: Optional[int] = 0
    geo_boost: Optional[float] = 1.0
    ltr_score: Optional[float] = None  # LTR-скор если модель доступна
    entities: Optional[List[EntityTag]] = []

class SearchResponse(BaseModel):
    query: str
    total_found: int
    news: List[NewsItem]
    timestamp: str

class StatsResponse(BaseModel):
    total_news: int
    by_source: dict
    by_category: dict
    last_update: str

# Загрузка банковских ключевых слов
def load_bank_keywords():
    try:
        with open("/Users/david/bank_news_agent/news_sources.json", 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('bank_keywords', {})
    except:
        return {"critical": [], "high": [], "exclude": []}

def calculate_banking_relevance(title: str, description: str) -> dict:
    """Вычислить релевантность новости для банка (ОТКЛЮЧЕНО)"""
    keywords = load_bank_keywords()
    text = f"{title} {description}".lower()

    score = {
        'critical_matches': 0,
        'high_matches': 0,
        'exclude_matches': 0,
        'boost': 1.0  # ВСЕ новости получают нейтральный буст
    }

    # Считаем для статистики, но не используем для буста
    for keyword in keywords.get('critical', []):
        if keyword.lower() in text:
            score['critical_matches'] += 1

    for keyword in keywords.get('high', []):
        if keyword.lower() in text:
            score['high_matches'] += 1

    for keyword in keywords.get('exclude', []):
        if keyword.lower() in text:
            score['exclude_matches'] += 1

    # ПРИОРИТЕЗАЦИЯ ОТКЛЮЧЕНА - все новости равны
    score['boost'] = 1.0

    return score

def expand_query(query: str) -> list:
    """Расширение запроса синонимами и вариантами (фразовое + словарное)"""

    # Фразовые синонимы (обрабатываются ПЕРЕД разбиением на слова)
    phrase_synonyms = {
        'ставка цб': ['ключевая ставка', 'ставка цб', 'ставка центробанка', 'ставка банка россии'],
        'ключевая ставка': ['ключевая ставка', 'ставка цб', 'ставка центробанка'],
        'курс рубля': ['курс рубля', 'курс доллара', 'рубль доллар', 'валютный курс'],
        'курс доллара': ['курс доллара', 'курс рубля', 'доллар рубль', 'валютный курс'],
    }

    # Проверяем фразы
    query_lower = query.lower()
    for phrase, variants in phrase_synonyms.items():
        if phrase in query_lower:
            # Если нашли фразу - возвращаем только её варианты
            return variants

    # Словарь синонимов для отдельных слов (если фразы не найдены)
    synonyms = {
        'лукойл': ['лукойл', 'lukoil'],
        'роснефть': ['роснефть', 'rosneft'],
        'газпром': ['газпром', 'gazprom'],
        'сбербанк': ['сбербанк', 'sberbank', 'сбер'],
        'втб': ['втб', 'vtb'],
        'санкции': ['санкции', 'ограничения'],
        'цб': ['цб', 'центробанк', 'банк россии'],
        'рубль': ['рубль', 'рубля', 'руб'],
        'доллар': ['доллар', 'usd'],
        'евро': ['евро', 'eur'],
    }

    words = query.lower().split()
    expanded = set(words)

    for word in words:
        for key, variants in synonyms.items():
            if word == key or key in word:
                expanded.update(variants)

    return list(expanded)

def calculate_recency_boost(published_date: str) -> float:
    """
    Вычислить буст на основе свежести новости

    Args:
        published_date: дата публикации (ISO format string)

    Returns:
        Коэффициент буста (1.0-1.3)
    """
    from datetime import datetime, timedelta
    from email.utils import parsedate_to_datetime

    try:
        # Парсим дату публикации
        if not published_date:
            return 1.0

        # Пробуем RFC 2822 формат (из RSS feeds)
        try:
            pub_date = parsedate_to_datetime(published_date)
        except:
            # Fallback на ISO формат
            pub_date = datetime.fromisoformat(published_date.replace('Z', '+00:00'))

        now = datetime.now(pub_date.tzinfo) if pub_date.tzinfo else datetime.now()

        # Вычисляем возраст новости
        age = now - pub_date
        age_hours = age.total_seconds() / 3600

        # Применяем буст в зависимости от возраста
        if age_hours < 24:
            # Последние 24 часа - максимальный буст
            return 1.3
        elif age_hours < 72:
            # Последние 3 дня - средний буст
            return 1.2
        elif age_hours < 168:
            # Последняя неделя - небольшой буст
            return 1.1
        elif age_hours < 720:
            # Последний месяц - минимальный буст
            return 1.05
        else:
            # Старше месяца - без буста
            return 1.0

    except Exception as e:
        # В случае ошибки парсинга - без буста
        return 1.0


def hybrid_search_internal(query: str, top_k: int = 20):
    """Гибридный поиск с улучшениями: позиционный вес, query expansion, NER-буст, recency boost"""
    import sqlite3
    import numpy as np
    from news_ner import NewsNERExtractor

    # Query expansion
    expanded_keywords = expand_query(query)

    stop_words = {
        # Предлоги и союзы
        'про', 'о', 'об', 'в', 'на', 'с', 'по', 'для', 'к', 'у', 'из', 'от', 'и', 'или', 'а', 'но', 'за', 'перед', 'между', 'под', 'над',
        # Команды
        'покажи', 'найди', 'дай', 'ищи', 'смотри',
    }
    keywords = [k for k in expanded_keywords if k not in stop_words and len(k) > 2]

    # Извлекаем NER-сущности из запроса
    ner_extractor = NewsNERExtractor()
    query_entities = ner_extractor.extract_from_news(query, "")

    # Создаем множество нормализованных NER-сущностей из запроса
    query_ner_normalized = set()
    for entity in query_entities['all']:
        normalized = entity.get('normalized', entity['text'])
        query_ner_normalized.add(normalized.lower())

    conn = sqlite3.connect(rag.db_path)
    cursor = conn.cursor()

    # Текстовый поиск с позиционным весом (без LOWER для поддержки русских букв)
    keyword_results = {}

    import re
    import pymorphy2

    morph = pymorphy2.MorphAnalyzer()

    def word_in_text(word: str, text: str) -> bool:
        """Проверить что слово есть в тексте как целое слово (не подстрока)"""
        # Регулярное выражение: границы слов вокруг ключевого слова
        pattern = r'\b' + re.escape(word.lower()) + r'\b'
        return bool(re.search(pattern, text.lower(), re.UNICODE))

    def get_word_forms(word: str) -> set:
        """Получить все формы слова (Путин, Путина, Путину, etc.)"""
        forms = {word.lower()}  # Базовая форма

        # Парсим слово и получаем все его формы
        parsed = morph.parse(word)
        if parsed:
            lexeme = parsed[0].lexeme  # Все формы слова
            for form in lexeme:
                forms.add(form.word.lower())

        return forms

    for keyword in keywords:
        # Получаем все морфологические формы слова заранее
        word_forms = get_word_forms(keyword)

        # Создаем SQL запрос со всеми формами слова
        # Для каждой формы: lowercase и capitalized
        like_conditions = []
        like_params = []

        for word_form in word_forms:
            form_lower = word_form.lower()
            form_cap = word_form.capitalize()
            form_upper = word_form.upper()
            like_conditions.append("title LIKE ? OR title LIKE ? OR title LIKE ?")
            like_conditions.append("description LIKE ? OR description LIKE ? OR description LIKE ?")
            like_conditions.append("full_text LIKE ? OR full_text LIKE ? OR full_text LIKE ?")
            like_params.extend([f'%{form_lower}%', f'%{form_cap}%', f'%{form_upper}%'] * 3)

        sql_query = f'''
            SELECT id, title, description, link, source, published, embedding, full_text
            FROM news
            WHERE {' OR '.join(like_conditions)}
        '''

        cursor.execute(sql_query, like_params)
        rows = cursor.fetchall()

        for row in rows:
            news_id = row[0]
            title = row[1] or ''
            description = row[2] or ''
            full_text = row[7] or ''

            # Проверяем что хотя бы одна форма слова есть как целое слово
            found = False
            for word_form in word_forms:
                if (word_in_text(word_form, title) or
                    word_in_text(word_form, description) or
                    word_in_text(word_form, full_text)):
                    found = True
                    break

            if not found:
                continue  # Пропускаем если ни одна форма слова не найдена

            if news_id not in keyword_results:
                keyword_results[news_id] = {
                    'id': news_id,
                    'title': title,
                    'description': description,
                    'link': row[3],
                    'source': row[4],
                    'published': row[5],
                    'embedding': np.frombuffer(row[6], dtype=np.float32),
                    'keyword_score': 0
                }

            # ПОЗИЦИОННЫЙ ВЕС с NER-бустом
            position_weight = 0

            # Фразы (2+ слова) получают повышенный вес
            is_phrase = ' ' in keyword
            title_weight = 10.0 if is_phrase else 5.0
            desc_weight = 3.0 if is_phrase else 1.5
            text_weight = 2.0 if is_phrase else 1.0

            # NER-буст: если ключевое слово является NER-сущностью из запроса
            is_ner_entity = keyword.lower() in query_ner_normalized
            ner_multiplier = 5.0 if is_ner_entity else 1.0

            # Проверяем наличие любой морфологической формы слова
            found_in_title = any(word_in_text(form, title) for form in word_forms)
            found_in_description = any(word_in_text(form, description) for form in word_forms)
            found_in_full_text = any(word_in_text(form, full_text) for form in word_forms)

            if found_in_title:
                position_weight += title_weight * ner_multiplier  # NER в заголовке: x5
            if found_in_description:
                position_weight += desc_weight * ner_multiplier  # NER в описании: x5
            if found_in_full_text:
                position_weight += text_weight * ner_multiplier  # NER в тексте: x5

            keyword_results[news_id]['keyword_score'] += position_weight

    # Буст за множественные совпадения в заголовке
    for news_id, data in keyword_results.items():
        title = data['title']
        # Считаем сколько ключевых слов из запроса есть в заголовке (с учетом морф. форм)
        matched_in_title = 0
        for kw in keywords:
            kw_forms = get_word_forms(kw)
            if any(word_in_text(form, title) for form in kw_forms):
                matched_in_title += 1

        if matched_in_title >= 2:
            # 2 слова -> x1.3, 3 слова -> x1.5, 4+ слов -> x1.7
            multi_match_boost = 1.0 + (matched_in_title - 1) * 0.3
            data['keyword_score'] *= multi_match_boost

    # Дополнительный NER-буст: проверяем совпадение с реальными NER-сущностями из БД
    if query_ner_normalized:
        for news_id in keyword_results.keys():
            cursor.execute('''
                SELECT COUNT(DISTINCT normalized_text)
                FROM entities
                WHERE news_id = ? AND LOWER(normalized_text) IN ({})
            '''.format(','.join('?' * len(query_ner_normalized))),
            [news_id] + list(query_ner_normalized))

            ner_matches = cursor.fetchone()[0]
            if ner_matches > 0:
                # Каждая совпавшая NER-сущность дает дополнительный буст x1.4
                ner_match_boost = 1.0 + (ner_matches * 0.4)
                keyword_results[news_id]['keyword_score'] *= ner_match_boost
                keyword_results[news_id]['ner_matches'] = ner_matches

    # DEBUG: print(f"DEBUG: Total keyword_results: {len(keyword_results)}")

    # Векторный поиск
    query_embedding = rag.get_embedding(query)
    print(f"DEBUG: query_embedding shape: {query_embedding.shape if query_embedding is not None else None}")
    vector_results = {}

    if query_embedding is not None:
        cursor.execute('SELECT id, title, description, link, source, published, embedding FROM news')

        for row in cursor.fetchall():
            news_id, title, description, link, source, published, embedding_blob = row
            news_embedding = np.frombuffer(embedding_blob, dtype=np.float32)

            similarity = np.dot(query_embedding, news_embedding) / (
                np.linalg.norm(query_embedding) * np.linalg.norm(news_embedding)
            )

            vector_results[news_id] = {
                'id': news_id,
                'title': title,
                'description': description,
                'link': link,
                'source': source,
                'published': published,
                'vector_score': float(similarity)
            }

    conn.close()

    # Объединение с банковской релевантностью
    combined_results = {}
    max_keyword_score = max([r['keyword_score'] for r in keyword_results.values()]) if keyword_results else 1
    # DEBUG: print(f"DEBUG: max_keyword_score={max_keyword_score}")

    for news_id, data in keyword_results.items():
        bank_relevance = calculate_banking_relevance(data['title'], data['description'])

        combined_results[news_id] = {
            'id': data['id'],
            'title': data['title'],
            'description': data['description'],
            'link': data['link'],
            'source': data['source'],
            'published': data['published'],
            'keyword_score': data['keyword_score'] / max_keyword_score if max_keyword_score > 0 else 0,
            'vector_score': vector_results.get(news_id, {}).get('vector_score', 0),
            'bank_boost': bank_relevance['boost'],
            'critical_keywords': bank_relevance['critical_matches'],
            'is_excluded': bank_relevance['exclude_matches'] > 0
        }

    # DEBUG: After keyword_results logging disabled

    # НЕ добавляем vector-only results - они разбавляют релевантную выдачу
    # for news_id, data in vector_results.items():
    #     if news_id not in combined_results:
    #         bank_relevance = calculate_banking_relevance(data['title'], data['description'])
    #
    #         combined_results[news_id] = {
    #             'id': data['id'],
    #             'title': data['title'],
    #             'description': data['description'],
    #             'link': data['link'],
    #             'source': data['source'],
    #             'published': data['published'],
    #             'keyword_score': 0,
    #             'vector_score': data['vector_score'],
    #             'bank_boost': bank_relevance['boost'],
    #             'critical_keywords': bank_relevance['critical_matches'],
    #             'is_excluded': bank_relevance['exclude_matches'] > 0
    #         }

    # Финальный score с приоритетом свежих новостей
    for data in combined_results.values():
        if data['keyword_score'] > 0:
            # Keyword match получает больший вес (85%)
            base_score = 0.85 * data['keyword_score'] + 0.15 * data['vector_score']
        else:
            # Только vector search
            base_score = data['vector_score']

        # Применяем recency boost - свежие новости получают приоритет
        recency_boost = calculate_recency_boost(data.get('published', ''))
        final_score = base_score * recency_boost

        data['similarity'] = final_score
        data['recency_boost'] = recency_boost
        data['geo_boost'] = 1.0
        data['war_penalty'] = 1.0

    results = sorted(combined_results.values(), key=lambda x: x['similarity'], reverse=True)

    # DEBUG: Top 10 results logging disabled

    # ФИЛЬТРАЦИЯ ОТКЛЮЧЕНА - показываем все новости
    return results[:top_k]

# Background task для периодического обновления
async def periodic_update():
    """Периодическое обновление новостей (асинхронная версия)"""
    # Ждем 5 минут перед первым обновлением, чтобы не блокировать сервис при старте
    await asyncio.sleep(300)  # 5 минут

    while True:
        try:
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] 🔄 Начинаю обновление новостей...")
            # Используем асинхронную версию - не блокирует API!
            # Загружаем все новости из RSS, дедупликация отфильтрует существующие
            new_count = await rag.fetch_and_index_news_async(limit_per_source=50, max_concurrent=5)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ Добавлено {new_count} новых новостей")
        except Exception as e:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ Ошибка обновления: {e}")

        await asyncio.sleep(UPDATE_INTERVAL_SECONDS)

@app.on_event("startup")
async def startup_event():
    """Запуск фонового обновления"""
    asyncio.create_task(periodic_update())
    print("=" * 70)
    print("🚀 News Collector Service запущен")
    print(f"📡 API: http://localhost:8001")
    print(f"📖 Docs: http://localhost:8001/docs")
    print(f"🔄 Автообновление каждые {UPDATE_INTERVAL_SECONDS // 60} минут (первое через 5 мин)")
    print(f"📰 Источников: {len(rag.sources) if hasattr(rag, 'sources') else '255'} (вкл. GDELT)")
    print("=" * 70)

@app.get("/")
async def root():
    return {
        "service": "News Collector API",
        "version": "1.0",
        "status": "running",
        "docs": "/docs"
    }

@app.get("/health")
async def health():
    """Проверка здоровья сервиса"""
    stats = rag.get_stats()
    return {
        "status": "healthy",
        "total_news": stats['total'],
        "timestamp": datetime.now().isoformat()
    }

@app.get("/stats", response_model=StatsResponse)
async def get_stats():
    """Получить статистику базы"""
    stats = rag.get_stats()
    return StatsResponse(
        total_news=stats['total'],
        by_source=stats['by_source'],
        by_category=stats['by_category'],
        last_update=datetime.now().isoformat()
    )

def get_news_entities_tags(news_id: int) -> List[EntityTag]:
    """Получить NER-теги для новости"""
    import sqlite3
    try:
        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT entity_text, entity_type, is_banking
            FROM entities
            WHERE news_id = ?
            ORDER BY position
        ''', (news_id,))

        entities = []
        for row in cursor.fetchall():
            entities.append(EntityTag(
                text=row[0],
                type=row[1],
                is_banking=bool(row[2])
            ))

        conn.close()
        return entities
    except Exception as e:
        print(f"Error loading entities for news {news_id}: {e}")
        return []

@app.post("/search", response_model=SearchResponse)
async def search_news(request: SearchRequest):
    """
    Поиск новостей (с LTR-переранжированием если доступно)
    """
    try:
        # Используем rag.search_similar который автоматически применяет LTR если модель загружена
        results = rag.search_similar(request.query, top_k=request.top_k)

        news_items = []
        for item in results:
            # Загружаем NER-сущности для каждой новости
            entities = get_news_entities_tags(item['id'])

            news_items.append(NewsItem(
                id=item['id'],
                title=item['title'],
                description=item['description'],
                link=item['link'],
                source=item['source'],
                published=item['published'],
                similarity=item.get('similarity', 0),
                keyword_score=item.get('keyword_score', 0),
                vector_score=item.get('vector_score', 0),
                bank_boost=item.get('bank_boost', 1.0),
                critical_keywords=item.get('critical_keywords', 0),
                geo_boost=item.get('geo_boost', 1.0),
                ltr_score=item.get('ltr_score', None),  # LTR-скор если есть
                entities=entities
            ))

        return SearchResponse(
            query=request.query,
            total_found=len(news_items),
            news=news_items,
            timestamp=datetime.now().isoformat()
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search error: {str(e)}")

@app.get("/api/latest")
async def get_latest_news(limit: int = 20):
    """
    Получить последние новости (отсортированные по дате публикации)
    """
    try:
        import sqlite3
        from email.utils import parsedate_to_datetime
        from datetime import datetime as dt

        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        # Получаем новости за последние 3 года (чтобы отсортировать по дате)
        cursor.execute('''
            SELECT id, title, description, link, source, published
            FROM news
            WHERE published LIKE '%2025%'
               OR published LIKE '%2024%'
               OR published LIKE '%2023%'
        ''')

        rows = cursor.fetchall()
        conn.close()

        # Парсим даты и сортируем
        news_with_dates = []
        for row in rows:
            news_id, title, description, link, source, published = row

            try:
                # Пробуем разные форматы дат
                try:
                    # RFC 2822 формат: "Tue, 12 Mar 2019 06:06:29 GMT"
                    parsed_date = parsedate_to_datetime(published)
                except:
                    # Альтернативный формат: "14 Nov 2025 04:45:00 +0000"
                    try:
                        parsed_date = dt.strptime(published, "%d %b %Y %H:%M:%S %z")
                    except:
                        # Если не удалось распарсить, используем текущую дату минус ID
                        parsed_date = dt(2000, 1, 1)

                news_with_dates.append({
                    'id': news_id,
                    'title': title,
                    'description': description,
                    'link': link,
                    'source': source,
                    'published': published,
                    'parsed_date': parsed_date
                })
            except Exception as e:
                continue

        # Сортируем по дате (от новых к старым)
        news_with_dates.sort(key=lambda x: x['parsed_date'], reverse=True)

        # Берем топ-N
        top_news = news_with_dates[:limit]

        news_items = []
        for news_data in top_news:
            # Загружаем NER-сущности для каждой новости
            entities = get_news_entities_tags(news_data['id'])

            news_items.append(NewsItem(
                id=news_data['id'],
                title=news_data['title'],
                description=news_data['description'],
                link=news_data['link'],
                source=news_data['source'],
                published=news_data['published'],
                similarity=0.0,
                keyword_score=0.0,
                vector_score=0.0,
                bank_boost=1.0,
                critical_keywords=0,
                geo_boost=1.0,
                entities=entities
            ))

        return SearchResponse(
            query="",
            total_found=len(news_items),
            news=news_items,
            timestamp=datetime.now().isoformat()
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching latest news: {str(e)}")

@app.post("/update")
async def trigger_update(background_tasks: BackgroundTasks):
    """Запустить обновление новостей вручную (асинхронная версия)"""
    async def update_news():
        try:
            # Используем асинхронную версию - не блокирует API!
            # Загружаем все новости из RSS, дедупликация отфильтрует существующие
            new_count = await rag.fetch_and_index_news_async(limit_per_source=50, max_concurrent=5)
            print(f"✅ Обновление завершено: {new_count} новых новостей")
        except Exception as e:
            print(f"❌ Ошибка обновления: {e}")

    background_tasks.add_task(update_news)
    return {"status": "update_started", "message": "Обновление запущено в фоне (async)"}

@app.get("/entities/search/{entity_text}")
async def search_by_entity(entity_text: str, limit: int = 20):
    """
    Найти новости, содержащие указанную NER-сущность
    """
    try:
        import sqlite3
        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT DISTINCT n.id, n.title, n.description, n.link, n.source, n.published
            FROM news n
            INNER JOIN entities e ON n.id = e.news_id
            WHERE e.entity_text LIKE ?
            ORDER BY n.published DESC
            LIMIT ?
        ''', (f'%{entity_text}%', limit))

        news_items = []
        for row in cursor.fetchall():
            news_items.append({
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'link': row[3],
                'source': row[4],
                'published': row[5]
            })

        conn.close()

        return {
            'entity': entity_text,
            'total_found': len(news_items),
            'news': news_items
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error searching by entity: {str(e)}")

@app.get("/entities/stats")
async def get_entities_stats():
    """
    Получить статистику по NER-сущностям
    """
    try:
        import sqlite3
        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        # Топ персон (группируем по нормализованной форме)
        cursor.execute('''
            SELECT normalized_text, COUNT(DISTINCT news_id) as count
            FROM entities
            WHERE entity_type = 'person' AND normalized_text IS NOT NULL
            GROUP BY normalized_text
            ORDER BY count DESC
            LIMIT 10
        ''')
        top_persons = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall()]

        # Топ организаций (исключаем источники СМИ, группируем по нормализованной форме)
        cursor.execute('''
            SELECT e.normalized_text, COUNT(DISTINCT e.news_id) as count
            FROM entities e
            INNER JOIN news n ON e.news_id = n.id
            WHERE e.entity_type = 'organization'
            AND e.normalized_text IS NOT NULL
            AND LOWER(e.entity_text) NOT LIKE '%' || LOWER(n.source) || '%'
            AND LOWER(n.source) NOT LIKE '%' || LOWER(e.entity_text) || '%'
            GROUP BY e.normalized_text
            ORDER BY count DESC
            LIMIT 10
        ''')
        top_organizations = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall()]

        # Топ локаций (группируем по нормализованной форме)
        cursor.execute('''
            SELECT normalized_text, COUNT(DISTINCT news_id) as count
            FROM entities
            WHERE entity_type = 'location' AND normalized_text IS NOT NULL
            GROUP BY normalized_text
            ORDER BY count DESC
            LIMIT 10
        ''')
        top_locations = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall()]

        # Банковские сущности (исключаем источники СМИ, группируем по нормализованной форме)
        cursor.execute('''
            SELECT e.normalized_text, COUNT(DISTINCT e.news_id) as count
            FROM entities e
            INNER JOIN news n ON e.news_id = n.id
            WHERE e.is_banking = 1
            AND e.normalized_text IS NOT NULL
            AND LOWER(e.entity_text) NOT LIKE '%' || LOWER(n.source) || '%'
            AND LOWER(n.source) NOT LIKE '%' || LOWER(e.entity_text) || '%'
            GROUP BY e.normalized_text
            ORDER BY count DESC
            LIMIT 10
        ''')
        banking_entities = [{'name': row[0], 'count': row[1]} for row in cursor.fetchall()]

        # Общая статистика
        cursor.execute('SELECT COUNT(DISTINCT entity_text) FROM entities')
        total_unique_entities = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM entities')
        total_entity_mentions = cursor.fetchone()[0]

        conn.close()

        return {
            'total_unique_entities': total_unique_entities,
            'total_mentions': total_entity_mentions,
            'top_persons': top_persons,
            'top_organizations': top_organizations,
            'top_locations': top_locations,
            'banking_entities': banking_entities
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entity stats: {str(e)}")

@app.get("/entities/id/{news_id}")
async def get_news_entities(news_id: int):
    """
    Получить все NER-сущности для конкретной новости
    """
    try:
        import sqlite3
        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        cursor.execute('''
            SELECT entity_text, entity_type, is_banking
            FROM entities
            WHERE news_id = ?
            ORDER BY position
        ''', (news_id,))

        entities = []
        for row in cursor.fetchall():
            entities.append({
                'text': row[0],
                'type': row[1],
                'is_banking': bool(row[2])
            })

        conn.close()

        return {
            'news_id': news_id,
            'entities': entities,
            'total': len(entities)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entities: {str(e)}")

@app.get("/entities/trends")
async def get_entity_trends(days: int = 30, entity_type: Optional[str] = None, top_n: int = 10):
    """
    Получить тренды упоминания NER-сущностей за последние N дней

    Args:
        days: количество дней для анализа (по умолчанию 30)
        entity_type: фильтр по типу сущности (person/organization/location)
        top_n: количество топ сущностей для отображения
    """
    try:
        import sqlite3
        from datetime import datetime, timedelta
        from email.utils import parsedate_to_datetime
        from collections import defaultdict

        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        # Вычисляем дату отсечки (делаем timezone-aware)
        from datetime import timezone
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)

        # Получаем все новости с сущностями (исключаем источники новостей, группируем по нормализованной форме)
        type_filter = f"AND e.entity_type = '{entity_type}'" if entity_type else ""

        cursor.execute(f'''
            SELECT e.normalized_text, n.published, e.news_id, n.source
            FROM entities e
            INNER JOIN news n ON e.news_id = n.id
            WHERE e.normalized_text IS NOT NULL
            {type_filter}
        ''')

        # Парсим даты и фильтруем
        entity_mentions = defaultdict(lambda: defaultdict(set))

        for row in cursor.fetchall():
            normalized_text, published, news_id, source = row

            # Пропускаем если сущность совпадает с источником (это название СМИ, а не упоминание)
            if normalized_text.lower() in source.lower() or source.lower() in normalized_text.lower():
                continue

            try:
                # Парсим дату из RFC 2822 формата
                pub_date = parsedate_to_datetime(published)

                # Фильтруем по дате
                if pub_date >= cutoff_date:
                    date_str = pub_date.strftime('%Y-%m-%d')
                    entity_mentions[normalized_text][date_str].add(news_id)
            except:
                continue

        # Получаем топ сущностей по общему количеству упоминаний
        entity_totals = {}
        for entity, dates_dict in entity_mentions.items():
            total = sum(len(news_ids) for news_ids in dates_dict.values())
            entity_totals[entity] = total

        # Сортируем и берем топ N
        top_entities = sorted(entity_totals.items(), key=lambda x: x[1], reverse=True)[:top_n]
        top_entity_names = [entity for entity, _ in top_entities]

        # Формируем список всех дат в диапазоне
        dates = []
        current_date = cutoff_date.date()
        end_date = datetime.now().date()

        while current_date <= end_date:
            dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)

        # Формируем данные для графика
        datasets = []
        for entity in top_entity_names:
            data = [len(entity_mentions[entity].get(date, set())) for date in dates]
            datasets.append({
                'label': entity,
                'data': data
            })

        conn.close()

        return {
            'dates': dates,
            'datasets': datasets,
            'period_days': days,
            'entity_type': entity_type or 'all',
            'top_n': top_n
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entity trends: {str(e)}")

@app.get("/api/trends/daily")
async def get_daily_trends(top_n: int = 20):
    """
    Получить топ-N трендов по изменению упоминаний (вчера vs сегодня)
    Возвращает сущности с наибольшим изменением в абсолютном значении
    """
    try:
        import sqlite3
        from datetime import datetime, timedelta
        from email.utils import parsedate_to_datetime

        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        # Определяем сегодня и вчера
        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        yesterday_start = today_start - timedelta(days=1)

        # Получаем упоминания сущностей за сегодня
        cursor.execute('''
            SELECT e.normalized_text, COUNT(DISTINCT e.news_id) as count
            FROM entities e
            INNER JOIN news n ON e.news_id = n.id
            WHERE e.normalized_text IS NOT NULL
            GROUP BY e.normalized_text
        ''')

        all_entities = {}
        for row in cursor.fetchall():
            entity_name, count = row
            all_entities[entity_name] = {'today': 0, 'yesterday': 0}

        # Считаем упоминания по дням
        cursor.execute('''
            SELECT e.normalized_text, n.published
            FROM entities e
            INNER JOIN news n ON e.news_id = n.id
            WHERE e.normalized_text IS NOT NULL
        ''')

        for row in cursor.fetchall():
            entity_name, published = row

            if entity_name not in all_entities:
                all_entities[entity_name] = {'today': 0, 'yesterday': 0}

            try:
                # Парсим дату
                try:
                    pub_date = parsedate_to_datetime(published)
                except:
                    # Альтернативный формат
                    pub_date = datetime.strptime(published, "%d %b %Y %H:%M:%S %z")

                # Определяем день (сравниваем только даты, игнорируя время и timezone)
                pub_date_only = pub_date.date() if hasattr(pub_date, 'date') else pub_date
                today_only = today_start.date()
                yesterday_only = yesterday_start.date()

                if pub_date_only == today_only:
                    all_entities[entity_name]['today'] += 1
                elif pub_date_only == yesterday_only:
                    all_entities[entity_name]['yesterday'] += 1
            except:
                continue

        conn.close()

        # Вычисляем изменения
        trends = []
        for entity_name, counts in all_entities.items():
            today_count = counts['today']
            yesterday_count = counts['yesterday']

            # Пропускаем если нет упоминаний ни вчера, ни сегодня
            if today_count == 0 and yesterday_count == 0:
                continue

            change = today_count - yesterday_count

            trends.append({
                'entity': entity_name,
                'today': today_count,
                'yesterday': yesterday_count,
                'change': change,
                'change_abs': abs(change)
            })

        # Сортируем по абсолютному изменению (наибольшие изменения вверху)
        trends.sort(key=lambda x: x['change_abs'], reverse=True)

        # Топ-N
        top_trends = trends[:top_n]

        return {
            'trends': top_trends,
            'total_entities': len(all_entities),
            'timestamp': datetime.now().isoformat()
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching daily trends: {str(e)}")

@app.get("/api/entity/{entity_name}/timeline")
async def get_entity_timeline(entity_name: str, days: int = 30):
    """
    Получить временной ряд упоминаний сущности по дням
    """
    try:
        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        # Получаем все упоминания сущности с датами
        cursor.execute('''
            SELECT n.published
            FROM news n
            JOIN entities e ON n.id = e.news_id
            WHERE LOWER(e.entity_text) = LOWER(?)
            ORDER BY n.published DESC
        ''', (entity_name,))

        # Подсчитываем упоминания по дням
        daily_counts = {}
        all_dates = []

        for row in cursor.fetchall():
            pub_str = row[0]
            if pub_str:
                try:
                    # Парсим разные форматы дат
                    if pub_str.startswith(('Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun')):
                        # RFC 2822 формат (из RSS): "Wed, 30 Apr 2025 07:00:00 GMT"
                        from email.utils import parsedate_to_datetime
                        pub_date = parsedate_to_datetime(pub_str)
                    else:
                        pub_date = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))

                    date_key = pub_date.date().isoformat()
                    daily_counts[date_key] = daily_counts.get(date_key, 0) + 1
                    all_dates.append(pub_date.date())
                except Exception as e:
                    # Логируем ошибки парсинга только для отладки
                    # print(f"Could not parse date '{pub_str}': {e}")
                    continue

        conn.close()

        # Если нет данных, возвращаем пустой график за последние N дней от сегодня
        if not all_dates:
            today = datetime.now().date()
            timeline = []
            for i in range(days - 1, -1, -1):
                date = today - timedelta(days=i)
                timeline.append({
                    'date': date.isoformat(),
                    'count': 0
                })
        else:
            # Строим график от последней даты упоминания назад на N дней
            latest_date = max(all_dates)
            timeline = []

            for i in range(days - 1, -1, -1):
                date = latest_date - timedelta(days=i)
                date_str = date.isoformat()
                count = daily_counts.get(date_str, 0)
                timeline.append({
                    'date': date_str,
                    'count': count
                })

        return {
            'entity': entity_name,
            'timeline': timeline,
            'total_mentions': sum(daily_counts.values())
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entity timeline: {str(e)}")

@app.get("/api/entity/{entity_name}/news")
async def get_entity_news(entity_name: str, limit: int = 20):
    """
    Получить новости с упоминанием данной сущности
    """
    try:
        conn = sqlite3.connect(rag.db_path)
        cursor = conn.cursor()

        # Получаем новости с этой сущностью
        cursor.execute('''
            SELECT DISTINCT n.id, n.title, n.description, n.link, n.source, n.published
            FROM news n
            JOIN entities e ON n.id = e.news_id
            WHERE LOWER(e.entity_text) = LOWER(?)
            ORDER BY n.published DESC
            LIMIT ?
        ''', (entity_name, limit))

        news_list = []
        for row in cursor.fetchall():
            news_list.append({
                'id': row[0],
                'title': row[1],
                'description': row[2],
                'url': row[3],
                'source': row[4],
                'published': row[5]
            })

        conn.close()

        return {
            'entity': entity_name,
            'news': news_list,
            'total': len(news_list)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching entity news: {str(e)}")

@app.post("/api/ltr/generate_candidates")
async def generate_ltr_candidates(request: SearchRequest):
    """
    Генерирует кандидатов с фичами для LTR-разметки по запросу
    """
    try:
        from ltr_dataset_generator import LTRDatasetGenerator

        generator = LTRDatasetGenerator()
        top_k = min(request.top_k, 20)  # Максимум 20 кандидатов

        candidates = generator.generate_candidates(request.query, top_k=top_k)

        return {
            'query': request.query,
            'candidates': candidates,
            'total': len(candidates)
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error generating LTR candidates: {str(e)}")


class RetrainRequest(BaseModel):
    """Запрос на переобучение модели с размеченным датасетом"""
    dataset: List[Dict]  # Список размеченных пар {query, news_id, features, label}


@app.post("/api/ltr/retrain")
async def retrain_ltr_model(request: RetrainRequest):
    """
    Переобучает LTR модель на размеченном датасете
    Возвращает старые и новые feature importances
    """
    try:
        import pandas as pd
        import numpy as np
        from sklearn.model_selection import train_test_split
        from lightgbm import LGBMRanker
        import pickle
        from datetime import datetime

        # 1. Получаем старые feature importances
        global rag
        old_importances = {}
        if hasattr(rag, 'ltr_model') and rag.ltr_model:
            feature_names = rag.feature_columns
            importances = rag.ltr_model.feature_importances_
            old_importances = {name: float(imp) for name, imp in zip(feature_names, importances)}

        # 2. Фильтруем только размеченные данные (label не null)
        labeled_data = [item for item in request.dataset if item.get('label') is not None]

        if len(labeled_data) < 10:
            raise HTTPException(status_code=400, detail=f"Недостаточно размеченных данных: {len(labeled_data)}. Нужно минимум 10.")

        # 3. Подготавливаем данные для обучения
        df = pd.DataFrame(labeled_data)

        # Извлекаем фичи из словаря features
        feature_columns = list(labeled_data[0]['features'].keys())
        X = np.array([list(item['features'].values()) for item in labeled_data])
        y = df['label'].values

        # Группы запросов (для LTR важно!)
        queries = df['query'].values
        query_groups = []
        current_query = None
        current_count = 0

        for q in queries:
            if q != current_query:
                if current_count > 0:
                    query_groups.append(current_count)
                current_query = q
                current_count = 1
            else:
                current_count += 1
        query_groups.append(current_count)

        # 4. Train/val split (80/20)
        n_queries = len(query_groups)
        split_idx = int(0.8 * n_queries)

        train_size = sum(query_groups[:split_idx])

        X_train = X[:train_size]
        y_train = y[:train_size]
        train_groups = query_groups[:split_idx]

        X_val = X[train_size:]
        y_val = y[train_size:]
        val_groups = query_groups[split_idx:]

        # 5. Обучаем новую модель
        model = LGBMRanker(
            objective='lambdarank',
            metric='ndcg',
            n_estimators=100,
            learning_rate=0.05,
            num_leaves=31,
            max_depth=6,
            min_child_samples=5,
            random_state=42,
            n_jobs=-1
        )

        model.fit(
            X_train, y_train,
            group=train_groups,
            eval_set=[(X_val, y_val)],
            eval_group=[val_groups],
            eval_metric='ndcg',
            callbacks=[],
        )

        # 6. Получаем новые feature importances
        new_importances = {name: float(imp) for name, imp in zip(feature_columns, model.feature_importances_)}

        # 7. Вычисляем изменения
        changes = {}
        for feature in feature_columns:
            old_val = old_importances.get(feature, 0)
            new_val = new_importances.get(feature, 0)
            changes[feature] = {
                'old': old_val,
                'new': new_val,
                'delta': new_val - old_val,
                'delta_percent': ((new_val - old_val) / old_val * 100) if old_val > 0 else 0
            }

        # 8. Сохраняем модель
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Backup старой модели
        import shutil
        if os.path.exists('ltr_model.pkl'):
            shutil.copy('ltr_model.pkl', f'ltr_model_backup_{timestamp}.pkl')

        # Сохраняем новую модель
        model_data = {
            'model': model,
            'feature_columns': feature_columns,
            'trained_on': timestamp,
            'n_samples': len(labeled_data),
            'n_queries': len(set(df['query']))
        }

        with open('ltr_model.pkl', 'wb') as f:
            pickle.dump(model_data, f)

        # 9. Перезагружаем модель в памяти
        rag.ltr_model = model
        rag.feature_columns = feature_columns

        # 10. Вычисляем метрики на валидации
        from sklearn.metrics import ndcg_score

        # Предсказания на валидации
        y_pred_val = model.predict(X_val)

        # NDCG по группам
        ndcg_scores = []
        start_idx = 0
        for group_size in val_groups:
            end_idx = start_idx + group_size
            y_true_group = y_val[start_idx:end_idx].reshape(1, -1)
            y_pred_group = y_pred_val[start_idx:end_idx].reshape(1, -1)

            if len(y_true_group[0]) > 0:
                ndcg = ndcg_score(y_true_group, y_pred_group)
                ndcg_scores.append(ndcg)

            start_idx = end_idx

        avg_ndcg = float(np.mean(ndcg_scores)) if ndcg_scores else 0.0

        return {
            'success': True,
            'message': f'Модель переобучена на {len(labeled_data)} примерах ({len(set(df["query"]))} запросов)',
            'old_importances': old_importances,
            'new_importances': new_importances,
            'changes': changes,
            'metrics': {
                'val_ndcg': avg_ndcg,
                'n_train_samples': len(X_train),
                'n_val_samples': len(X_val),
                'n_queries_total': len(set(df['query'])),
            },
            'backup_file': f'ltr_model_backup_{timestamp}.pkl'
        }

    except Exception as e:
        import traceback
        raise HTTPException(status_code=500, detail=f"Error retraining model: {str(e)}\n{traceback.format_exc()}")


if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
