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
from typing import List, Optional
import uvicorn
import asyncio
from datetime import datetime
import json

from news_rag_system import NewsRAGSystem

app = FastAPI(title="News Collector API", version="1.0")

# Добавляем CORS middleware для веб-интерфейса
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешаем все источники (для локальной разработки)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Глобальный экземпляр RAG системы
rag = NewsRAGSystem()

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
    Поиск новостей (гибридный: текст + векторный + банковская приоритезация)
    """
    try:
        results = hybrid_search_internal(request.query, request.top_k)

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
                similarity=item['similarity'],
                keyword_score=item.get('keyword_score', 0),
                vector_score=item.get('vector_score', 0),
                bank_boost=item.get('bank_boost', 1.0),
                critical_keywords=item.get('critical_keywords', 0),
                geo_boost=item.get('geo_boost', 1.0),
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

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
