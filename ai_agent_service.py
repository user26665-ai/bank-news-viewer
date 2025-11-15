#!/usr/bin/env python3
"""
AI Agent Service (БЕЗ интернета)
- Получает новости через API первого сервиса
- Работает с локальным LLM (Qwen)
- Генерирует ответы

Запуск: python3 ai_agent_service.py
API: http://localhost:8002
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import uvicorn
import requests
from datetime import datetime

app = FastAPI(title="AI Agent API", version="1.0")

# Добавляем CORS middleware для веб-интерфейса
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Разрешаем все источники (для локальной разработки)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Конфигурация
NEWS_COLLECTOR_URL = "http://localhost:8001"
LM_STUDIO_API_URL = "http://localhost:1234/v1/chat/completions"

class QueryRequest(BaseModel):
    question: str
    top_k: int = 15

class NewsItem(BaseModel):
    title: str
    description: str
    link: str
    source: str
    published: str
    similarity: float
    critical_keywords: Optional[int] = 0

class AgentResponse(BaseModel):
    question: str
    answer: str
    news_found: int
    top_news: List[NewsItem]
    timestamp: str

def get_news_from_collector(query: str, top_k: int = 15) -> dict:
    """Получить новости от News Collector сервиса"""
    try:
        response = requests.post(
            f"{NEWS_COLLECTOR_URL}/search",
            json={"query": query, "top_k": top_k},
            timeout=10
        )
        response.raise_for_status()
        return response.json()
    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="News Collector Service недоступен. Запустите: python3 news_collector_service.py"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка получения новостей: {str(e)}")

def hide_reasoning_under_spoiler(text: str) -> str:
    """
    Обернуть рассуждения модели в markdown спойлер

    Ищет паттерны рассуждений и оборачивает в <details>
    """
    import re

    # Паттерн 1: Теги <think> или <reasoning>
    if '<think>' in text.lower() or '<reasoning>' in text.lower():
        text = re.sub(
            r'<think>(.*?)</think>',
            r'<details>\n<summary>💭 Рассуждения модели</summary>\n\n\1\n</details>\n\n',
            text,
            flags=re.DOTALL | re.IGNORECASE
        )
        text = re.sub(
            r'<reasoning>(.*?)</reasoning>',
            r'<details>\n<summary>💭 Рассуждения модели</summary>\n\n\1\n</details>\n\n',
            text,
            flags=re.DOTALL | re.IGNORECASE
        )

    # Паттерн 2: Всё до "Ответ:", "Итого:", "Вывод:"
    reasoning_patterns = [
        (r'(.*?)(Ответ:|Итого:|Вывод:|Заключение:)', 2),
        (r'(.*?)(На основании|Исходя из)', 2),
    ]

    for pattern, min_group in reasoning_patterns:
        match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
        if match:
            reasoning = match.group(1).strip()
            # Проверяем что это действительно рассуждения (> 80 символов)
            if len(reasoning) > 80:
                rest_of_text = text[len(match.group(1)):].strip()
                text = f"<details>\n<summary>💭 Рассуждения модели</summary>\n\n{reasoning}\n</details>\n\n{rest_of_text}"
                break

    return text

def query_llm(user_question: str, relevant_news: list) -> str:
    """Отправить запрос к локальному LLM"""

    news_context = "Релевантные финансовые новости (отсортированы по важности для банка):\n\n"
    for i, news in enumerate(relevant_news, 1):
        news_context += f"{i}. {news['title']}\n"
        news_context += f"   Источник: {news['source']}\n"
        news_context += f"   Описание: {news['description']}\n"
        news_context += f"   Дата: {news['published']}\n"
        news_context += f"   Релевантность: {news['similarity']:.2%}"
        if news.get('critical_keywords', 0) > 0:
            news_context += f" ⚠️ КРИТИЧНО (финансовых ключевых слов: {news['critical_keywords']})"
        news_context += "\n\n"

    system_prompt = """Ты - AI ассистент для банка, специализирующийся на финансовых новостях.
Тебе предоставлены наиболее релевантные новости, ОТСОРТИРОВАННЫЕ ПО ВАЖНОСТИ ДЛЯ БАНКОВСКОГО СЕКТОРА.

ВАЖНО:
- Приоритизируй ответы на основе предоставленных новостей (если они релевантны вопросу)
- Если в новостях ЕСТЬ релевантная информация - используй ее в первую очередь
- Приоритезируй новости с меткой ⚠️ КРИТИЧНО - это финансово-значимые события
- Фокусируйся на: санкциях, валюте, ЦБ, процентных ставках, кредитных рисках
- Если в новостях НЕТ информации по вопросу - напиши "Этой информации нет в актуальных новостях, но..." и дай ответ на основе своих знаний
- Указывай конкретные факты, цифры, компании
- Отвечай кратко, по делу, на русском языке
- Можешь использовать свои знания, если они полезны для ответа"""

    user_prompt = f"{news_context}\n\nВопрос: {user_question}\n\nОтвет:"

    payload = {
        "model": "qwen3-8b",  # Используем Qwen3 8B
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0.2,
        "max_tokens": 2000,  # Увеличено для более подробных ответов
        "stream": False
    }

    try:
        response = requests.post(LM_STUDIO_API_URL, json=payload, timeout=120)
        if response.status_code == 200:
            result = response.json()
            answer = result['choices'][0]['message']['content'].strip()

            # Прячем рассуждения под спойлер
            answer = hide_reasoning_under_spoiler(answer)

            return answer
        else:
            return f"Ошибка LLM API: {response.status_code}"
    except requests.exceptions.ConnectionError:
        raise HTTPException(
            status_code=503,
            detail="LM Studio недоступен. Запустите LM Studio с моделью Qwen3 8B"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка LLM: {str(e)}")

@app.on_event("startup")
async def startup_event():
    print("=" * 70)
    print("🤖 AI Agent Service запущен")
    print(f"📡 API: http://localhost:8002")
    print(f"📖 Docs: http://localhost:8002/docs")
    print(f"🔗 News Collector: {NEWS_COLLECTOR_URL}")
    print(f"🧠 LLM: {LM_STUDIO_API_URL}")
    print("=" * 70)

@app.get("/")
async def root():
    return {
        "service": "AI Agent API",
        "version": "1.0",
        "status": "running",
        "docs": "/docs",
        "dependencies": {
            "news_collector": NEWS_COLLECTOR_URL,
            "llm": LM_STUDIO_API_URL
        }
    }

@app.get("/health")
async def health():
    """Проверка здоровья сервиса и зависимостей"""
    status = {"service": "healthy", "dependencies": {}}

    # Проверка News Collector
    try:
        response = requests.get(f"{NEWS_COLLECTOR_URL}/health", timeout=5)
        status["dependencies"]["news_collector"] = "healthy" if response.status_code == 200 else "unhealthy"
    except:
        status["dependencies"]["news_collector"] = "unavailable"

    # Проверка LM Studio
    try:
        response = requests.get(f"{LM_STUDIO_API_URL.replace('/v1/chat/completions', '/v1/models')}", timeout=5)
        status["dependencies"]["llm"] = "healthy" if response.status_code == 200 else "unhealthy"
    except:
        status["dependencies"]["llm"] = "unavailable"

    return status

@app.post("/ask", response_model=AgentResponse)
async def ask_question(request: QueryRequest):
    """
    Задать вопрос агенту

    Процесс:
    1. Получает релевантные новости от News Collector
    2. Отправляет их в локальный LLM
    3. Генерирует ответ
    """

    # 1. Получаем новости от коллектора
    search_result = get_news_from_collector(request.question, request.top_k)

    if not search_result['news']:
        return AgentResponse(
            question=request.question,
            answer="Релевантных новостей не найдено. База может быть пуста.",
            news_found=0,
            top_news=[],
            timestamp=datetime.now().isoformat()
        )

    # 2. Генерируем ответ с помощью LLM
    answer = query_llm(request.question, search_result['news'])

    # 3. Формируем топ-5 новостей для ответа
    top_news = [
        NewsItem(
            title=item['title'],
            description=item['description'],
            link=item['link'],
            source=item['source'],
            published=item['published'],
            similarity=item['similarity'],
            critical_keywords=item.get('critical_keywords', 0)
        )
        for item in search_result['news'][:5]
    ]

    return AgentResponse(
        question=request.question,
        answer=answer,
        news_found=len(search_result['news']),
        top_news=top_news,
        timestamp=datetime.now().isoformat()
    )

@app.get("/collector/stats")
async def get_collector_stats():
    """Получить статистику от News Collector"""
    try:
        response = requests.get(f"{NEWS_COLLECTOR_URL}/stats", timeout=5)
        response.raise_for_status()
        return response.json()
    except:
        raise HTTPException(status_code=503, detail="News Collector недоступен")

if __name__ == "__main__":
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8002,
        log_level="info"
    )
