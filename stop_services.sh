#!/bin/bash
# Скрипт остановки сервисов

echo "🛑 Остановка сервисов..."
echo ""

# Останавливаем Service 1
if [ -f /tmp/news_collector.pid ]; then
    PID=$(cat /tmp/news_collector.pid)
    if kill $PID 2>/dev/null; then
        echo "✓ Service 1 (News Collector) остановлен (PID: $PID)"
    else
        echo "⚠️  Service 1 уже остановлен"
    fi
    rm /tmp/news_collector.pid
else
    echo "⚠️  Service 1: PID файл не найден"
fi

# Останавливаем Service 2
if [ -f /tmp/ai_agent.pid ]; then
    PID=$(cat /tmp/ai_agent.pid)
    if kill $PID 2>/dev/null; then
        echo "✓ Service 2 (AI Agent) остановлен (PID: $PID)"
    else
        echo "⚠️  Service 2 уже остановлен"
    fi
    rm /tmp/ai_agent.pid
else
    echo "⚠️  Service 2: PID файл не найден"
fi

# Дополнительная очистка на случай зомби-процессов
pkill -f "news_collector_service.py" 2>/dev/null
pkill -f "ai_agent_service.py" 2>/dev/null

echo ""
echo "✅ Готово"
