#!/bin/bash
# Скрипт запуска обоих сервисов для банковского новостного агента

echo "🚀 Запуск банковского новостного агента..."
echo ""

# Проверяем LM Studio
if ! curl -s http://localhost:1234/v1/models > /dev/null 2>&1; then
    echo "⚠️  LM Studio не запущен!"
    echo "   Запустите LM Studio с моделью Qwen 8B перед запуском сервисов"
    exit 1
fi

echo "✓ LM Studio доступен"
echo ""

# Запускаем Service 1 (News Collector) в фоне
echo "📡 Запускаю Service 1: News Collector (порт 8001)..."
cd /Users/david/bank_news_agent
python3 news_collector_service.py > news_collector.log 2>&1 &
SERVICE1_PID=$!
echo "   PID: $SERVICE1_PID"

# Ждем запуска
sleep 5

# Проверяем что Service 1 запустился
if ! curl -s http://localhost:8001/health > /dev/null 2>&1; then
    echo "❌ Service 1 не запустился. Проверьте news_collector.log"
    kill $SERVICE1_PID 2>/dev/null
    exit 1
fi

echo "✓ Service 1 запущен"
echo ""

# Запускаем Service 2 (AI Agent) в фоне
echo "🤖 Запускаю Service 2: AI Agent (порт 8002)..."
python3 ai_agent_service.py > ai_agent.log 2>&1 &
SERVICE2_PID=$!
echo "   PID: $SERVICE2_PID"

# Ждем запуска
sleep 5

# Проверяем что Service 2 запустился
if ! curl -s http://localhost:8002/health > /dev/null 2>&1; then
    echo "❌ Service 2 не запустился. Проверьте ai_agent.log"
    kill $SERVICE1_PID 2>/dev/null
    kill $SERVICE2_PID 2>/dev/null
    exit 1
fi

echo "✓ Service 2 запущен"
echo ""

# Сохраняем PIDs
echo $SERVICE1_PID > /tmp/news_collector.pid
echo $SERVICE2_PID > /tmp/ai_agent.pid

echo "=" * 70
echo "✅ Оба сервиса успешно запущены!"
echo ""
echo "📊 API endpoints:"
echo "   Service 1 (News Collector): http://localhost:8001"
echo "   Service 2 (AI Agent):       http://localhost:8002"
echo ""
echo "📖 Документация:"
echo "   http://localhost:8001/docs"
echo "   http://localhost:8002/docs"
echo ""
echo "📝 Логи:"
echo "   news_collector.log"
echo "   ai_agent.log"
echo ""
echo "🛑 Остановка:"
echo "   ./stop_services.sh"
echo ""
echo "🧪 Тестирование:"
echo "   ./test_services.sh"
echo ""
