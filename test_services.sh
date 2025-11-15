#!/bin/bash
# Тестирование сервисов

echo "🧪 Тестирование банковского новостного агента"
echo ""

# Проверка Service 1
echo "1️⃣ Тест Service 1 (News Collector)..."
if curl -s http://localhost:8001/health > /dev/null; then
    echo "   ✓ Service 1 доступен"

    # Получаем статистику
    STATS=$(curl -s http://localhost:8001/stats)
    TOTAL=$(echo $STATS | python3 -c "import sys, json; print(json.load(sys.stdin)['total_news'])" 2>/dev/null)
    if [ ! -z "$TOTAL" ]; then
        echo "   ✓ В базе $TOTAL новостей"
    fi
else
    echo "   ❌ Service 1 недоступен"
    exit 1
fi
echo ""

# Проверка Service 2
echo "2️⃣ Тест Service 2 (AI Agent)..."
if curl -s http://localhost:8002/health > /dev/null; then
    echo "   ✓ Service 2 доступен"
else
    echo "   ❌ Service 2 недоступен"
    exit 1
fi
echo ""

# Тест поиска в Service 1
echo "3️⃣ Тест поиска новостей..."
SEARCH_RESULT=$(curl -s -X POST http://localhost:8001/search \
    -H "Content-Type: application/json" \
    -d '{"query": "санкции", "top_k": 5}')

FOUND=$(echo $SEARCH_RESULT | python3 -c "import sys, json; print(json.load(sys.stdin)['total_found'])" 2>/dev/null)
if [ ! -z "$FOUND" ]; then
    echo "   ✓ Найдено $FOUND новостей по запросу 'санкции'"
else
    echo "   ❌ Ошибка поиска"
    exit 1
fi
echo ""

# Тест AI агента
echo "4️⃣ Тест AI агента (может занять 10-30 сек)..."
AI_RESULT=$(curl -s -X POST http://localhost:8002/ask \
    -H "Content-Type: application/json" \
    -d '{"question": "санкции против России", "top_k": 10}' \
    --max-time 120)

if echo $AI_RESULT | grep -q "answer"; then
    echo "   ✓ AI агент работает"

    # Показываем топ-3 новости из ответа
    echo ""
    echo "   📊 Топ-3 новости из ответа:"
    echo $AI_RESULT | python3 -c "
import sys, json
data = json.load(sys.stdin)
for i, news in enumerate(data.get('top_news', [])[:3], 1):
    print(f'   {i}. {news[\"title\"]}')
    print(f'      Релевантность: {news[\"similarity\"]:.1%}', end='')
    if news.get('critical_keywords', 0) > 0:
        print(f' ⚠️ КРИТИЧНО', end='')
    print()
" 2>/dev/null
else
    echo "   ❌ Ошибка AI агента"
    echo "   Ответ: $AI_RESULT"
    exit 1
fi
echo ""

echo "=" * 70
echo "✅ Все тесты пройдены!"
echo ""
echo "💡 Попробуйте запросы через API:"
echo ""
echo "# Прямой поиск новостей (Service 1):"
echo "curl -X POST http://localhost:8001/search \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"query\": \"ключевая ставка ЦБ\", \"top_k\": 10}'"
echo ""
echo "# AI агент (Service 2):"
echo "curl -X POST http://localhost:8002/ask \\"
echo "  -H 'Content-Type: application/json' \\"
echo "  -d '{\"question\": \"решения ЦБ по ключевой ставке\", \"top_k\": 15}'"
echo ""
