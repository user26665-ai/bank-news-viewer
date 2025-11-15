# 🌐 Инструкция по хостингу для коллег

## Быстрый старт (5 минут)

### Шаг 1: Запустите ngrok для доступа к вашему компьютеру

```bash
# Установка ngrok (если еще не установлен)
brew install ngrok

# Регистрация (бесплатно)
# Перейдите на https://dashboard.ngrok.com/get-started/your-authtoken
# Скопируйте authtoken и выполните:
ngrok config add-authtoken YOUR_AUTHTOKEN_HERE

# Запустите 2 туннеля в отдельных терминалах:

# Terminal 1 - News Collector API
ngrok http 8001

# Terminal 2 - AI Agent API
ngrok http 8002
```

**Ngrok покажет URL типа:**
```
Forwarding  https://abc123-456.ngrok-free.app -> http://localhost:8001
```

**Скопируйте оба URL!**

---

### Шаг 2: Обновите файл news_viewer_hosted.html

Откройте файл `news_viewer_hosted.html` и замените URL:

```javascript
// БЫЛО:
const API_URL = 'http://localhost:8001';
const AI_URL = 'http://localhost:8002';

// СТАЛО (вставьте ваши ngrok URL):
const API_URL = 'https://abc123-456.ngrok-free.app';
const AI_URL = 'https://xyz789-012.ngrok-free.app';
```

---

### Шаг 3: Выложите на GitHub Pages (бесплатно)

```bash
cd /Users/david/bank_news_agent

# Создать новый репозиторий
git init
git add news_viewer_hosted.html
git commit -m "Add hosted news viewer"

# Создать репозиторий на GitHub (через браузер или CLI)
# Если используете gh CLI:
gh repo create bank-news-viewer --public --source=. --push

# Или добавить remote вручную:
git remote add origin https://github.com/YOUR_USERNAME/bank-news-viewer.git
git branch -M main
git push -u origin main
```

**Настройка GitHub Pages:**
1. Откройте репозиторий на GitHub
2. Settings → Pages
3. Source: Deploy from branch → `main` → `/root` → Save

**Готово!** Сайт будет доступен по адресу:
```
https://YOUR_USERNAME.github.io/bank-news-viewer/news_viewer_hosted.html
```

---

### Шаг 4: Отправьте ссылку коллегам

Просто отправьте URL:
```
https://YOUR_USERNAME.github.io/bank-news-viewer/news_viewer_hosted.html
```

**Важно:** Ваш компьютер должен быть включен, сервисы запущены, и ngrok работает!

---

## Альтернатива: Простой способ без GitHub

Просто откройте `news_viewer_hosted.html` локально и отправьте коллегам:

```bash
# Запустите простой HTTP сервер
python3 -m http.server 8080
```

Затем используйте ngrok для порта 8080:
```bash
ngrok http 8080
```

Отправьте коллегам ngrok URL, например:
```
https://abc123.ngrok-free.app/news_viewer_hosted.html
```

---

## Проверка работоспособности

```bash
# Проверьте, что сервисы запущены:
curl http://localhost:8001/health
curl http://localhost:8002/health

# Проверьте ngrok URL в браузере:
# Откройте ваш ngrok URL в браузере - должен быть JSON ответ
```

---

## Troubleshooting

**Ошибка CORS:**
- Сервисы уже настроены с CORS, должно работать из коробки

**Ngrok показывает "Visit Site" страницу:**
- Бесплатный ngrok показывает предупреждение - нажмите "Visit Site"
- Для убрать - используйте платный план ($8/месяц)

**Сервисы не отвечают:**
- Убедитесь что запущены: `./start_services.sh`
- Проверьте логи: `tail -f news_collector.log ai_agent.log`

---

## Остановка сервисов

```bash
# Остановить backend
./stop_services.sh

# Остановить ngrok
# Нажмите Ctrl+C в терминалах с ngrok
```
