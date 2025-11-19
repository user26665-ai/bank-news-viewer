#!/usr/bin/env python3
"""
Простой прототип LLM-as-Judge для оценки релевантности новостей

Использует Qwen 14B через LM Studio для оценки релевантности пары (запрос, новость)
"""

import json
import requests
from typing import List, Dict, Optional


class LLMJudge:
    """LLM-as-Judge для оценки релевантности новостей"""

    def __init__(self, lm_studio_url: str = "http://localhost:1234/v1/chat/completions"):
        self.lm_studio_url = lm_studio_url

    def build_prompt(self, query: str, news_title: str, news_description: str,
                     few_shot_examples: List[Dict] = None) -> str:
        """
        Формирует промпт для оценки релевантности

        Args:
            query: Поисковый запрос пользователя
            news_title: Заголовок новости
            news_description: Описание новости
            few_shot_examples: Примеры с известной разметкой
        """

        prompt = """Ты - эксперт по оценке релевантности новостей банковской тематики.

Твоя задача: оценить, насколько новость релевантна поисковому запросу.

Шкала оценки:
- 0 (Нерелевантна) - новость совсем не подходит к запросу
- 1 (Слабо связана) - новость косвенно связана с темой
- 2 (Релевантна) - новость хорошо отвечает на запрос
- 3 (Идеально) - новость точно отвечает на запрос

"""

        # Добавляем few-shot примеры, если есть
        if few_shot_examples:
            prompt += "Примеры разметки:\n\n"
            for i, ex in enumerate(few_shot_examples, 1):
                prompt += f"Пример {i}:\n"
                prompt += f"Запрос: {ex['query']}\n"
                prompt += f"Заголовок: {ex['title']}\n"
                if ex.get('description'):
                    prompt += f"Описание: {ex['description']}\n"
                prompt += f"Оценка: {ex['label']}\n\n"

        # Добавляем текущий вопрос
        prompt += "Теперь оцени следующую пару:\n\n"
        prompt += f"Запрос: {query}\n"
        prompt += f"Заголовок: {news_title}\n"
        if news_description:
            prompt += f"Описание: {news_description}\n"

        prompt += "\nОтветь ТОЛЬКО одной цифрой (0, 1, 2 или 3) без объяснений."

        return prompt

    def judge(self, query: str, news_title: str, news_description: str = "",
              few_shot_examples: List[Dict] = None) -> Optional[int]:
        """
        Оценивает релевантность пары (запрос, новость)

        Returns:
            Оценка 0-3 или None если ошибка
        """

        prompt = self.build_prompt(query, news_title, news_description, few_shot_examples)

        try:
            response = requests.post(
                self.lm_studio_url,
                json={
                    "model": "qwen3-14b",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,  # Детерминированно
                    "max_tokens": 300,  # Увеличиваем для полного ответа
                },
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                answer = result['choices'][0]['message']['content'].strip()

                # DEBUG: показываем полный ответ первых 3 примеров
                # print(f"\n🔍 Полный ответ модели:\n{answer}\n")

                # Модель может использовать <think> теги - ищем после них
                # Сначала проверяем, есть ли </think>
                if '</think>' in answer:
                    # Берем все после </think>
                    after_think = answer.split('</think>')[-1]
                    for char in after_think:
                        if char.isdigit():
                            score = int(char)
                            if 0 <= score <= 3:
                                return score

                # Если нет </think>, ищем просто последнюю цифру
                digits = [int(c) for c in answer if c.isdigit() and 0 <= int(c) <= 3]
                if digits:
                    return digits[-1]  # Последняя цифра в диапазоне 0-3

                print(f"⚠️ Не удалось извлечь оценку из ответа: {answer[:200]}")
                return None
            else:
                print(f"❌ Ошибка API: {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return None


def load_few_shot_examples(dataset_path: str = "ltr_dataset.json",
                           count: int = 5) -> List[Dict]:
    """
    Загружает несколько примеров из размеченного датасета для few-shot

    Выбирает по 1-2 примера каждого класса (0, 1, 2, 3)
    """

    try:
        with open(dataset_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        # Фильтруем только размеченные примеры
        labeled = [item for item in data if item.get('label') is not None]

        if not labeled:
            print("⚠️ Нет размеченных примеров в датасете")
            return []

        # Группируем по label
        by_label = {0: [], 1: [], 2: [], 3: []}
        for item in labeled:
            label = item['label']
            if label in by_label:
                by_label[label].append(item)

        # Берем по 1 примеру каждого класса
        examples = []
        for label in [0, 1, 2, 3]:
            if by_label[label]:
                examples.append(by_label[label][0])

        return examples[:count]

    except FileNotFoundError:
        print(f"⚠️ Файл {dataset_path} не найден")
        return []
    except Exception as e:
        print(f"❌ Ошибка загрузки датасета: {e}")
        return []


def test_llm_judge():
    """Тестовый запуск LLM-as-Judge"""

    print("=" * 70)
    print("🧪 Тестирование LLM-as-Judge на Qwen 14B")
    print("=" * 70)
    print()

    # Инициализируем судью
    judge = LLMJudge()

    # Загружаем few-shot примеры
    print("📚 Загружаем few-shot примеры из датасета...")
    few_shot = load_few_shot_examples(count=4)

    if few_shot:
        print(f"✓ Загружено {len(few_shot)} примеров")
        for ex in few_shot:
            print(f"  - Label {ex['label']}: {ex['query'][:50]}...")
    else:
        print("⚠️ Few-shot примеры не загружены, работаем без них")
    print()

    # Тестовые пары (запрос, заголовок, ожидаемая оценка)
    test_cases = [
        {
            "query": "ЦБ повысил ставку",
            "title": "Центробанк РФ повысил ключевую ставку до 18%",
            "description": "Совет директоров Банка России принял решение повысить ключевую ставку на 200 б.п., до 18% годовых",
            "expected": 3
        },
        {
            "query": "ЦБ повысил ставку",
            "title": "Курс доллара вырос до 95 рублей",
            "description": "На Московской бирже доллар подорожал на 2 рубля",
            "expected": 0
        },
        {
            "query": "Сбербанк ипотека",
            "title": "Сбербанк снизил ставки по ипотеке до 15%",
            "description": "Крупнейший банк России запустил новую программу льготной ипотеки",
            "expected": 3
        },
        {
            "query": "Сбербанк ипотека",
            "title": "Сбербанк опубликовал финансовую отчетность",
            "description": "Чистая прибыль банка за год выросла на 20%",
            "expected": 1
        }
    ]

    print("🎯 Тестируем на примерах:")
    print()

    correct = 0
    total = 0

    for i, test in enumerate(test_cases, 1):
        print(f"Тест {i}:")
        print(f"  Запрос: {test['query']}")
        print(f"  Новость: {test['title']}")
        print(f"  Ожидается: {test['expected']}")

        # Получаем оценку от LLM
        score = judge.judge(
            query=test['query'],
            news_title=test['title'],
            news_description=test['description'],
            few_shot_examples=few_shot
        )

        if score is not None:
            match = "✓" if score == test['expected'] else "✗"
            print(f"  Получено: {score} {match}")
            total += 1
            if score == test['expected']:
                correct += 1
        else:
            print(f"  Получено: ОШИБКА")

        print()

    # Итоги
    print("=" * 70)
    if total > 0:
        accuracy = correct / total * 100
        print(f"📊 Результаты: {correct}/{total} ({accuracy:.1f}% точность)")
    else:
        print("❌ Не удалось выполнить ни одного теста")
    print("=" * 70)


if __name__ == "__main__":
    test_llm_judge()
