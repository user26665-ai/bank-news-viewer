#!/usr/bin/env python3
"""
LLM-as-Judge v2 с Chain of Thought

Добавляем reasoning step для лучших результатов
"""

import json
import requests
import random
from typing import List, Dict, Optional
from sklearn.metrics import accuracy_score, classification_report


class LLMJudgeV2:
    """LLM-as-Judge с Chain of Thought"""

    def __init__(self, lm_studio_url: str = "http://localhost:1234/v1/chat/completions"):
        self.lm_studio_url = lm_studio_url

    def build_prompt_with_cot(self, query: str, news_title: str, news_description: str,
                              few_shot_examples: List[Dict] = None) -> str:
        """
        Промпт с Chain of Thought - модель сначала рассуждает, потом дает оценку
        """

        prompt = """Ты - эксперт по оценке релевантности новостей банковской тематики.

Твоя задача: оценить релевантность новости поисковому запросу.

Шкала оценки:
- 0 (Нерелевантна) - новость совсем не подходит к запросу
- 1 (Слабо связана) - новость косвенно связана с темой
- 2 (Релевантна) - новость хорошо отвечает на запрос
- 3 (Идеально) - новость точно отвечает на запрос

"""

        # Few-shot примеры с reasoning
        if few_shot_examples:
            prompt += "Примеры разметки:\n\n"
            for i, ex in enumerate(few_shot_examples, 1):
                prompt += f"Пример {i}:\n"
                prompt += f"Запрос: {ex['query']}\n"
                prompt += f"Новость: {ex['title']}\n"

                # Генерируем reasoning на основе label
                label = ex['label']
                if label == 3:
                    reasoning = "Новость напрямую отвечает на запрос, содержит ключевые слова."
                elif label == 2:
                    reasoning = "Новость связана с темой запроса, может быть полезна."
                elif label == 1:
                    reasoning = "Новость затрагивает тему косвенно."
                else:
                    reasoning = "Новость не связана с запросом."

                prompt += f"Рассуждение: {reasoning}\n"
                prompt += f"Оценка: {label}\n\n"

        # Текущий вопрос
        prompt += "Теперь оцени следующую пару:\n\n"
        prompt += f"Запрос: {query}\n"
        prompt += f"Новость: {news_title}\n"
        if news_description:
            prompt += f"Описание: {news_description}\n"

        prompt += "\nСначала напиши краткое рассуждение (1-2 предложения), затем на новой строке только цифру оценки (0, 1, 2 или 3)."

        return prompt

    def judge(self, query: str, news_title: str, news_description: str = "",
              few_shot_examples: List[Dict] = None, use_cot: bool = True) -> Optional[int]:
        """
        Оценивает релевантность

        Args:
            use_cot: Использовать Chain of Thought
        """

        if use_cot:
            prompt = self.build_prompt_with_cot(query, news_title, news_description, few_shot_examples)
            max_tokens = 150  # Больше токенов для рассуждения
        else:
            # Старый промпт без CoT
            prompt = self.build_simple_prompt(query, news_title, news_description, few_shot_examples)
            max_tokens = 10

        try:
            response = requests.post(
                self.lm_studio_url,
                json={
                    "model": "qwen",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.0,  # Детерминированно
                    "max_tokens": max_tokens,
                },
                timeout=30
            )

            if response.status_code == 200:
                result = response.json()
                answer = result['choices'][0]['message']['content'].strip()

                # Извлекаем последнюю цифру (это должна быть оценка после рассуждения)
                lines = answer.split('\n')
                for line in reversed(lines):
                    for char in line:
                        if char.isdigit():
                            score = int(char)
                            if 0 <= score <= 3:
                                return score

                # Если не нашли в конце, ищем любую цифру
                for char in answer:
                    if char.isdigit():
                        score = int(char)
                        if 0 <= score <= 3:
                            return score

                print(f"⚠️ Не удалось извлечь оценку из: {answer[:100]}")
                return None
            else:
                print(f"❌ Ошибка API: {response.status_code}")
                return None

        except Exception as e:
            print(f"❌ Ошибка: {e}")
            return None

    def build_simple_prompt(self, query: str, news_title: str, news_description: str,
                           few_shot_examples: List[Dict] = None) -> str:
        """Простой промпт без CoT (для сравнения)"""

        prompt = """Ты - эксперт по оценке релевантности новостей банковской тематики.

Оцени релевантность новости запросу по шкале:
- 0 (нерелевантна)
- 1 (слабо связана)
- 2 (релевантна)
- 3 (идеально)

"""

        if few_shot_examples:
            prompt += "Примеры:\n\n"
            for ex in few_shot_examples[:3]:
                prompt += f"Запрос: {ex['query']}\n"
                prompt += f"Новость: {ex['title']}\n"
                prompt += f"Оценка: {ex['label']}\n\n"

        prompt += f"Запрос: {query}\n"
        prompt += f"Новость: {news_title}\n"
        prompt += "\nОтветь только одной цифрой:"

        return prompt


def evaluate_both_approaches(dataset_path: str = "ltr_dataset.json",
                            num_eval: int = 20):
    """
    Сравнивает два подхода: с CoT и без
    """

    print("=" * 70)
    print("📊 Сравнение: Chain of Thought vs Простой промпт")
    print("=" * 70)
    print()

    # Загружаем данные
    with open(dataset_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    labeled = [item for item in data if item.get('label') is not None]
    random.seed(42)
    random.shuffle(labeled)

    few_shot = labeled[:5]
    test_data = labeled[5:5 + num_eval]

    print(f"📚 Few-shot: {len(few_shot)} примеров")
    print(f"🎯 Тест: {len(test_data)} примеров")
    print()

    judge = LLMJudgeV2()

    # Тест 1: Без CoT
    print("=" * 70)
    print("🔵 Подход 1: Простой промпт (БЕЗ Chain of Thought)")
    print("=" * 70)
    print()

    y_true_simple = []
    y_pred_simple = []

    for i, item in enumerate(test_data, 1):
        print(f"[{i}/{len(test_data)}] ", end='', flush=True)

        score = judge.judge(
            query=item['query'],
            news_title=item.get('title', ''),
            news_description=item.get('description', ''),
            few_shot_examples=few_shot,
            use_cot=False  # БЕЗ CoT
        )

        if score is not None:
            y_true_simple.append(item['label'])
            y_pred_simple.append(score)
            match = "✓" if score == item['label'] else "✗"
            print(f"{item['query'][:30]}... (истина: {item['label']}, предсказано: {score}) {match}")
        else:
            print(f"ОШИБКА")

    print()
    if y_true_simple:
        acc_simple = accuracy_score(y_true_simple, y_pred_simple)
        within_one_simple = sum(1 for t, p in zip(y_true_simple, y_pred_simple) if abs(t - p) <= 1) / len(y_true_simple)
        print(f"🎯 Точность: {acc_simple * 100:.1f}%")
        print(f"📊 Точность ±1: {within_one_simple * 100:.1f}%")
    print()

    # Тест 2: С CoT
    print("=" * 70)
    print("🟢 Подход 2: Chain of Thought (С рассуждением)")
    print("=" * 70)
    print()

    y_true_cot = []
    y_pred_cot = []

    for i, item in enumerate(test_data, 1):
        print(f"[{i}/{len(test_data)}] ", end='', flush=True)

        score = judge.judge(
            query=item['query'],
            news_title=item.get('title', ''),
            news_description=item.get('description', ''),
            few_shot_examples=few_shot,
            use_cot=True  # С CoT
        )

        if score is not None:
            y_true_cot.append(item['label'])
            y_pred_cot.append(score)
            match = "✓" if score == item['label'] else "✗"
            print(f"{item['query'][:30]}... (истина: {item['label']}, предсказано: {score}) {match}")
        else:
            print(f"ОШИБКА")

    print()
    if y_true_cot:
        acc_cot = accuracy_score(y_true_cot, y_pred_cot)
        within_one_cot = sum(1 for t, p in zip(y_true_cot, y_pred_cot) if abs(t - p) <= 1) / len(y_true_cot)
        print(f"🎯 Точность: {acc_cot * 100:.1f}%")
        print(f"📊 Точность ±1: {within_one_cot * 100:.1f}%")
    print()

    # Сравнение
    print("=" * 70)
    print("📈 ИТОГОВОЕ СРАВНЕНИЕ")
    print("=" * 70)
    print()
    print(f"{'Метод':<30} {'Точность':>12} {'Точность ±1':>15}")
    print("-" * 70)
    if y_true_simple:
        print(f"{'Простой промпт':<30} {acc_simple * 100:>11.1f}% {within_one_simple * 100:>14.1f}%")
    if y_true_cot:
        print(f"{'Chain of Thought':<30} {acc_cot * 100:>11.1f}% {within_one_cot * 100:>14.1f}%")

        if y_true_simple:
            improvement = (acc_cot - acc_simple) * 100
            sign = "+" if improvement > 0 else ""
            print(f"{'Улучшение:':<30} {sign}{improvement:>11.1f}%")
    print()


if __name__ == "__main__":
    evaluate_both_approaches(num_eval=20)
