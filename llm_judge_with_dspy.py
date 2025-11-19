#!/usr/bin/env python3
"""
LLM-as-Judge с автоматической оптимизацией промптов через DSPy
"""

import json
import random
import dspy
from typing import List, Dict
from sklearn.metrics import accuracy_score, classification_report


class NewsRelevanceSignature(dspy.Signature):
    """Оценка релевантности новости поисковому запросу"""

    query = dspy.InputField(desc="Поисковый запрос пользователя")
    title = dspy.InputField(desc="Заголовок новости")
    description = dspy.InputField(desc="Описание новости")

    relevance_score = dspy.OutputField(
        desc="Оценка релевантности: 0 (нерелевантна), 1 (слабо связана), 2 (релевантна), 3 (идеально)"
    )


class NewsRelevanceModule(dspy.Module):
    """DSPy модуль для оценки релевантности с Chain of Thought"""

    def __init__(self):
        super().__init__()
        # Используем Chain of Thought для лучших результатов
        self.prog = dspy.ChainOfThought(NewsRelevanceSignature)

    def forward(self, query, title, description=""):
        # DSPy автоматически форматирует промпт
        result = self.prog(query=query, title=title, description=description)

        # Извлекаем оценку
        score_str = result.relevance_score.strip()

        # Пытаемся распарсить
        for char in score_str:
            if char.isdigit():
                score = int(char)
                if 0 <= score <= 3:
                    return score

        # Если не получилось, возвращаем 2 (релевантна) как baseline
        print(f"⚠️ Не удалось распарсить: '{score_str}', использую 2")
        return 2


def load_and_split_dataset(dataset_path: str = "ltr_dataset.json",
                           train_size: int = 30,
                           test_size: int = 20):
    """
    Загружает датасет и разбивает на train/test

    Returns:
        train_data: Список примеров для обучения (оптимизации промпта)
        test_data: Список примеров для тестирования
    """

    with open(dataset_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Фильтруем размеченные
    labeled = [item for item in data if item.get('label') is not None]

    # Перемешиваем
    random.seed(42)
    random.shuffle(labeled)

    # Разбиваем
    train_data = labeled[:train_size]
    test_data = labeled[train_size:train_size + test_size]

    return train_data, test_data


def dataset_to_dspy_examples(data: List[Dict], with_labels: bool = True):
    """
    Конвертирует датасет в DSPy примеры
    """

    examples = []
    for item in data:
        example_dict = {
            'query': item['query'],
            'title': item.get('title', ''),
            'description': item.get('description', '')
        }

        if with_labels:
            example_dict['relevance_score'] = str(item['label'])

        examples.append(dspy.Example(**example_dict).with_inputs('query', 'title', 'description'))

    return examples


def metric(gold: dspy.Example, pred, trace=None):
    """
    Метрика для DSPy оптимизатора

    Возвращает 1.0 если оценка точная, 0.5 если в пределах ±1, 0.0 иначе
    """

    try:
        gold_score = int(gold.relevance_score)
    except:
        return 0.0

    # pred - это результат forward(), т.е. int
    pred_score = pred if isinstance(pred, int) else 0

    # Точное совпадение
    if gold_score == pred_score:
        return 1.0

    # В пределах ±1 (приемлемо для ранжирования)
    if abs(gold_score - pred_score) == 1:
        return 0.5

    return 0.0


def optimize_with_dspy(train_data: List[Dict],
                      test_data: List[Dict],
                      max_bootstrapped_demos: int = 4):
    """
    Оптимизирует промпт через DSPy и тестирует

    Args:
        train_data: Данные для оптимизации
        test_data: Данные для тестирования
        max_bootstrapped_demos: Сколько few-shot примеров использовать
    """

    print("=" * 70)
    print("🚀 DSPy: Автоматическая оптимизация промптов")
    print("=" * 70)
    print()

    # Настраиваем LM Studio как бэкенд
    print("🔧 Настройка LM Studio...")
    lm = dspy.LM(
        model="openai/qwen3-14b",  # Используем qwen3-14b через OpenAI API
        api_base="http://localhost:1234/v1",
        api_key="dummy",  # LM Studio не требует ключ
        temperature=0.0,
        max_tokens=50
    )
    dspy.configure(lm=lm)
    print("✓ LM Studio подключен")
    print()

    # Конвертируем данные
    print(f"📊 Подготовка данных:")
    print(f"   Train: {len(train_data)} примеров")
    print(f"   Test:  {len(test_data)} примеров")

    train_examples = dataset_to_dspy_examples(train_data, with_labels=True)
    test_examples = dataset_to_dspy_examples(test_data, with_labels=True)
    print()

    # Инициализируем модуль
    print("🏗️ Создание DSPy модуля...")
    relevance_module = NewsRelevanceModule()
    print()

    # Оптимизация с BootstrapFewShot
    print(f"⚙️ Запуск оптимизации (может занять 3-5 минут)...")
    print(f"   Метод: BootstrapFewShot")
    print(f"   Few-shot примеров: {max_bootstrapped_demos}")
    print()

    optimizer = dspy.BootstrapFewShot(
        metric=metric,
        max_bootstrapped_demos=max_bootstrapped_demos,
        max_labeled_demos=max_bootstrapped_demos
    )

    try:
        optimized_module = optimizer.compile(
            relevance_module,
            trainset=train_examples
        )
        print("✓ Оптимизация завершена!")
        print()
    except Exception as e:
        print(f"⚠️ Ошибка оптимизации: {e}")
        print("Используем базовый модуль без оптимизации")
        optimized_module = relevance_module
        print()

    # Тестирование
    print("=" * 70)
    print("🎯 Тестирование оптимизированного модуля")
    print("=" * 70)
    print()

    y_true = []
    y_pred = []

    for i, example in enumerate(test_examples, 1):
        true_label = int(example.relevance_score)

        print(f"[{i}/{len(test_examples)}] {example.query[:40]}... ", end='', flush=True)

        try:
            pred_label = optimized_module(
                query=example.query,
                title=example.title,
                description=example.description
            )

            y_true.append(true_label)
            y_pred.append(pred_label)

            match = "✓" if pred_label == true_label else "✗"
            print(f"(истина: {true_label}, предсказано: {pred_label}) {match}")

        except Exception as e:
            print(f"⚠️ Ошибка: {e}")

    print()
    print("=" * 70)

    # Метрики
    if y_true:
        accuracy = accuracy_score(y_true, y_pred)
        print(f"🎯 Точность: {accuracy * 100:.1f}%")
        print()

        # Точность ±1
        within_one = sum(1 for t, p in zip(y_true, y_pred) if abs(t - p) <= 1) / len(y_true)
        print(f"📊 Точность ±1: {within_one * 100:.1f}%")
        print()

        print("📋 Детальный отчет:")
        print(classification_report(y_true, y_pred,
                                   labels=[0, 1, 2, 3],
                                   target_names=['0 (нерелев)', '1 (слабо)', '2 (релев)', '3 (идеал)'],
                                   zero_division=0))

    print("=" * 70)

    # Показываем оптимизированный промпт
    print()
    print("=" * 70)
    print("📝 Оптимизированный промпт:")
    print("=" * 70)
    try:
        lm.inspect_history(n=1)
    except:
        print("(не удалось получить историю)")
    print()

    return optimized_module


if __name__ == "__main__":
    # Загружаем данные
    train_data, test_data = load_and_split_dataset(
        train_size=30,  # Используем 30 примеров для оптимизации
        test_size=20    # Тестируем на 20 примерах
    )

    # Запускаем оптимизацию
    optimized_module = optimize_with_dspy(
        train_data=train_data,
        test_data=test_data,
        max_bootstrapped_demos=4  # 4 few-shot примера
    )
