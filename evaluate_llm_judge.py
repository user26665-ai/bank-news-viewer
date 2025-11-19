#!/usr/bin/env python3
"""
Оценка LLM-as-Judge на реальном размеченном датасете
"""

import json
import random
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from llm_as_judge_prototype import LLMJudge, load_few_shot_examples
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report


def evaluate_on_dataset(dataset_path: str = "ltr_dataset.json",
                        num_eval_samples: int = 20,
                        num_few_shot: int = 5):
    """
    Оценивает LLM-as-Judge на случайных примерах из датасета

    Args:
        dataset_path: Путь к размеченному датасету
        num_eval_samples: Сколько примеров использовать для оценки
        num_few_shot: Сколько few-shot примеров показывать
    """

    print("=" * 70)
    print("📊 Оценка LLM-as-Judge на реальном датасете")
    print("=" * 70)
    print()

    # Загружаем датасет
    with open(dataset_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Фильтруем размеченные примеры
    labeled = [item for item in data if item.get('label') is not None]
    print(f"📁 Датасет: {len(labeled)} размеченных примеров")
    print()

    # Разбиваем на few-shot и eval
    random.seed(42)
    random.shuffle(labeled)

    few_shot_data = labeled[:num_few_shot]
    eval_data = labeled[num_few_shot:num_few_shot + num_eval_samples]

    print(f"📚 Few-shot: {num_few_shot} примеров")
    print(f"🎯 Оценка на: {len(eval_data)} примерах")
    print()

    # Инициализируем судью
    judge = LLMJudge()

    # Оцениваем
    y_true = []
    y_pred = []

    print("🔄 Оценка примеров (может занять 1-2 минуты)...")
    print()

    for i, item in enumerate(eval_data, 1):
        query = item['query']
        title = item.get('title', '')
        description = item.get('description', '')
        true_label = item['label']

        print(f"[{i}/{len(eval_data)}] {query[:40]}... ", end='', flush=True)

        # Получаем предсказание
        pred_label = judge.judge(
            query=query,
            news_title=title,
            news_description=description,
            few_shot_examples=few_shot_data
        )

        if pred_label is not None:
            y_true.append(true_label)
            y_pred.append(pred_label)

            match = "✓" if pred_label == true_label else "✗"
            print(f"(истина: {true_label}, предсказано: {pred_label}) {match}")
        else:
            print("⚠️ ОШИБКА")

    print()
    print("=" * 70)

    # Метрики
    if y_true:
        accuracy = accuracy_score(y_true, y_pred)
        print(f"🎯 Точность: {accuracy * 100:.1f}%")
        print()

        print("📊 Матрица ошибок:")
        cm = confusion_matrix(y_true, y_pred, labels=[0, 1, 2, 3])
        print("   Предсказано →")
        print("   ", "  0   1   2   3")
        for i, row in enumerate(cm):
            print(f"{i}  {row}")
        print()

        print("📋 Детальный отчет:")
        print(classification_report(y_true, y_pred,
                                   labels=[0, 1, 2, 3],
                                   target_names=['0 (нерелев)', '1 (слабо)', '2 (релев)', '3 (идеал)']))

    else:
        print("❌ Не удалось получить предсказания")

    print("=" * 70)


if __name__ == "__main__":
    evaluate_on_dataset(
        num_eval_samples=20,  # Начнем с 20 примеров
        num_few_shot=4
    )
