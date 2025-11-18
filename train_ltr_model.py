#!/usr/bin/env python3
"""
Обучение LTR модели на размеченных данных
"""

import json
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import lightgbm as lgb
import pickle

def load_annotated_dataset(json_path: str):
    """Загружает размеченный датасет"""
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Фильтруем только размеченные
    annotated = [item for item in data if item['label'] is not None]

    print(f"📊 Статистика датасета:")
    print(f"   Всего записей: {len(data)}")
    print(f"   Размечено: {len(annotated)}")

    if not annotated:
        raise ValueError("Нет размеченных данных! Сначала разметьте датасет.")

    # Считаем распределение оценок
    labels = [item['label'] for item in annotated]
    for i in range(4):
        count = labels.count(i)
        pct = count / len(labels) * 100
        print(f"   Оценка {i}: {count} ({pct:.1f}%)")

    return annotated

def prepare_training_data(annotated_data):
    """Подготавливает данные для обучения"""

    # Создаем DataFrame
    rows = []
    for item in annotated_data:
        row = {
            'query': item['query'],
            'label': item['label'],
            **item['features']
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    # Фичи для обучения
    feature_columns = [col for col in df.columns if col not in ['query', 'label']]

    X = df[feature_columns]
    y = df['label']

    # Группы запросов (для LTR важно!)
    query_groups = df.groupby('query').size().values

    print(f"\n📈 Фичи для обучения ({len(feature_columns)}):")
    for col in feature_columns:
        print(f"   • {col}")

    return X, y, query_groups, feature_columns

def train_lightgbm_ranker(X, y, query_groups):
    """Обучает LightGBM Ranker"""

    print("\n🚀 Обучение LightGBM Ranker...")

    # Разделяем на train/test по группам запросов (важно!)
    n_groups = len(query_groups)
    n_train_groups = int(0.8 * n_groups)

    # Разделяем группы
    train_groups = query_groups[:n_train_groups]
    test_groups = query_groups[n_train_groups:]

    # Находим индекс разделения в данных
    train_size = sum(train_groups)

    X_train = X[:train_size]
    y_train = y[:train_size]

    X_test = X[train_size:]
    y_test = y[train_size:]

    print(f"   Train: {len(X_train)} записей, {len(train_groups)} запросов")
    print(f"   Test: {len(X_test)} записей, {len(test_groups)} запросов")

    # Создаем датасеты LightGBM
    train_data = lgb.Dataset(X_train, label=y_train, group=train_groups)
    test_data = lgb.Dataset(X_test, label=y_test, group=test_groups, reference=train_data)

    # Параметры модели
    params = {
        'objective': 'lambdarank',
        'metric': 'ndcg',
        'ndcg_eval_at': [1, 3, 5, 10],
        'learning_rate': 0.05,
        'num_leaves': 31,
        'feature_fraction': 0.8,
        'bagging_fraction': 0.8,
        'bagging_freq': 5,
        'verbose': 1,
        'max_depth': 6,
    }

    # Обучение
    evals_result = {}
    model = lgb.train(
        params,
        train_data,
        num_boost_round=200,
        valid_sets=[train_data, test_data],
        valid_names=['train', 'test'],
        callbacks=[
            lgb.early_stopping(stopping_rounds=20),
            lgb.log_evaluation(period=10),
            lgb.record_evaluation(evals_result)
        ]
    )

    print("\n✅ Обучение завершено!")

    # Важность фичей
    importance = model.feature_importance(importance_type='gain')
    feature_names = X.columns

    print("\n📊 Важность фичей (top-5):")
    feature_importance = sorted(zip(feature_names, importance), key=lambda x: x[1], reverse=True)
    for i, (name, imp) in enumerate(feature_importance[:5], 1):
        print(f"   {i}. {name}: {imp:.2f}")

    # Метрики
    best_score = evals_result['test']['ndcg@10'][-1]
    print(f"\n🎯 NDCG@10 на тесте: {best_score:.4f}")

    return model, feature_importance

def save_model(model, feature_columns, output_path='ltr_model.pkl'):
    """Сохраняет модель"""
    model_data = {
        'model': model,
        'feature_columns': feature_columns
    }

    with open(output_path, 'wb') as f:
        pickle.dump(model_data, f)

    print(f"\n💾 Модель сохранена: {output_path}")

def main():
    print("=" * 70)
    print("🎓 Обучение Learning to Rank модели")
    print("=" * 70)
    print()

    # 1. Загружаем данные
    annotated = load_annotated_dataset('ltr_dataset.json')

    # 2. Подготавливаем данные
    X, y, query_groups, feature_columns = prepare_training_data(annotated)

    # 3. Обучаем модель
    model, feature_importance = train_lightgbm_ranker(X, y, query_groups)

    # 4. Сохраняем
    save_model(model, feature_columns)

    print("\n" + "=" * 70)
    print("✅ Готово!")
    print("=" * 70)
    print()
    print("📌 Следующие шаги:")
    print("   1. Модель сохранена в ltr_model.pkl")
    print("   2. Интегрируйте модель в поиск через integrate_ltr_model.py")
    print()

if __name__ == "__main__":
    try:
        import lightgbm
    except ImportError:
        print("❌ Установите LightGBM: pip install lightgbm")
        exit(1)

    main()
