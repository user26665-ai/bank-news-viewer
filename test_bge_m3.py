#!/usr/bin/env python3
"""
Тестирование BGE-M3 для русского языка
"""

import numpy as np
from sentence_transformers import SentenceTransformer

def cosine_similarity(a, b):
    """Косинусное сходство"""
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def test_bge_m3():
    """Тестирование BGE-M3"""

    print("="*70)
    print("🧪 Тестирование BGE-M3")
    print("="*70)
    print()

    print("Загрузка модели BGE-M3...")
    model = SentenceTransformer('BAAI/bge-m3')
    print("✓ Модель загружена")
    print()

    # Тестовые пары
    query = 'США бомбит Венесуэлу'
    texts = [
        'США нанесли удары по Венесуэле',  # Релевантно
        'Не начисляют кэшбэк - Банки.ру',  # Нерелевантно
        'Солдат ВСУ взорвался в машине',  # Частично (взрыв)
        'WSJ: США оправдывают удары по Венесуэле тем, что фентанил – это химоружие',  # Очень релевантно
        'Валентина Шевченко: во сколько начало боя',  # Нерелевантно
        'США и Китай давят на экономику',  # Частично (только США)
        'Трамп заявил о планах по Венесуэле',  # Релевантно
    ]

    print(f"Запрос: '{query}'")
    print("-"*70)
    print()

    # Получаем эмбеддинги
    query_emb = model.encode(query, normalize_embeddings=True)
    text_embs = model.encode(texts, normalize_embeddings=True)

    results = []
    for text, text_emb in zip(texts, text_embs):
        sim = cosine_similarity(query_emb, text_emb)
        results.append({
            'text': text,
            'similarity': float(sim)
        })

    # Сортируем по similarity
    results.sort(key=lambda x: x['similarity'], reverse=True)

    print("Результаты (отсортировано по similarity):")
    print()

    for i, res in enumerate(results, 1):
        # Определяем релевантность для наглядности
        if 'Венесуэл' in res['text'] or 'удар' in res['text'] or 'бомб' in res['text']:
            label = "✓ РЕЛЕВАНТНО"
        elif 'США' in res['text'] or 'Трамп' in res['text']:
            label = "~ ЧАСТИЧНО"
        else:
            label = "✗ НЕРЕЛЕВАНТНО"

        print(f"{i}. [Similarity: {res['similarity']:.4f}] {label}")
        print(f"   {res['text']}")
        print()

    print("="*70)
    print()

    # Сравнение с худшим результатом
    best = results[0]
    worst = results[-1]

    print("📊 Статистика:")
    print(f"   Лучший результат: {best['similarity']:.4f}")
    print(f"   Худший результат: {worst['similarity']:.4f}")
    print(f"   Разница: {best['similarity'] - worst['similarity']:.4f}")
    print(f"   Диапазон: {worst['similarity']:.4f} - {best['similarity']:.4f}")
    print()

    if (best['similarity'] - worst['similarity']) > 0.15:
        print("✅ ХОРОШАЯ РАЗЛИЧИМОСТЬ - модель хорошо разделяет релевантные и нерелевантные тексты")
    else:
        print("⚠️ ПЛОХАЯ РАЗЛИЧИМОСТЬ - модель плохо разделяет тексты")

    print("="*70)


if __name__ == "__main__":
    test_bge_m3()
