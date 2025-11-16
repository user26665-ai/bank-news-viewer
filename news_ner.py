#!/usr/bin/env python3
"""
NER модуль для извлечения именованных сущностей из новостей
Использует natasha для русского языка
"""

from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsNERTagger,
    Doc
)
from typing import List, Dict, Set
import re
import pymorphy2

class NewsNERExtractor:
    """Извлекатель именованных сущностей из новостей"""

    def __init__(self):
        """Инициализация NER-моделей"""
        # Инициализируем компоненты natasha
        self.segmenter = Segmenter()
        self.morph_vocab = MorphVocab()
        self.emb = NewsEmbedding()
        self.morph_tagger = NewsMorphTagger(self.emb)
        self.ner_tagger = NewsNERTagger(self.emb)

        # Инициализируем pymorphy2 для нормализации
        self.morph = pymorphy2.MorphAnalyzer()

        # Словарь для нормализации типов сущностей
        self.entity_type_map = {
            'PER': 'person',      # Персона
            'LOC': 'location',    # Локация
            'ORG': 'organization' # Организация
        }

    def normalize_entity(self, entity_text: str, entity_type: str) -> str:
        """
        Нормализовать сущность (привести к начальной форме)

        Args:
            entity_text: текст сущности
            entity_type: тип сущности (person/organization/location)

        Returns:
            Нормализованная форма сущности
        """
        # Разбиваем на слова
        words = entity_text.split()
        normalized_words = []

        for word in words:
            # Пропускаем короткие слова и аббревиатуры (ЦБ, РФ и т.д.)
            if len(word) <= 2 or word.isupper():
                normalized_words.append(word)
                continue

            # Для персон нормализуем каждое слово (фамилия, имя)
            if entity_type == 'person':
                parsed = self.morph.parse(word)
                if parsed:
                    # Берем именительный падеж
                    normal_form = parsed[0].normal_form
                    # Сохраняем заглавную букву
                    if word[0].isupper():
                        normal_form = normal_form.capitalize()
                    normalized_words.append(normal_form)
                else:
                    normalized_words.append(word)
            else:
                # Для организаций и локаций нормализуем
                parsed = self.morph.parse(word)
                if parsed:
                    normal_form = parsed[0].normal_form
                    if word[0].isupper():
                        normal_form = normal_form.capitalize()
                    normalized_words.append(normal_form)
                else:
                    normalized_words.append(word)

        return ' '.join(normalized_words)

    def extract_entities(self, text: str) -> List[Dict]:
        """
        Извлечь все NER-сущности из текста

        Args:
            text: текст для анализа

        Returns:
            Список словарей с сущностями: [{'text': 'Сбербанк', 'type': 'organization'}, ...]
        """
        if not text or len(text.strip()) < 3:
            return []

        try:
            # Создаем документ и извлекаем сущности
            doc = Doc(text)
            doc.segment(self.segmenter)
            doc.tag_morph(self.morph_tagger)
            doc.tag_ner(self.ner_tagger)

            # Нормализуем сущности
            entities = []
            for span in doc.spans:
                entity_type = self.entity_type_map.get(span.type, span.type.lower())
                normalized = self.normalize_entity(span.text, entity_type)
                entities.append({
                    'text': span.text,
                    'normalized': normalized,
                    'type': entity_type,
                    'start': span.start,
                    'stop': span.stop
                })

            return entities

        except Exception as e:
            print(f"Ошибка NER extraction: {e}")
            return []

    def extract_from_news(self, title: str, description: str = "") -> Dict:
        """
        Извлечь сущности из заголовка и описания новости

        Args:
            title: заголовок новости
            description: описание новости (опционально)

        Returns:
            Словарь с сущностями по типам и полный список
        """
        # Извлекаем из заголовка (приоритетнее)
        title_entities = self.extract_entities(title)

        # Извлекаем из описания
        desc_entities = self.extract_entities(description) if description else []

        # Объединяем и дедуплицируем по тексту + типу
        all_entities = title_entities + desc_entities
        unique_entities = {}

        for entity in all_entities:
            key = (entity['text'].lower(), entity['type'])
            if key not in unique_entities:
                unique_entities[key] = entity

        entities_list = list(unique_entities.values())

        # Группируем по типам
        by_type = {
            'persons': [],
            'locations': [],
            'organizations': []
        }

        for entity in entities_list:
            if entity['type'] == 'person':
                by_type['persons'].append(entity['text'])
            elif entity['type'] == 'location':
                by_type['locations'].append(entity['text'])
            elif entity['type'] == 'organization':
                by_type['organizations'].append(entity['text'])

        return {
            'all': entities_list,
            'by_type': by_type,
            'count': len(entities_list)
        }

    def extract_key_entities(self, title: str, description: str = "") -> Set[str]:
        """
        Извлечь уникальный список всех сущностей (для индексации)

        Returns:
            Set с текстами всех найденных сущностей
        """
        result = self.extract_from_news(title, description)
        return {entity['text'] for entity in result['all']}

    def is_banking_entity(self, entity_text: str) -> bool:
        """
        Проверить, является ли сущность банковской/финансовой

        Args:
            entity_text: текст сущности

        Returns:
            True если это банк или финансовая организация
        """
        banking_keywords = {
            'банк', 'bank', 'сбер', 'втб', 'альфа', 'тинькофф',
            'газпромбанк', 'россельхозбанк', 'уралсиб', 'открытие',
            'цб', 'центробанк', 'центральный банк', 'фрс', 'ecb'
        }

        entity_lower = entity_text.lower()
        return any(keyword in entity_lower for keyword in banking_keywords)


def main():
    """Тестирование NER-модуля"""
    print("="*70)
    print("🔍 Тестирование NER-модуля для новостей")
    print("="*70)
    print()

    extractor = NewsNERExtractor()

    # Тестовые новости
    test_cases = [
        {
            'title': 'Сбербанк повысил ставки по вкладам в рублях',
            'description': 'Крупнейший банк России Сбербанк объявил о повышении ставок по вкладам'
        },
        {
            'title': 'ЦБ РФ оставил ключевую ставку на уровне 16%',
            'description': 'Центральный банк России принял решение сохранить ключевую ставку на заседании в Москве'
        },
        {
            'title': 'Владимир Путин встретился с главой ВТБ Андреем Костиным',
            'description': 'Президент России обсудил с главой банка ВТБ вопросы развития финансового сектора'
        }
    ]

    for i, test in enumerate(test_cases, 1):
        print(f"Тест {i}: {test['title']}")
        print("-" * 70)

        result = extractor.extract_from_news(test['title'], test['description'])

        print(f"Всего сущностей: {result['count']}")

        if result['by_type']['persons']:
            print(f"  Персоны: {', '.join(result['by_type']['persons'])}")

        if result['by_type']['locations']:
            print(f"  Локации: {', '.join(result['by_type']['locations'])}")

        if result['by_type']['organizations']:
            print(f"  Организации: {', '.join(result['by_type']['organizations'])}")

        # Проверяем банковские сущности
        banking_entities = [e['text'] for e in result['all'] if extractor.is_banking_entity(e['text'])]
        if banking_entities:
            print(f"  🏦 Банковские: {', '.join(banking_entities)}")

        print()


if __name__ == "__main__":
    main()
