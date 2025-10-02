from typing import List, Dict, Union

movies = [
    {"title": "Побег из Шоушенка", "genre": "Драма", "rating": 9.3},
    {"title": "Зелёная миля", "genre": "Драма", "rating": 9.0},
    {"title": "Форрест Гамп", "genre": "Драма", "rating": 8.8},
    {"title": "Начало", "genre": "Фантастика", "rating": 8.7},
    {"title": "Интерстеллар", "genre": "Фантастика", "rating": 8.6},
    {"title": "Матрица", "genre": "Фантастика", "rating": 8.7},
    {"title": "Тёмный рыцарь", "genre": "Боевик", "rating": 9.0},
    {"title": "Безумный Макс: Дорога ярости", "genre": "Боевик", "rating": 8.1},
    {"title": "Джон Уик", "genre": "Боевик", "rating": 7.4},
    {"title": "1+1", "genre": "Комедия", "rating": 8.8},
    {"title": "Операция «Ы»", "genre": "Комедия", "rating": 8.5},
    {"title": "Охотники за привидениями", "genre": "Комедия", "rating": 7.8},
    {"title": "Семь", "genre": "Триллер", "rating": 8.6},
    {"title": "Бойцовский клуб", "genre": "Триллер", "rating": 8.8},
    {"title": "Джокер", "genre": "Триллер", "rating": 8.4},
]


def compute_average_rating(
    movies: List[Dict[str, Union[str, float]]],
) -> Dict[str, float]:
    sums = {}
    counts = {}
    for movie in movies:
        genre = movie["genre"]
        rating = movie["rating"]
        if isinstance(rating, str):
            rating = float(rating)
        if genre not in sums:
            sums[genre] = 0.0
            counts[genre] = 0
        sums[genre] += rating
        counts[genre] += 1

    averages = {str(genre): sums[genre] / counts[genre] for genre in sums}
    averages_sorted = sorted(averages.items(), key=lambda item: item[1], reverse=True)
    return dict(averages_sorted)
