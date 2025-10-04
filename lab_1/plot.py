import matplotlib.pyplot as plt

def plot_genre_ratings(genre_to_avg):
    genres = list(genre_to_avg.keys())
    avgs = list(genre_to_avg.values())

    plt.figure(figsize=(10, 6))
    bars = plt.bar(genres, avgs)

    plt.title("Средний рейтинг по жанрам (чем выше, тем лучше)")
    plt.xlabel("Жанр")
    plt.ylabel("Средний рейтинг")
    plt.grid(axis="y", linestyle="--")

    plt.xticks(rotation=20)

    for bar, value in zip(bars, avgs):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height, str(value),
                 ha='center', va='bottom')

    plt.tight_layout()

    plt.show()