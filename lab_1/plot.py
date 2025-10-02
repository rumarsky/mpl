import matplotlib.pyplot as plt

def plot_genre_ratings(genre_to_avg, save_path=None):
    genres = list(genre_to_avg.keys())
    avgs = list(genre_to_avg.values())

    plt.figure(figsize=(10, 6))
    bars = plt.bar(genres, avgs)

    plt.title("Средний рейтинг по жанрам (чем выше, тем лучше)")
    plt.xlabel("Жанр")
    plt.ylabel("Средний рейтинг")
    plt.grid(axis="y", linestyle="--", alpha=0.4)

    plt.xticks(rotation=20)

    for bar, value in zip(bars, avgs):
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, height + 0.02, str(value),
                 ha='center', va='bottom')

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=150)

    plt.show()