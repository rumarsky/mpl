from data import movies, compute_average_rating
from plot import plot_genre_ratings

def main():
    genre_avg = compute_average_rating(movies)
    plot_genre_ratings(genre_avg)

if __name__ == "__main__":
    main()
