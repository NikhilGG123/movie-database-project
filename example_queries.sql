-- Example Queries for Movie Database

-- Get all movies
SELECT movie_name, director, release_date FROM movies;

-- Get all songs in order
SELECT song_order, song_name FROM songs ORDER BY song_order;

-- Count records
SELECT 
    (SELECT COUNT(*) FROM movies) as movies,
    (SELECT COUNT(*) FROM songs) as songs,
    (SELECT COUNT(*) FROM cast_members) as cast,
    (SELECT COUNT(*) FROM commentaries) as commentaries;

-- Get languages supported
SELECT language, COUNT(*) FROM commentaries GROUP BY language ORDER BY language;

-- Get movie with cast
SELECT 
    m.movie_name,
    STRING_AGG(cm.cast_name, ', ') as cast
FROM movies m
JOIN movie_cast mc ON m.movie_id = mc.movie_id
JOIN cast_members cm ON mc.cast_id = cm.cast_id
GROUP BY m.movie_name;
