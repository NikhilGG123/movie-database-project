-- PostgreSQL Database Setup for Movie YAML Data
-- Run this script to create the database and user

-- Connect to PostgreSQL as admin and run:
-- CREATE DATABASE movie_db;
-- CREATE USER movie_user WITH PASSWORD 'your_secure_password';
-- GRANT ALL PRIVILEGES ON DATABASE movie_db TO movie_user;

-- Then connect to movie_db and create tables:

-- Movies table stores core movie metadata
CREATE TABLE IF NOT EXISTS movies (
    movie_id SERIAL PRIMARY KEY,
    movie_name VARCHAR(255) NOT NULL,
    release_date DATE,
    director VARCHAR(255),
    producer VARCHAR(255),
    music_director VARCHAR(255),
    lyricist VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cast members table
CREATE TABLE IF NOT EXISTS cast_members (
    cast_id SERIAL PRIMARY KEY,
    cast_name VARCHAR(255) UNIQUE NOT NULL
);

-- Junction table for movie-cast relationship
CREATE TABLE IF NOT EXISTS movie_cast (
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    cast_id INTEGER REFERENCES cast_members(cast_id) ON DELETE CASCADE,
    PRIMARY KEY (movie_id, cast_id)
);

-- Songs table
CREATE TABLE IF NOT EXISTS songs (
    song_id SERIAL PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    song_name VARCHAR(255) NOT NULL,
    song_order INTEGER,
    UNIQUE(movie_id, song_name)
);

-- Commentaries table for both movies and songs
CREATE TABLE IF NOT EXISTS commentaries (
    commentary_id SERIAL PRIMARY KEY,
    movie_id INTEGER REFERENCES movies(movie_id) ON DELETE CASCADE,
    song_id INTEGER REFERENCES songs(song_id) ON DELETE CASCADE,
    commentary_type VARCHAR(50) NOT NULL,
    language VARCHAR(50) NOT NULL,
    commentary_text TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (
        (song_id IS NULL AND commentary_text IS NOT NULL) OR
        (song_id IS NOT NULL)
    )
);

-- Indexes for better query performance
CREATE INDEX idx_movies_name ON movies(movie_name);
CREATE INDEX idx_songs_movie ON songs(movie_id);
CREATE INDEX idx_commentaries_movie ON commentaries(movie_id);
CREATE INDEX idx_commentaries_song ON commentaries(song_id);
CREATE INDEX idx_commentaries_type ON commentaries(commentary_type);

-- Grant permissions to movie_user
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO movie_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO movie_user;
