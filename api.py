"""
Movie Database REST API

Run with: uvicorn api:app --reload
Docs at:  http://127.0.0.1:8000/docs
"""

from contextlib import contextmanager
from datetime import date
from typing import List, Optional

import psycopg2
import psycopg2.pool
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "movie_db",
    "user": "movie_user",
    "password": "movie_pass_123",
}

pool: psycopg2.pool.SimpleConnectionPool = None


@contextmanager
def get_db():
    """Get a connection from the pool; auto-return on exit."""
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def rows_to_dicts(cur):
    """Convert cursor results to list of dicts."""
    columns = [desc[0] for desc in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


# ---------------------------------------------------------------------------
# Pydantic Models
# ---------------------------------------------------------------------------

# --- Request models ---

class MovieCreate(BaseModel):
    movie_name: str
    release_date: Optional[date] = None
    director: Optional[str] = None
    producer: Optional[str] = None
    music_director: Optional[str] = None
    lyricist: Optional[str] = None
    cast: List[str] = []


class MovieUpdate(BaseModel):
    movie_name: Optional[str] = None
    release_date: Optional[date] = None
    director: Optional[str] = None
    producer: Optional[str] = None
    music_director: Optional[str] = None
    lyricist: Optional[str] = None
    cast: Optional[List[str]] = None


class SongCreate(BaseModel):
    song_name: str
    song_order: int


class SongUpdate(BaseModel):
    song_name: Optional[str] = None
    song_order: Optional[int] = None


class CommentaryCreate(BaseModel):
    song_id: Optional[int] = None
    commentary_type: str
    language: str
    commentary_text: str


class CommentaryUpdate(BaseModel):
    commentary_type: Optional[str] = None
    language: Optional[str] = None
    commentary_text: Optional[str] = None


# --- Response models ---

class MovieResponse(BaseModel):
    movie_id: int
    movie_name: str
    release_date: Optional[date] = None
    director: Optional[str] = None
    producer: Optional[str] = None
    music_director: Optional[str] = None
    lyricist: Optional[str] = None
    cast: Optional[str] = None


class MovieDetail(MovieResponse):
    songs: list = []
    commentaries: list = []


class SongResponse(BaseModel):
    song_id: int
    movie_id: int
    song_name: str
    song_order: Optional[int] = None


class CommentaryResponse(BaseModel):
    commentary_id: int
    movie_id: int
    song_id: Optional[int] = None
    commentary_type: str
    language: str
    commentary_text: str


class StatsResponse(BaseModel):
    movies: int
    songs: int
    cast_members: int
    commentaries: int


class CastResponse(BaseModel):
    cast_id: int
    cast_name: str
    movie_count: int


class CommentarySearchResult(BaseModel):
    commentary_id: int
    movie_name: str
    subject: str
    commentary_type: str
    language: str
    commentary_text: str


class PersonCount(BaseModel):
    name: str
    movie_count: int


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Bollywood Movie Database API",
    description="REST API for querying and managing Bollywood movie data",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
def startup():
    global pool
    pool = psycopg2.pool.SimpleConnectionPool(1, 10, **DB_CONFIG)


@app.on_event("shutdown")
def shutdown():
    if pool:
        pool.closeall()


# ---------------------------------------------------------------------------
# GET endpoints
# ---------------------------------------------------------------------------

@app.get("/movies", response_model=List[MovieResponse])
def get_movies(skip: int = Query(0, ge=0), limit: int = Query(20, ge=1, le=100)):
    """List all movies with cast (paginated)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            GROUP BY m.movie_id
            ORDER BY m.movie_name
            OFFSET %s LIMIT %s
        """, (skip, limit))
        return rows_to_dicts(cur)


@app.get("/movies/{movie_id}", response_model=MovieDetail)
def get_movie(movie_id: int):
    """Get a single movie with cast, songs, and commentaries."""
    with get_db() as conn:
        cur = conn.cursor()

        # Movie + cast
        cur.execute("""
            SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE m.movie_id = %s
            GROUP BY m.movie_id
        """, (movie_id,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Movie not found")
        cols = [d[0] for d in cur.description]
        movie = dict(zip(cols, row))

        # Songs
        cur.execute("""
            SELECT song_id, movie_id, song_name, song_order
            FROM songs WHERE movie_id = %s ORDER BY song_order
        """, (movie_id,))
        movie["songs"] = rows_to_dicts(cur)

        # Commentaries
        cur.execute("""
            SELECT commentary_id, movie_id, song_id,
                   commentary_type, language, commentary_text
            FROM commentaries WHERE movie_id = %s
        """, (movie_id,))
        movie["commentaries"] = rows_to_dicts(cur)

        return movie


@app.get("/movies/{movie_id}/songs", response_model=List[SongResponse])
def get_movie_songs(movie_id: int):
    """Get all songs for a movie, ordered."""
    with get_db() as conn:
        cur = conn.cursor()
        # Verify movie exists
        cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Movie not found")

        cur.execute("""
            SELECT song_id, movie_id, song_name, song_order
            FROM songs WHERE movie_id = %s ORDER BY song_order
        """, (movie_id,))
        return rows_to_dicts(cur)


@app.get("/movies/{movie_id}/commentaries", response_model=List[CommentaryResponse])
def get_movie_commentaries(
    movie_id: int,
    type: Optional[str] = Query(None, alias="type"),
    language: Optional[str] = None,
):
    """Get commentaries for a movie, with optional type/language filters."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Movie not found")

        query = """
            SELECT commentary_id, movie_id, song_id,
                   commentary_type, language, commentary_text
            FROM commentaries WHERE movie_id = %s
        """
        params: list = [movie_id]

        if type:
            query += " AND commentary_type = %s"
            params.append(type)
        if language:
            query += " AND language = %s"
            params.append(language)

        cur.execute(query, params)
        return rows_to_dicts(cur)


@app.get("/stats", response_model=StatsResponse)
def get_stats():
    """Get database statistics (row counts)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT
                (SELECT COUNT(*) FROM movies) as movies,
                (SELECT COUNT(*) FROM songs) as songs,
                (SELECT COUNT(*) FROM cast_members) as cast_members,
                (SELECT COUNT(*) FROM commentaries) as commentaries
        """)
        row = cur.fetchone()
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))


# ---------------------------------------------------------------------------
# Search endpoints
# ---------------------------------------------------------------------------

@app.get("/search/movies", response_model=List[MovieResponse])
def search_movies(
    q: str = Query(..., min_length=1, description="Search term"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Search movies by name, director, or cast member."""
    with get_db() as conn:
        cur = conn.cursor()
        pattern = f"%{q}%"
        cur.execute("""
            SELECT DISTINCT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE m.movie_name ILIKE %s
               OR m.director ILIKE %s
               OR m.movie_id IN (
                   SELECT mc2.movie_id FROM movie_cast mc2
                   JOIN cast_members cm2 ON mc2.cast_id = cm2.cast_id
                   WHERE cm2.cast_name ILIKE %s
               )
            GROUP BY m.movie_id
            ORDER BY m.movie_name
            OFFSET %s LIMIT %s
        """, (pattern, pattern, pattern, skip, limit))
        return rows_to_dicts(cur)


@app.get("/search/commentaries", response_model=List[CommentarySearchResult])
def search_commentaries(
    q: str = Query(..., min_length=1, description="Search term"),
    type: Optional[str] = Query(None, alias="type"),
    language: Optional[str] = None,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Full-text search across all commentaries."""
    with get_db() as conn:
        cur = conn.cursor()
        query = """
            SELECT c.commentary_id, m.movie_name,
                   COALESCE(s.song_name, 'Movie') as subject,
                   c.commentary_type, c.language, c.commentary_text
            FROM commentaries c
            JOIN movies m ON c.movie_id = m.movie_id
            LEFT JOIN songs s ON c.song_id = s.song_id
            WHERE c.commentary_text ILIKE %s
        """
        params: list = [f"%{q}%"]

        if type:
            query += " AND c.commentary_type = %s"
            params.append(type)
        if language:
            query += " AND c.language = %s"
            params.append(language)

        query += " ORDER BY m.movie_name OFFSET %s LIMIT %s"
        params.extend([skip, limit])

        cur.execute(query, params)
        return rows_to_dicts(cur)


# ---------------------------------------------------------------------------
# Browse-by-attribute endpoints
# ---------------------------------------------------------------------------

@app.get("/movies/by-actor", response_model=List[MovieResponse])
def get_movies_by_actor(
    name: str = Query(..., description="Actor name (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Get movies featuring a specific actor."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            JOIN movie_cast mc ON m.movie_id = mc.movie_id
            JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE m.movie_id IN (
                SELECT mc2.movie_id FROM movie_cast mc2
                JOIN cast_members cm2 ON mc2.cast_id = cm2.cast_id
                WHERE cm2.cast_name ILIKE %s
            )
            GROUP BY m.movie_id
            ORDER BY m.movie_name
            OFFSET %s LIMIT %s
        """, (f"%{name}%", skip, limit))
        return rows_to_dicts(cur)


@app.get("/movies/by-director", response_model=List[MovieResponse])
def get_movies_by_director(
    name: str = Query(..., description="Director name (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Get movies by a specific director."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE m.director ILIKE %s
            GROUP BY m.movie_id
            ORDER BY m.movie_name
            OFFSET %s LIMIT %s
        """, (f"%{name}%", skip, limit))
        return rows_to_dicts(cur)


@app.get("/movies/by-year/{year}", response_model=List[MovieResponse])
def get_movies_by_year(
    year: int,
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=100),
):
    """Get all movies from a specific year."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE EXTRACT(YEAR FROM m.release_date) = %s
            GROUP BY m.movie_id
            ORDER BY m.movie_name
            OFFSET %s LIMIT %s
        """, (year, skip, limit))
        return rows_to_dicts(cur)


# ---------------------------------------------------------------------------
# Cast & crew listing endpoints
# ---------------------------------------------------------------------------

@app.get("/cast", response_model=List[CastResponse])
def get_cast(
    q: Optional[str] = Query(None, description="Filter by name (partial match)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
):
    """List all actors with their movie count."""
    with get_db() as conn:
        cur = conn.cursor()
        query = """
            SELECT cm.cast_id, cm.cast_name, COUNT(mc.movie_id) as movie_count
            FROM cast_members cm
            LEFT JOIN movie_cast mc ON cm.cast_id = mc.cast_id
        """
        params: list = []

        if q:
            query += " WHERE cm.cast_name ILIKE %s"
            params.append(f"%{q}%")

        query += " GROUP BY cm.cast_id ORDER BY cm.cast_name OFFSET %s LIMIT %s"
        params.extend([skip, limit])

        cur.execute(query, params)
        return rows_to_dicts(cur)


@app.get("/directors", response_model=List[PersonCount])
def get_directors():
    """List all directors with their movie count."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT director as name, COUNT(*) as movie_count
            FROM movies
            WHERE director IS NOT NULL
            GROUP BY director
            ORDER BY movie_count DESC, director
        """)
        return rows_to_dicts(cur)


# ---------------------------------------------------------------------------
# Song-level commentary endpoint
# ---------------------------------------------------------------------------

@app.get("/songs/{song_id}/commentaries", response_model=List[CommentaryResponse])
def get_song_commentaries(
    song_id: int,
    type: Optional[str] = Query(None, alias="type"),
    language: Optional[str] = None,
):
    """Get commentaries for a specific song."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM songs WHERE song_id = %s", (song_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Song not found")

        query = """
            SELECT commentary_id, movie_id, song_id,
                   commentary_type, language, commentary_text
            FROM commentaries WHERE song_id = %s
        """
        params: list = [song_id]

        if type:
            query += " AND commentary_type = %s"
            params.append(type)
        if language:
            query += " AND language = %s"
            params.append(language)

        cur.execute(query, params)
        return rows_to_dicts(cur)


# ---------------------------------------------------------------------------
# POST endpoints
# ---------------------------------------------------------------------------

@app.post("/movies", response_model=MovieResponse, status_code=201)
def create_movie(movie: MovieCreate):
    """Create a new movie with optional cast list."""
    with get_db() as conn:
        cur = conn.cursor()
        try:
            cur.execute("""
                INSERT INTO movies (movie_name, release_date, director, producer, music_director, lyricist)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING movie_id
            """, (movie.movie_name, movie.release_date, movie.director,
                  movie.producer, movie.music_director, movie.lyricist))
            movie_id = cur.fetchone()[0]

            # Insert cast
            for name in movie.cast:
                cur.execute("INSERT INTO cast_members (cast_name) VALUES (%s) ON CONFLICT (cast_name) DO NOTHING", (name,))
                cur.execute("SELECT cast_id FROM cast_members WHERE cast_name = %s", (name,))
                cast_id = cur.fetchone()[0]
                cur.execute("INSERT INTO movie_cast (movie_id, cast_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (movie_id, cast_id))

            conn.commit()

            # Return the created movie
            cur.execute("""
                SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                       m.producer, m.music_director, m.lyricist,
                       STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
                FROM movies m
                LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
                LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
                WHERE m.movie_id = %s
                GROUP BY m.movie_id
            """, (movie_id,))
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, cur.fetchone()))

        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(400, f"Movie '{movie.movie_name}' already exists")
        except Exception:
            conn.rollback()
            raise


@app.post("/movies/{movie_id}/songs", response_model=SongResponse, status_code=201)
def create_song(movie_id: int, song: SongCreate):
    """Add a song to a movie."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Movie not found")

        try:
            cur.execute("""
                INSERT INTO songs (movie_id, song_name, song_order)
                VALUES (%s, %s, %s)
                RETURNING song_id, movie_id, song_name, song_order
            """, (movie_id, song.song_name, song.song_order))
            row = cur.fetchone()
            conn.commit()
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))

        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            raise HTTPException(400, f"Song '{song.song_name}' already exists for this movie")
        except Exception:
            conn.rollback()
            raise


@app.post("/movies/{movie_id}/commentaries", response_model=CommentaryResponse, status_code=201)
def create_commentary(movie_id: int, commentary: CommentaryCreate):
    """Add commentary to a movie (or to a specific song if song_id provided)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Movie not found")

        if commentary.song_id:
            cur.execute("SELECT 1 FROM songs WHERE song_id = %s AND movie_id = %s",
                        (commentary.song_id, movie_id))
            if not cur.fetchone():
                raise HTTPException(404, "Song not found for this movie")

        try:
            cur.execute("""
                INSERT INTO commentaries (movie_id, song_id, commentary_type, language, commentary_text)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING commentary_id, movie_id, song_id, commentary_type, language, commentary_text
            """, (movie_id, commentary.song_id, commentary.commentary_type,
                  commentary.language, commentary.commentary_text))
            row = cur.fetchone()
            conn.commit()
            cols = [d[0] for d in cur.description]
            return dict(zip(cols, row))
        except Exception:
            conn.rollback()
            raise


# ---------------------------------------------------------------------------
# PUT endpoints
# ---------------------------------------------------------------------------

@app.put("/movies/{movie_id}", response_model=MovieResponse)
def update_movie(movie_id: int, movie: MovieUpdate):
    """Update a movie's fields. Only provided (non-None) fields are updated."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM movies WHERE movie_id = %s", (movie_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Movie not found")

        # Build dynamic UPDATE
        fields = {}
        for field in ["movie_name", "release_date", "director", "producer", "music_director", "lyricist"]:
            value = getattr(movie, field)
            if value is not None:
                fields[field] = value

        if fields:
            set_clause = ", ".join(f"{k} = %s" for k in fields)
            values = list(fields.values()) + [movie_id]
            try:
                cur.execute(f"UPDATE movies SET {set_clause} WHERE movie_id = %s", values)
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                raise HTTPException(400, f"Movie name '{movie.movie_name}' already exists")
            except Exception:
                conn.rollback()
                raise

        # Update cast if provided
        if movie.cast is not None:
            cur.execute("DELETE FROM movie_cast WHERE movie_id = %s", (movie_id,))
            for name in movie.cast:
                cur.execute("INSERT INTO cast_members (cast_name) VALUES (%s) ON CONFLICT (cast_name) DO NOTHING", (name,))
                cur.execute("SELECT cast_id FROM cast_members WHERE cast_name = %s", (name,))
                cast_id = cur.fetchone()[0]
                cur.execute("INSERT INTO movie_cast (movie_id, cast_id) VALUES (%s, %s)", (movie_id, cast_id))

        conn.commit()

        # Return updated movie
        cur.execute("""
            SELECT m.movie_id, m.movie_name, m.release_date, m.director,
                   m.producer, m.music_director, m.lyricist,
                   STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE m.movie_id = %s
            GROUP BY m.movie_id
        """, (movie_id,))
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchone()))


@app.put("/songs/{song_id}", response_model=SongResponse)
def update_song(song_id: int, song: SongUpdate):
    """Update a song's name or order."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM songs WHERE song_id = %s", (song_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Song not found")

        fields = {}
        if song.song_name is not None:
            fields["song_name"] = song.song_name
        if song.song_order is not None:
            fields["song_order"] = song.song_order

        if fields:
            set_clause = ", ".join(f"{k} = %s" for k in fields)
            values = list(fields.values()) + [song_id]
            try:
                cur.execute(f"UPDATE songs SET {set_clause} WHERE song_id = %s", values)
                conn.commit()
            except psycopg2.errors.UniqueViolation:
                conn.rollback()
                raise HTTPException(400, "Song name already exists for this movie")
            except Exception:
                conn.rollback()
                raise

        cur.execute("SELECT song_id, movie_id, song_name, song_order FROM songs WHERE song_id = %s", (song_id,))
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchone()))


@app.put("/commentaries/{commentary_id}", response_model=CommentaryResponse)
def update_commentary(commentary_id: int, commentary: CommentaryUpdate):
    """Update a commentary's type, language, or text."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM commentaries WHERE commentary_id = %s", (commentary_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Commentary not found")

        fields = {}
        for field in ["commentary_type", "language", "commentary_text"]:
            value = getattr(commentary, field)
            if value is not None:
                fields[field] = value

        if fields:
            set_clause = ", ".join(f"{k} = %s" for k in fields)
            values = list(fields.values()) + [commentary_id]
            cur.execute(f"UPDATE commentaries SET {set_clause} WHERE commentary_id = %s", values)
            conn.commit()

        cur.execute("""
            SELECT commentary_id, movie_id, song_id, commentary_type, language, commentary_text
            FROM commentaries WHERE commentary_id = %s
        """, (commentary_id,))
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchone()))


# ---------------------------------------------------------------------------
# DELETE endpoints
# ---------------------------------------------------------------------------

@app.delete("/movies/{movie_id}")
def delete_movie(movie_id: int):
    """Delete a movie and all related data (cascade)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM movies WHERE movie_id = %s RETURNING movie_id", (movie_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Movie not found")
        conn.commit()
        return {"message": "Movie deleted", "movie_id": movie_id}


@app.delete("/songs/{song_id}")
def delete_song(song_id: int):
    """Delete a song and its commentaries (cascade)."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM songs WHERE song_id = %s RETURNING song_id", (song_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Song not found")
        conn.commit()
        return {"message": "Song deleted", "song_id": song_id}


@app.delete("/commentaries/{commentary_id}")
def delete_commentary(commentary_id: int):
    """Delete a commentary."""
    with get_db() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM commentaries WHERE commentary_id = %s RETURNING commentary_id", (commentary_id,))
        if not cur.fetchone():
            raise HTTPException(404, "Commentary not found")
        conn.commit()
        return {"message": "Commentary deleted", "commentary_id": commentary_id}
