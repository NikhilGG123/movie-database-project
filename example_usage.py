"""
Example usage of the movie database
Demonstrates common query patterns and data retrieval
"""

import psycopg2
from typing import List, Dict, Optional


class MovieDatabase:
    """Simple wrapper for movie database queries"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
    
    def get_all_movies(self) -> List[Dict]:
        """Get all movies with basic info"""
        query = """
            SELECT 
                m.movie_id,
                m.movie_name,
                m.release_date,
                m.director,
                STRING_AGG(cm.cast_name, ', ' ORDER BY cm.cast_name) as cast
            FROM movies m
            LEFT JOIN movie_cast mc ON m.movie_id = mc.movie_id
            LEFT JOIN cast_members cm ON mc.cast_id = cm.cast_id
            GROUP BY m.movie_id
        """
        
        self.cur.execute(query)
        columns = [desc[0] for desc in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur.fetchall()]
    
    def get_movie_songs(self, movie_name: str) -> List[Dict]:
        """Get all songs for a movie in order"""
        query = """
            SELECT s.song_name, s.song_order
            FROM songs s
            JOIN movies m ON s.movie_id = m.movie_id
            WHERE m.movie_name = %s
            ORDER BY s.song_order
        """
        
        self.cur.execute(query, (movie_name,))
        columns = [desc[0] for desc in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur.fetchall()]
    
    def get_movie_commentary(
        self, 
        movie_name: str, 
        commentary_type: str = 'long',
        language: str = 'Hindi'
    ) -> Optional[str]:
        """Get movie-level commentary"""
        query = """
            SELECT c.commentary_text
            FROM commentaries c
            JOIN movies m ON c.movie_id = m.movie_id
            WHERE m.movie_name = %s
              AND c.song_id IS NULL
              AND c.commentary_type = %s
              AND c.language = %s
        """
        
        self.cur.execute(query, (movie_name, commentary_type, language))
        result = self.cur.fetchone()
        return result[0] if result else None
    
    def get_song_commentary(
        self,
        song_name: str,
        commentary_type: str = 'long',
        language: str = 'Hindi'
    ) -> Optional[str]:
        """Get commentary for a specific song"""
        query = """
            SELECT c.commentary_text
            FROM commentaries c
            JOIN songs s ON c.song_id = s.song_id
            WHERE s.song_name = %s
              AND c.commentary_type = %s
              AND c.language = %s
        """
        
        self.cur.execute(query, (song_name, commentary_type, language))
        result = self.cur.fetchone()
        return result[0] if result else None
    
    def search_commentaries(self, keyword: str) -> List[Dict]:
        """Search for a keyword across all commentaries"""
        query = """
            SELECT 
                m.movie_name,
                CASE 
                    WHEN c.song_id IS NULL THEN 'Movie'
                    ELSE s.song_name 
                END as subject,
                c.commentary_type,
                c.language,
                c.commentary_text
            FROM commentaries c
            JOIN movies m ON c.movie_id = m.movie_id
            LEFT JOIN songs s ON c.song_id = s.song_id
            WHERE c.commentary_text ILIKE %s
        """
        
        self.cur.execute(query, (f'%{keyword}%',))
        columns = [desc[0] for desc in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur.fetchall()]
    
    def get_movies_by_actor(self, actor_name: str) -> List[Dict]:
        """Find all movies featuring a specific actor"""
        query = """
            SELECT DISTINCT
                m.movie_name,
                m.release_date,
                m.director
            FROM movies m
            JOIN movie_cast mc ON m.movie_id = mc.movie_id
            JOIN cast_members cm ON mc.cast_id = cm.cast_id
            WHERE cm.cast_name ILIKE %s
            ORDER BY m.release_date DESC
        """
        
        self.cur.execute(query, (f'%{actor_name}%',))
        columns = [desc[0] for desc in self.cur.description]
        return [dict(zip(columns, row)) for row in self.cur.fetchall()]
    
    def get_complete_movie_data(self, movie_name: str) -> Dict:
        """Get all data for a movie in one structured response"""
        
        # Get movie info
        self.cur.execute(
            "SELECT * FROM movies WHERE movie_name = %s",
            (movie_name,)
        )
        movie_columns = [desc[0] for desc in self.cur.description]
        movie_data = dict(zip(movie_columns, self.cur.fetchone() or []))
        
        if not movie_data:
            return {}
        
        # Add cast, songs, and commentaries
        movie_data['cast'] = [
            row[0] for row in self.cur.execute("""
                SELECT cm.cast_name
                FROM cast_members cm
                JOIN movie_cast mc ON cm.cast_id = mc.cast_id
                WHERE mc.movie_id = %s
            """, (movie_data['movie_id'],)) or []
        ]
        
        movie_data['songs'] = self.get_movie_songs(movie_name)
        movie_data['commentary'] = self.get_movie_commentary(movie_name)
        
        return movie_data
    
    def close(self):
        """Close database connection"""
        self.cur.close()
        self.conn.close()


def main():
    """Example usage"""
    
    # Database configuration
    db_config = {
        'host': 'localhost',
        'database': 'movie_db',
        'user': 'movie_user',
        'password': 'movie_pass_123',  # UPDATE THIS
        'port': 5433  # Updated to 5433
    }
    
    db = MovieDatabase(db_config)
    
    try:
        # Example 1: Get all movies
        print("=" * 60)
        print("All Movies:")
        print("=" * 60)
        movies = db.get_all_movies()
        for movie in movies:
            print(f"\n{movie['movie_name']} ({movie['release_date']})")
            print(f"Director: {movie['director']}")
            print(f"Cast: {movie['cast']}")
        
        # Example 2: Get songs for a movie
        print("\n" + "=" * 60)
        print("Songs from 'Aan Milo Sajna':")
        print("=" * 60)
        songs = db.get_movie_songs('Aan Milo Sajna')
        for song in songs:
            print(f"{song['song_order']}. {song['song_name']}")
        
        # Example 3: Get movie commentary
        print("\n" + "=" * 60)
        print("Movie Commentary (Long):")
        print("=" * 60)
        commentary = db.get_movie_commentary('Aan Milo Sajna', 'long')
        if commentary:
            print(commentary[:200] + "..." if len(commentary) > 200 else commentary)
        
        # Example 4: Get song commentary
        print("\n" + "=" * 60)
        print("Song Commentary:")
        print("=" * 60)
        song_commentary = db.get_song_commentary('Achha To Hum Chalte Hain', 'short')
        if song_commentary:
            print(song_commentary)
        
        # Example 5: Search commentaries
        print("\n" + "=" * 60)
        print("Search results for 'किशोर कुमार':")
        print("=" * 60)
        results = db.search_commentaries('किशोर कुमार')
        for result in results[:3]:  # Show first 3 results
            print(f"\nMovie: {result['movie_name']}")
            print(f"Subject: {result['subject']}")
            print(f"Type: {result['commentary_type']}")
            preview = result['commentary_text'][:100]
            print(f"Preview: {preview}...")
        
        # Example 6: Find movies by actor
        print("\n" + "=" * 60)
        print("Movies featuring Rajesh Khanna:")
        print("=" * 60)
        actor_movies = db.get_movies_by_actor('Rajesh Khanna')
        for movie in actor_movies:
            print(f"- {movie['movie_name']} ({movie['release_date']}) - Dir: {movie['director']}")
        
    except Exception as e:
        print(f"Error: {e}")
    
    finally:
        db.close()


if __name__ == "__main__":
    main()
