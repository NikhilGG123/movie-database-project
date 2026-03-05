"""
Enhanced YAML to PostgreSQL Loader with Directory Depth Control

USAGE:
    python load_with_depth.py 0    # Load single movie
    python load_with_depth.py 1    # Load all movies from 1970
    python load_with_depth.py 2    # Load all movies from all years
"""

import yaml
import psycopg2
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any


class DepthBasedLoader:
    
    def __init__(self, db_config: Dict[str, str]):
        self.conn = psycopg2.connect(**db_config)
        self.cur = self.conn.cursor()
        self.files_processed = 0
        self.movies_created = set()
    
    def find_yaml_files(self, base_path: str, depth: int) -> List[Path]:
        base = Path(base_path)
        yaml_files = []
        
        if depth == 0:
            yaml_files = list(base.glob("*.yaml"))
            yaml_files.extend(list(base.glob("*/*.yaml")))
        elif depth == 1:
            for subdir in base.iterdir():
                if subdir.is_dir():
                    yaml_files.extend(subdir.glob("*.yaml"))
                    yaml_files.extend(subdir.glob("*/*.yaml"))
        elif depth == 2:
            for year_dir in base.iterdir():
                if year_dir.is_dir():
                    for movie_dir in year_dir.iterdir():
                        if movie_dir.is_dir():
                            yaml_files.extend(movie_dir.glob("*.yaml"))
                            yaml_files.extend(movie_dir.glob("*/*.yaml"))
        
        return sorted(yaml_files)
    
    def load_yaml_file(self, filepath: Path) -> None:
        with open(filepath, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
        
        metadata = data['metadata']
        movie_name = metadata['movie_name']
        release_date = datetime.strptime(metadata['release_date'], '%d %B %Y').date()
        
        self.cur.execute("""
            INSERT INTO movies (movie_name, release_date, director, producer, music_director, lyricist)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (movie_name) DO NOTHING
            RETURNING movie_id
        """, (movie_name, release_date, metadata['director'], metadata['producer'], 
              metadata['music_director'], metadata['lyricist']))
        
        result = self.cur.fetchone()
        if result:
            movie_id = result[0]
            self.movies_created.add(movie_name)
            print(f"  ✓ Movie: {movie_name} (ID: {movie_id})")
        else:
            self.cur.execute("SELECT movie_id FROM movies WHERE movie_name = %s", (movie_name,))
            movie_id = self.cur.fetchone()[0]
        
        for cast_name in metadata['cast']:
            self.cur.execute("INSERT INTO cast_members (cast_name) VALUES (%s) ON CONFLICT (cast_name) DO NOTHING", (cast_name,))
            self.cur.execute("SELECT cast_id FROM cast_members WHERE cast_name = %s", (cast_name,))
            cast_id = self.cur.fetchone()[0]
            self.cur.execute("INSERT INTO movie_cast (movie_id, cast_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (movie_id, cast_id))
        
        song_mapping = {}
        for idx, song_name in enumerate(data['songs_order'], start=1):
            self.cur.execute("""
                INSERT INTO songs (movie_id, song_name, song_order)
                VALUES (%s, %s, %s)
                ON CONFLICT (movie_id, song_name) DO UPDATE SET song_order = EXCLUDED.song_order
                RETURNING song_id
            """, (movie_id, song_name, idx))
            song_id = self.cur.fetchone()[0]
            song_mapping[song_name] = song_id
        
        for language, commentary_data in data['commentaries'].items():
            movie_commentary_key = list(commentary_data.keys())[0]
            movie_commentary = commentary_data[movie_commentary_key]
            
            self.cur.execute("""
                INSERT INTO commentaries (movie_id, song_id, commentary_type, language, commentary_text)
                VALUES (%s, NULL, %s, %s, %s)
            """, (movie_id, data['commentary_type'], language, movie_commentary))
            
            for song_name, song_id in song_mapping.items():
                if song_name in commentary_data:
                    self.cur.execute("""
                        INSERT INTO commentaries (movie_id, song_id, commentary_type, language, commentary_text)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (movie_id, song_id, data['commentary_type'], language, commentary_data[song_name]))
        
        self.conn.commit()
        self.files_processed += 1
    
    def load_with_depth(self, base_path: str, depth: int) -> None:
        print("=" * 70)
        print(f"LOADING YAML DATA WITH DEPTH={depth}")
        print(f"Base Path: {base_path}")
        print("=" * 70)
        
        yaml_files = self.find_yaml_files(base_path, depth)
        
        if not yaml_files:
            print(f"\nNo YAML files found at depth {depth}")
            return
        
        print(f"\nFound {len(yaml_files)} YAML files\n")
        
        for filepath in yaml_files:
            try:
                rel_path = filepath.relative_to(base_path) if Path(base_path) in filepath.parents else filepath.name
                print(f"Processing: {rel_path}")
                self.load_yaml_file(filepath)
            except Exception as e:
                print(f"  ✗ Error: {e}")
        
        print("\n" + "=" * 70)
        print(f"✓ Processed {self.files_processed} YAML files")
        print(f"✓ Loaded {len(self.movies_created)} unique movies")
        print("=" * 70)
    
    def get_statistics(self) -> Dict[str, int]:
        self.cur.execute("""
            SELECT 
                (SELECT COUNT(*) FROM movies) as movies,
                (SELECT COUNT(*) FROM songs) as songs,
                (SELECT COUNT(*) FROM cast_members) as cast,
                (SELECT COUNT(*) FROM commentaries) as commentaries
        """)
        result = self.cur.fetchone()
        return {'movies': result[0], 'songs': result[1], 'cast': result[2], 'commentaries': result[3]}
    
    def close(self) -> None:
        self.cur.close()
        self.conn.close()


def main():
    # Check for command line argument
    if len(sys.argv) != 2:
        print("Usage: python load_with_depth.py <depth>")
        print("  depth=0: Load single movie (Aan Milo Sajna)")
        print("  depth=1: Load all movies from 1970")
        print("  depth=2: Load all movies from all years")
        sys.exit(1)
    
    try:
        depth = int(sys.argv[1])
        if depth not in [0, 1, 2]:
            print("Error: depth must be 0, 1, or 2")
            sys.exit(1)
    except ValueError:
        print("Error: depth must be an integer (0, 1, or 2)")
        sys.exit(1)
    
    # Set base path based on depth
    if depth == 0:
        base_path = 'C:/Users/Owner/Desktop/Content/1970/Aan Milo Sajna'
    elif depth == 1:
        base_path = 'C:/Users/Owner/Desktop/Content/1970'
    else:  # depth == 2
        base_path = 'C:/Users/Owner/Desktop/Content'
    
    db_config = {
        'host': 'localhost',
        'database': 'movie_db',
        'user': 'movie_user',
        'password': 'movie_pass_123',
        'port': 5433
    }
    
    loader = DepthBasedLoader(db_config)
    
    try:
        loader.load_with_depth(base_path, depth)
        
        stats = loader.get_statistics()
        print("\n📊 Database Statistics:")
        print(f"   Movies: {stats['movies']}")
        print(f"   Songs: {stats['songs']}")
        print(f"   Cast Members: {stats['cast']}")
        print(f"   Commentaries: {stats['commentaries']}")
    except Exception as e:
        print(f"\n✗ Error: {e}")
        loader.conn.rollback()
    finally:
        loader.close()


if __name__ == "__main__":
    main()
