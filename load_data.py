"""
Simple YAML to PostgreSQL loader - ready to run
"""

import yaml
import psycopg2
from pathlib import Path
from datetime import datetime


db_config = {
    'host': 'localhost',
    'database': 'movie_db',
    'user': 'movie_user',
    'password': 'movie_pass_123',
    'port': 5433
}

yaml_directory = './yaml_files'


def load_yaml_file(filepath, conn):
    """Load a single YAML file into database"""
    
    print(f"\nProcessing: {Path(filepath).name}")
    
    with open(filepath, 'r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    
    cur = conn.cursor()
    
    # Insert movie
    release_date = datetime.strptime(data['metadata']['release_date'], '%d %B %Y').date()
    
    cur.execute("""
        INSERT INTO movies (movie_name, release_date, director, producer, music_director, lyricist)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING movie_id
    """, (
        data['metadata']['movie_name'],
        release_date,
        data['metadata']['director'],
        data['metadata']['producer'],
        data['metadata']['music_director'],
        data['metadata']['lyricist']
    ))
    
    result = cur.fetchone()
    if result:
        movie_id = result[0]
    else:
        cur.execute("SELECT movie_id FROM movies WHERE movie_name = %s", (data['metadata']['movie_name'],))
        movie_id = cur.fetchone()[0]
    
    print(f"  ✓ Movie inserted (ID: {movie_id})")
    
    # Insert cast
    for cast_name in data['metadata']['cast']:
        cur.execute("INSERT INTO cast_members (cast_name) VALUES (%s) ON CONFLICT DO NOTHING", (cast_name,))
        cur.execute("SELECT cast_id FROM cast_members WHERE cast_name = %s", (cast_name,))
        cast_id = cur.fetchone()[0]
        cur.execute("INSERT INTO movie_cast (movie_id, cast_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (movie_id, cast_id))
    
    print(f"  ✓ Cast members inserted ({len(data['metadata']['cast'])} actors)")
    
    # Insert songs
    song_mapping = {}
    for idx, song_name in enumerate(data['songs_order'], start=1):
        cur.execute("""
            INSERT INTO songs (movie_id, song_name, song_order)
            VALUES (%s, %s, %s)
            ON CONFLICT (movie_id, song_name) DO UPDATE SET song_order = EXCLUDED.song_order
            RETURNING song_id
        """, (movie_id, song_name, idx))
        song_id = cur.fetchone()[0]
        song_mapping[song_name] = song_id
    
    print(f"  ✓ Songs inserted ({len(song_mapping)} songs)")
    
    # Insert commentaries
    for language, commentary_data in data['commentaries'].items():
        # Movie commentary
        movie_commentary_key = list(commentary_data.keys())[0]
        movie_commentary = commentary_data[movie_commentary_key]
        
        cur.execute("""
            INSERT INTO commentaries (movie_id, song_id, commentary_type, language, commentary_text)
            VALUES (%s, NULL, %s, %s, %s)
        """, (movie_id, data['commentary_type'], language, movie_commentary))
        
        # Song commentaries
        for song_name, song_id in song_mapping.items():
            if song_name in commentary_data:
                cur.execute("""
                    INSERT INTO commentaries (movie_id, song_id, commentary_type, language, commentary_text)
                    VALUES (%s, %s, %s, %s, %s)
                """, (movie_id, song_id, data['commentary_type'], language, commentary_data[song_name]))
    
    print(f"  ✓ Commentaries inserted (type: {data['commentary_type']})")
    
    conn.commit()
    cur.close()


def main():
    print("=" * 60)
    print("Loading YAML Data into PostgreSQL")
    print("=" * 60)
    
    # Connect to database
    conn = psycopg2.connect(**db_config)
    
    # Find all YAML files
    yaml_files = list(Path(yaml_directory).glob("*.yaml"))
    print(f"\nFound {len(yaml_files)} YAML files")
    
    # Load each file
    for filepath in yaml_files:
        try:
            load_yaml_file(str(filepath), conn)
        except Exception as e:
            print(f"  ✗ Error: {e}")
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("✓ All data loaded successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
