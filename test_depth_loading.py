"""
Interactive Depth Testing Script

Run this to test different depth levels with sample data
"""

import psycopg2
from load_with_depth import DepthBasedLoader
from pathlib import Path


def clear_database(db_config):
    """Clear all data from database"""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    cur.execute("TRUNCATE movies CASCADE;")
    conn.commit()
    cur.close()
    conn.close()
    print("✓ Database cleared\n")


def show_statistics(db_config):
    """Display database statistics"""
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()
    
    print("\n" + "=" * 70)
    print("DATABASE STATISTICS")
    print("=" * 70)
    
    # Overall counts
    cur.execute("""
        SELECT 
            (SELECT COUNT(*) FROM movies) as movies,
            (SELECT COUNT(*) FROM songs) as songs,
            (SELECT COUNT(*) FROM cast_members) as cast,
            (SELECT COUNT(*) FROM commentaries) as commentaries
    """)
    result = cur.fetchone()
    print(f"\n📊 Record Counts:")
    print(f"   Movies: {result[0]}")
    print(f"   Songs: {result[1]}")
    print(f"   Cast Members: {result[2]}")
    print(f"   Commentaries: {result[3]}")
    
    # Movies by year
    cur.execute("""
        SELECT 
            EXTRACT(YEAR FROM release_date) as year,
            COUNT(*) as num_movies
        FROM movies
        GROUP BY year
        ORDER BY year
    """)
    
    years = cur.fetchall()
    if years:
        print(f"\n🎬 Movies by Year:")
        for year, count in years:
            print(f"   {int(year)}: {count} movies")
    
    # Sample movies
    cur.execute("""
        SELECT movie_name, release_date, director
        FROM movies
        ORDER BY release_date
        LIMIT 5
    """)
    
    movies = cur.fetchall()
    if movies:
        print(f"\n🎥 Sample Movies:")
        for name, date, director in movies:
            print(f"   • {name} ({date.year}) - {director}")
    
    cur.close()
    conn.close()
    print("=" * 70)


def test_depth_0(db_config, base_path):
    """Test depth=0: Single movie"""
    print("\n" + "🔬 TEST 1: DEPTH=0 (Single Movie)")
    print("=" * 70)
    
    clear_database(db_config)
    
    # Update this path to your single movie folder
    movie_path = f"{base_path}/1970/Aan Milo Sajna"
    
    if not Path(movie_path).exists():
        print(f"⚠️  Path not found: {movie_path}")
        print("Please download 'Aan Milo Sajna' folder and update the path")
        return
    
    loader = DepthBasedLoader(db_config)
    loader.load_with_depth(movie_path, depth=0)
    loader.close()
    
    show_statistics(db_config)
    input("\nPress Enter to continue to Test 2...")


def test_depth_1(db_config, base_path):
    """Test depth=1: All movies from one year"""
    print("\n" + "🔬 TEST 2: DEPTH=1 (One Year)")
    print("=" * 70)
    
    clear_database(db_config)
    
    # Update this path to your year folder
    year_path = f"{base_path}/1970"
    
    if not Path(year_path).exists():
        print(f"⚠️  Path not found: {year_path}")
        print("Please download '1970' folder and update the path")
        return
    
    loader = DepthBasedLoader(db_config)
    loader.load_with_depth(year_path, depth=1)
    loader.close()
    
    show_statistics(db_config)
    input("\nPress Enter to continue to Test 3...")


def test_depth_2(db_config, base_path):
    """Test depth=2: All movies from all years"""
    print("\n" + "🔬 TEST 3: DEPTH=2 (All Years)")
    print("=" * 70)
    
    clear_database(db_config)
    
    if not Path(base_path).exists():
        print(f"⚠️  Path not found: {base_path}")
        print("Please download 'Content' folder and update the path")
        return
    
    loader = DepthBasedLoader(db_config)
    loader.load_with_depth(base_path, depth=2)
    loader.close()
    
    show_statistics(db_config)


def main():
    """Run all depth tests"""
    
    print("=" * 70)
    print("DEPTH-BASED LOADING - INTERACTIVE TEST SUITE")
    print("=" * 70)
    
    # Configuration
    db_config = {
        'host': 'localhost',
        'database': 'movie_db',
        'user': 'movie_user',
        'password': 'movie_pass_123',
        'port': 5433
    }
    
    # UPDATE THIS PATH to where you downloaded the Content folder
    base_path = "C:/Users/Owner/Desktop/Content"
    
    print(f"\nBase Path: {base_path}")
    print("\nThis test will:")
    print("1. Test depth=0 with a single movie")
    print("2. Test depth=1 with all movies from one year")
    print("3. Test depth=2 with all movies from all years")
    print("\nEach test clears the database before running.")
    
    input("\nPress Enter to start Test 1 (Depth=0)...")
    
    try:
        test_depth_0(db_config, base_path)
        test_depth_1(db_config, base_path)
        test_depth_2(db_config, base_path)
        
        print("\n" + "=" * 70)
        print("✨ ALL TESTS COMPLETED SUCCESSFULLY!")
        print("=" * 70)
        
    except Exception as e:
        print(f"\n✗ Error during testing: {e}")


if __name__ == "__main__":
    main()
