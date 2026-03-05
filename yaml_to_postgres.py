#!/usr/bin/env python3
"""
YAML to PostgreSQL Pipeline

Loads data from YAML files into PostgreSQL with automatic schema inference,
dynamic table creation/alteration, and UPSERT support.
"""

import os
import sys
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple, Set
from decimal import Decimal

import yaml
import psycopg2
from psycopg2 import sql
from psycopg2.extras import Json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class YAMLPostgresPipeline:
    """Pipeline for loading YAML data into PostgreSQL with dynamic schema management."""
    
    def __init__(self, table_name: str = "yaml_data"):
        self.table_name = table_name
        self.connection = None
        
    def connect(self) -> None:
        """Establish connection to PostgreSQL using environment variables."""
        try:
            self.connection = psycopg2.connect(
                host=os.getenv('PGHOST', 'localhost'),
                port=os.getenv('PGPORT', '5432'),
                database=os.getenv('PGDATABASE', 'postgres'),
                user=os.getenv('PGUSER', 'postgres'),
                password=os.getenv('PGPASSWORD', '')
            )
            self.connection.autocommit = False
            logger.info(f"Connected to PostgreSQL at {os.getenv('PGHOST', 'localhost')}")
        except psycopg2.Error as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def close(self) -> None:
        """Close database connection."""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def load_yaml(self, filepath: str) -> List[Dict[str, Any]]:
        """
        Load and normalize YAML file into a list of records.
        
        Args:
            filepath: Path to YAML file
            
        Returns:
            List of record dictionaries
        """
        try:
            with open(filepath, 'r') as f:
                data = yaml.safe_load(f)
            
            if data is None:
                logger.warning("Empty YAML file")
                return []
            
            # Normalize to list format
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                raise ValueError(f"Unexpected YAML structure: {type(data)}")
            
            logger.info(f"Loaded {len(records)} records from {filepath}")
            return records
            
        except Exception as e:
            logger.error(f"Failed to load YAML file: {e}")
            raise
    
    def infer_postgres_type(self, value: Any) -> str:
        """
        Infer PostgreSQL column type from Python value.
        
        Defaults to TEXT for safety when uncertain.
        """
        if value is None:
            return "TEXT"
        elif isinstance(value, bool):
            return "BOOLEAN"
        elif isinstance(value, int):
            return "BIGINT"
        elif isinstance(value, float) or isinstance(value, Decimal):
            return "DOUBLE PRECISION"
        elif isinstance(value, (dict, list)):
            return "JSONB"
        else:
            return "TEXT"
    
    def analyze_schema(self, records: List[Dict[str, Any]]) -> Dict[str, str]:
        """
        Analyze all records to determine column names and types.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Dictionary mapping column names to PostgreSQL types
        """
        schema = {}
        
        for record in records:
            for key, value in record.items():
                if key not in schema:
                    # First time seeing this key - infer type
                    schema[key] = self.infer_postgres_type(value)
                else:
                    # Already seen - check if we need to widen type
                    existing_type = schema[key]
                    new_type = self.infer_postgres_type(value)
                    
                    # If types differ, default to TEXT or JSONB
                    if existing_type != new_type:
                        if new_type == "JSONB" or existing_type == "JSONB":
                            schema[key] = "JSONB"
                        else:
                            schema[key] = "TEXT"
        
        logger.info(f"Inferred schema with {len(schema)} columns")
        return schema
    
    def ensure_id_column(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Ensure all records have an 'id' field. Generate UUID if missing.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Updated records with 'id' field
        """
        for record in records:
            if 'id' not in record:
                record['id'] = str(uuid.uuid4())
        
        return records
    
    def create_table(self, schema: Dict[str, str]) -> None:
        """
        Create table if it doesn't exist.
        
        Args:
            schema: Dictionary mapping column names to PostgreSQL types
        """
        cursor = self.connection.cursor()
        
        try:
            # Build column definitions
            columns = []
            for col_name, col_type in schema.items():
                if col_name == 'id':
                    columns.append(f'"{col_name}" TEXT PRIMARY KEY')
                else:
                    columns.append(f'"{col_name}" {col_type}')
            
            create_stmt = f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    {', '.join(columns)}
                )
            """
            
            cursor.execute(create_stmt)
            self.connection.commit()
            logger.info(f"Table '{self.table_name}' created or already exists")
            
            # Create GIN indexes on JSONB columns
            for col_name, col_type in schema.items():
                if col_type == "JSONB":
                    index_name = f"{self.table_name}_{col_name}_gin"
                    try:
                        cursor.execute(f"""
                            CREATE INDEX IF NOT EXISTS {index_name}
                            ON {self.table_name} USING GIN ("{col_name}")
                        """)
                        self.connection.commit()
                        logger.info(f"Created GIN index on {col_name}")
                    except psycopg2.Error as e:
                        logger.warning(f"Could not create index on {col_name}: {e}")
                        self.connection.rollback()
            
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to create table: {e}")
            raise
        finally:
            cursor.close()
    
    def get_existing_columns(self) -> Set[str]:
        """Get set of existing column names in the table."""
        cursor = self.connection.cursor()
        try:
            cursor.execute("""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = %s
            """, (self.table_name,))
            
            return {row[0] for row in cursor.fetchall()}
        finally:
            cursor.close()
    
    def add_missing_columns(self, schema: Dict[str, str]) -> None:
        """
        Alter table to add any missing columns from the schema.
        
        Args:
            schema: Dictionary mapping column names to PostgreSQL types
        """
        existing_cols = self.get_existing_columns()
        new_cols = set(schema.keys()) - existing_cols
        
        if not new_cols:
            logger.info("No new columns to add")
            return
        
        cursor = self.connection.cursor()
        try:
            for col_name in new_cols:
                col_type = schema[col_name]
                logger.info(f"Adding new column: {col_name} ({col_type})")
                
                cursor.execute(
                    sql.SQL("ALTER TABLE {} ADD COLUMN {} {}").format(
                        sql.Identifier(self.table_name),
                        sql.Identifier(col_name),
                        sql.SQL(col_type)
                    )
                )
                
                # Add GIN index if JSONB
                if col_type == "JSONB":
                    index_name = f"{self.table_name}_{col_name}_gin"
                    cursor.execute(f"""
                        CREATE INDEX IF NOT EXISTS {index_name}
                        ON {self.table_name} USING GIN ("{col_name}")
                    """)
            
            self.connection.commit()
            logger.info(f"Added {len(new_cols)} new columns")
            
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to add columns: {e}")
            raise
        finally:
            cursor.close()
    
    def prepare_value(self, value: Any, col_type: str) -> Any:
        """
        Prepare value for insertion based on column type.
        
        Args:
            value: Python value
            col_type: PostgreSQL column type
            
        Returns:
            Prepared value for psycopg2
        """
        if value is None:
            return None
        
        if col_type == "JSONB":
            return Json(value)
        elif col_type == "BOOLEAN":
            return bool(value)
        elif col_type == "BIGINT":
            try:
                return int(value)
            except (ValueError, TypeError):
                return None
        elif col_type == "DOUBLE PRECISION":
            try:
                return float(value)
            except (ValueError, TypeError):
                return None
        else:
            return str(value)
    
    def insert_records(self, records: List[Dict[str, Any]], schema: Dict[str, str]) -> None:
        """
        Insert or update records using UPSERT.
        
        Args:
            records: List of record dictionaries
            schema: Dictionary mapping column names to PostgreSQL types
        """
        if not records:
            logger.info("No records to insert")
            return
        
        cursor = self.connection.cursor()
        
        try:
            for record in records:
                # Prepare columns and values
                columns = []
                values = []
                
                for col_name, col_type in schema.items():
                    columns.append(col_name)
                    raw_value = record.get(col_name)
                    prepared_value = self.prepare_value(raw_value, col_type)
                    values.append(prepared_value)
                
                # Build UPSERT query
                col_identifiers = [sql.Identifier(c) for c in columns]
                placeholders = [sql.Placeholder()] * len(columns)
                
                # ON CONFLICT UPDATE all columns except id
                update_cols = [c for c in columns if c != 'id']
                update_clause = sql.SQL(', ').join([
                    sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
                    for c in update_cols
                ])
                
                query = sql.SQL("""
                    INSERT INTO {} ({})
                    VALUES ({})
                    ON CONFLICT (id) DO UPDATE SET {}
                """).format(
                    sql.Identifier(self.table_name),
                    sql.SQL(', ').join(col_identifiers),
                    sql.SQL(', ').join(placeholders),
                    update_clause
                )
                
                cursor.execute(query, values)
            
            self.connection.commit()
            logger.info(f"Successfully inserted/updated {len(records)} records")
            
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error(f"Failed to insert records: {e}")
            raise
        finally:
            cursor.close()
    
    def run(self, yaml_filepath: str) -> None:
        """
        Execute the full pipeline: load YAML, create/update table, insert records.
        
        Args:
            yaml_filepath: Path to YAML file
        """
        try:
            # Load YAML
            records = self.load_yaml(yaml_filepath)
            if not records:
                logger.warning("No records to process")
                return
            
            # Ensure id column
            records = self.ensure_id_column(records)
            
            # Analyze schema
            schema = self.analyze_schema(records)
            
            # Connect to database
            self.connect()
            
            # Create table if needed
            self.create_table(schema)
            
            # Add any missing columns
            self.add_missing_columns(schema)
            
            # Insert records
            self.insert_records(records, schema)
            
            logger.info("Pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            raise
        finally:
            self.close()


def main():
    """CLI entry point."""
    if len(sys.argv) < 2:
        print("Usage: python yaml_to_postgres.py <yaml_file> [table_name]")
        sys.exit(1)
    
    yaml_file = sys.argv[1]
    table_name = sys.argv[2] if len(sys.argv) > 2 else "yaml_data"
    
    pipeline = YAMLPostgresPipeline(table_name=table_name)
    pipeline.run(yaml_file)


if __name__ == "__main__":
    main()
