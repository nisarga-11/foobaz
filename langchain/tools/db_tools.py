import psycopg2
import psycopg2.extras
from typing import Dict, List, Optional, Any
from config.settings import settings

class DatabaseTools:
    """Database utility tools for connection and integrity checks."""
    
    @staticmethod
    def test_connection(db_config: Dict[str, Any]) -> Dict[str, Any]:
        """Test database connection and return status."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            # Test basic connectivity
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            
            # Get database size
            cursor.execute("""
                SELECT pg_size_pretty(pg_database_size(current_database()));
            """)
            size = cursor.fetchone()[0]
            
            # Get table count
            cursor.execute("""
                SELECT COUNT(*) FROM information_schema.tables 
                WHERE table_schema = 'public';
            """)
            table_count = cursor.fetchone()[0]
            
            cursor.close()
            conn.close()
            
            return {
                "status": "connected",
                "version": version,
                "size": size,
                "table_count": table_count,
                "error": None
            }
            
        except Exception as e:
            return {
                "status": "error",
                "version": None,
                "size": None,
                "table_count": None,
                "error": str(e)
            }
    

    
    @staticmethod
    def get_table_relationships(db_config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Get foreign key relationships between tables."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    tc.table_name, 
                    kcu.column_name, 
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name,
                    tc.constraint_name
                FROM 
                    information_schema.table_constraints AS tc 
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                      AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                      AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' 
                AND tc.table_schema = 'public'
                ORDER BY tc.table_name, kcu.column_name;
            """)
            
            relationships = []
            for row in cursor.fetchall():
                relationships.append({
                    "table_name": row[0],
                    "column_name": row[1],
                    "foreign_table_name": row[2],
                    "foreign_column_name": row[3],
                    "constraint_name": row[4]
                })
            
            cursor.close()
            conn.close()
            
            return relationships
            
        except Exception as e:
            return []
    
    @staticmethod
    def get_database_info(db_config: Dict[str, Any]) -> Dict[str, Any]:
        """Get comprehensive database information."""
        try:
            conn = psycopg2.connect(**db_config)
            cursor = conn.cursor()
            
            # Get database name
            cursor.execute("SELECT current_database();")
            db_name = cursor.fetchone()[0]
            
            # Get table information
            cursor.execute("""
                SELECT 
                    table_name,
                    pg_size_pretty(pg_total_relation_size(quote_ident(table_name)::regclass)) as size,
                    (SELECT COUNT(*) FROM information_schema.columns WHERE table_name = t.table_name) as column_count
                FROM information_schema.tables t
                WHERE table_schema = 'public'
                AND table_type = 'BASE TABLE'
                ORDER BY table_name;
            """)
            tables = cursor.fetchall()
            
            # Get index information
            cursor.execute("""
                SELECT 
                    indexname,
                    tablename,
                    indexdef
                FROM pg_indexes
                WHERE schemaname = 'public'
                ORDER BY tablename, indexname;
            """)
            indexes = cursor.fetchall()
            
            cursor.close()
            conn.close()
            
            return {
                "database_name": db_name,
                "tables": [{"name": t[0], "size": t[1], "column_count": t[2]} for t in tables],
                "indexes": [{"name": i[0], "table": i[1], "definition": i[2]} for i in indexes],
                "error": None
            }
            
        except Exception as e:
            return {
                "database_name": None,
                "tables": [],
                "indexes": [],
                "error": str(e)
            }
