from clickhouse_driver import Client
from typing import Dict, Any, Optional
import json
from datetime import datetime

class ClickHouseClient:
    def __init__(self, host: str = 'localhost', port: int = 9000, username: str = 'default', password: str = ''):
        try:
            self.client = Client(
                host=host,
                user=username,
                secure=True,
                port=port,
                password=password,
                settings={
                    'max_execution_time': 60
                }
            )
            self._test_connection()
            self._ensure_table_exists()
        except Exception as e:
            print(f"Error connecting to ClickHouse: {str(e)}")
            raise

    def _test_connection(self):
        try:
            result = self.client.execute('SELECT 1')
            if result:
                print("Successfully connected to ClickHouse")
            else:
                raise Exception("Connection test returned no results")
        except Exception as e:
            print(f"Connection test failed: {str(e)}")
            raise

    def _ensure_table_exists(self):
        try:
            check_query = """
            SELECT name 
            FROM system.tables 
            WHERE database = currentDatabase() 
            AND name = 'documents'
            """
            result = self.client.execute(check_query)
            
            if not result:
                create_table_query = """
                CREATE TABLE IF NOT EXISTS documents (
                    document_id String,
                    filename String,
                    upload_timestamp DateTime64(3, 'UTC'),
                    content String,
                    content_length UInt32,
                    analysis_result String,
                    sensitive_info_count UInt32,
                    email_count UInt32,
                    ssn_count UInt32
                )
                ENGINE = MergeTree()
                PRIMARY KEY (document_id)
                ORDER BY (upload_timestamp, document_id)
                SETTINGS index_granularity = 8192
                """
                self.client.execute(create_table_query)
                print("Created table 'documents'")
            else:
                print("Table 'documents' already exists")
        except Exception as e:
            print(f"Error with table setup: {str(e)}")
            raise

    def get_document_by_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        """Check if a document with the given filename exists and return its details."""
        try:
            query = """
            SELECT
                document_id,
                filename,
                upload_timestamp,
                content,
                content_length,
                analysis_result,
                sensitive_info_count,
                email_count,
                ssn_count
            FROM documents
            WHERE filename = %(filename)s
            ORDER BY upload_timestamp DESC
            LIMIT 1
            """
            result = self.client.execute(query, {'filename': filename}, with_column_types=True)
            
            if not result[0]:
                return None
                
            row = result[0][0]
            return {
                'document_id': row[0],
                'filename': row[1],
                'upload_timestamp': row[2].isoformat(),
                'content': row[3],
                'content_length': row[4],
                'analysis_result': json.loads(row[5]),
                'sensitive_info_count': row[6],
                'email_count': row[7],
                'ssn_count': row[8]
            }
        except Exception as e:
            print(f"Error checking for existing document: {str(e)}")
            return None

    def store_document(self, document_id: str, filename: str, content: str, analysis_result: Dict[str, Any]) -> bool:
        try:
            stats = analysis_result.get('statistics', {})
            findings_by_type = stats.get('findings_by_type', {})
            
            data = [(
                document_id,
                filename,
                datetime.utcnow(),
                content,
                len(content),
                json.dumps(analysis_result),
                stats.get('total_findings', 0),
                findings_by_type.get('email', 0),
                findings_by_type.get('ssn', 0)
            )]
            
            self.client.execute(
                """
                INSERT INTO documents (
                    document_id, filename, upload_timestamp, content,
                    content_length, analysis_result, sensitive_info_count,
                    email_count, ssn_count
                ) VALUES
                """,
                data
            )
            print(f"Successfully stored document {document_id}")
            return True
        except Exception as e:
            print(f"Error storing document in ClickHouse: {str(e)}")
            return False

    def get_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        try:
            query = """
            SELECT
                document_id,
                filename,
                upload_timestamp,
                content,
                content_length,
                analysis_result,
                sensitive_info_count,
                email_count,
                ssn_count
            FROM documents
            WHERE document_id = %(document_id)s
            LIMIT 1
            """
            result = self.client.execute(query, {'document_id': document_id}, with_column_types=True)
            
            if not result[0]:
                return None
                
            row = result[0][0]
            return {
                'document_id': row[0],
                'filename': row[1],
                'upload_timestamp': row[2].isoformat(),
                'content': row[3],
                'content_length': row[4],
                'analysis_result': json.loads(row[5]),
                'sensitive_info_count': row[6],
                'email_count': row[7],
                'ssn_count': row[8]
            }
        except Exception as e:
            print(f"Error retrieving document from ClickHouse: {str(e)}")
            return None

    def get_statistics(self) -> Dict[str, Any]:
        try:
            query = """
            SELECT
                count() as total_documents,
                sum(sensitive_info_count) as total_sensitive_info,
                sum(email_count) as total_emails,
                sum(ssn_count) as total_ssns,
                avg(sensitive_info_count) as avg_sensitive_per_doc,
                max(sensitive_info_count) as max_sensitive_in_doc
            FROM documents
            """
            result = self.client.execute(query)
            if not result:
                return {}
            row = result[0]
            return {
                'total_documents': row[0],
                'total_sensitive_info': row[1],
                'total_emails': row[2],
                'total_ssns': row[3],
                'avg_sensitive_per_doc': float(row[4]),
                'max_sensitive_in_doc': row[5]
            }
        except Exception as e:
            print(f"Error getting statistics from ClickHouse: {str(e)}")
            return {} 