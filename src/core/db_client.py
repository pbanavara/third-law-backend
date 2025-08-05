from clickhouse_driver import Client
from typing import Dict, Any, Optional, List
import json
from datetime import datetime
import time
import logging
from contextlib import contextmanager
from queue import Queue, Empty
import threading

logger = logging.getLogger(__name__)

class ConnectionPool:
    def __init__(self, size: int, **db_params):
        self.size = size
        self.db_params = db_params
        self.pool: Queue[Client] = Queue(maxsize=size)
        self.lock = threading.Lock()
        self._fill_pool()

    def _fill_pool(self):
        for _ in range(self.size):
            client = Client(**self.db_params)
            self.pool.put(client)

    @contextmanager
    def get_connection(self):
        connection = None
        try:
            connection = self.pool.get(timeout=5)  # 5 second timeout
            yield connection
        finally:
            if connection:
                try:
                    # Test if connection is still good
                    connection.execute('SELECT 1')
                    self.pool.put(connection)
                except Exception as e:
                    logger.error(f"Connection error, creating new one: {e}")
                    # Create new connection to replace the bad one
                    try:
                        new_conn = Client(**self.db_params)
                        self.pool.put(new_conn)
                    except Exception as e:
                        logger.error(f"Failed to create new connection: {e}")

class ClickHouseClient:
    def __init__(self, host: str = 'localhost', port: int = 9000, username: str = 'default', password: str = ''):
        try:
            self.db_params = {
                'host': host,
                'user': username,
                'secure': True,
                'port': port,
                'password': password,
                'settings': {
                    'max_execution_time': 60
                }
            }
            
            # Create a connection pool with 5 connections
            self.pool = ConnectionPool(5, **self.db_params)
            
            # Test the connection and create table
            with self.pool.get_connection() as client:
                self._test_connection(client)
                self._ensure_table_exists(client)
                
        except Exception as e:
            logger.error(f"Error connecting to ClickHouse: {str(e)}")
            raise

    def _test_connection(self, client: Client):
        result = client.execute('SELECT 1')
        if result:
            logger.info("Successfully connected to ClickHouse")
        else:
            raise Exception("Connection test returned no results")

    def _ensure_table_exists(self, client: Client):
        try:
            check_query = """
            SELECT name
            FROM system.tables
            WHERE database = currentDatabase()
            AND name = 'documents'
            """
            result = client.execute(check_query)

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
                client.execute(create_table_query)
                logger.info("Created table 'documents'")
            else:
                logger.info("Table 'documents' already exists")
        except Exception as e:
            logger.error(f"Error with table setup: {str(e)}")
            raise

    def store_document(self, document_id: str, filename: str, content: str, analysis_result: Dict[str, Any]) -> bool:
        try:
            stats = analysis_result.get('statistics', {})
            findings_by_type = stats.get('findings_by_type', {})

            start_prepare = time.time()
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
            prepare_time = time.time()
            logger.info(f"Data preparation took {(prepare_time - start_prepare):.3f}s")

            with self.pool.get_connection() as client:
                # Check if document already exists
                start_check = time.time()
                existing = client.execute(
                    "SELECT count() FROM documents WHERE document_id = %(id)s",
                    {'id': document_id}
                )
                check_time = time.time()
                logger.info(f"Document existence check took {(check_time - start_check):.3f}s")

                if existing[0][0] > 0:
                    # Document exists, update it
                    logger.info(f"Document {document_id} already exists, updating...")
                    start_update = time.time()
                    client.execute(
                        """
                        ALTER TABLE documents 
                        UPDATE 
                            filename = %(filename)s,
                            upload_timestamp = %(timestamp)s,
                            content = %(content)s,
                            content_length = %(content_length)s,
                            analysis_result = %(analysis_result)s,
                            sensitive_info_count = %(sensitive_count)s,
                            email_count = %(email_count)s,
                            ssn_count = %(ssn_count)s
                        WHERE document_id = %(document_id)s
                        """,
                        {
                            'document_id': document_id,
                            'filename': filename,
                            'timestamp': datetime.utcnow(),
                            'content': content,
                            'content_length': len(content),
                            'analysis_result': json.dumps(analysis_result),
                            'sensitive_count': stats.get('total_findings', 0),
                            'email_count': findings_by_type.get('email', 0),
                            'ssn_count': findings_by_type.get('ssn', 0)
                        }
                    )
                    update_time = time.time()
                    logger.info(f"Document update took {(update_time - start_update):.3f}s")
                    logger.info(f"Total database operation took {(update_time - start_prepare):.3f}s")
                    return True
                else:
                    # Document doesn't exist, insert it
                    logger.info(f"Document {document_id} doesn't exist, inserting...")
                    start_insert = time.time()
                    client.execute(
                        """
                        INSERT INTO documents (
                            document_id, filename, upload_timestamp, content,
                            content_length, analysis_result, sensitive_info_count,
                            email_count, ssn_count
                        ) VALUES
                        """,
                        data
                    )
                    insert_time = time.time()
                    logger.info(f"Document insert took {(insert_time - start_insert):.3f}s")
                    logger.info(f"Total database operation took {(insert_time - start_prepare):.3f}s")
                    return True

        except Exception as e:
            logger.error(f"Error storing document in ClickHouse: {str(e)}")
            return False

    def get_document(self, document_id: str) -> Optional[Dict[str, Any]]:
        try:
            with self.pool.get_connection() as client:
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
                result = client.execute(query, {'document_id': document_id}, with_column_types=True)

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
            logger.error(f"Error retrieving document from ClickHouse: {str(e)}")
            return None

    def get_document_by_filename(self, filename: str) -> Optional[Dict[str, Any]]:
        try:
            with self.pool.get_connection() as client:
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
                result = client.execute(query, {'filename': filename}, with_column_types=True)

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
            logger.error(f"Error checking for existing document: {str(e)}")
            return None

    def get_statistics(self) -> Dict[str, Any]:
        try:
            with self.pool.get_connection() as client:
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
                result = client.execute(query)
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
            logger.error(f"Error getting statistics from ClickHouse: {str(e)}")
            return {}

    def get_all_documents(self, limit: int = 100, offset: int = 0) -> List[Dict[str, Any]]:
        try:
            with self.pool.get_connection() as client:
                query = """
                SELECT
                    document_id,
                    filename,
                    upload_timestamp,
                    content_length,
                    sensitive_info_count,
                    email_count,
                    ssn_count
                FROM documents
                ORDER BY upload_timestamp DESC
                LIMIT %(limit)s
                OFFSET %(offset)s
                """
                result = client.execute(
                    query,
                    {'limit': limit, 'offset': offset},
                    with_column_types=True
                )

                if not result[0]:
                    return []

                documents = []
                for row in result[0]:
                    documents.append({
                        'document_id': row[0],
                        'filename': row[1],
                        'upload_timestamp': row[2].isoformat(),
                        'content_length': row[3],
                        'sensitive_info_count': row[4],
                        'email_count': row[5],
                        'ssn_count': row[6]
                    })
                return documents
        except Exception as e:
            logger.error(f"Error retrieving documents from ClickHouse: {str(e)}")
            return [] 