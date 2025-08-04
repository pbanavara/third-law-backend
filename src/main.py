from fastapi import FastAPI, UploadFile, HTTPException, Header, Query
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import uuid
import os
import time
from typing import Dict, Optional
from enum import Enum
from core.text_processor import PDFTextProcessor
from core.db_client import ClickHouseClient
from dotenv import load_dotenv
from prometheus_client import (
    Counter, Gauge, Histogram, generate_latest,
    CONTENT_TYPE_LATEST, CollectorRegistry
)
import psutil
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Create a new registry
REGISTRY = CollectorRegistry()

# Custom metrics
process = psutil.Process()
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status'],
    registry=REGISTRY
)

REQUEST_LATENCY = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint'],
    buckets=[0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
    registry=REGISTRY
)

PROCESS_MEMORY = Gauge('process_memory_rss_bytes', 'Memory usage in bytes', registry=REGISTRY)
PROCESS_CPU_USAGE = Gauge('process_cpu_usage_percent', 'CPU usage percent per core', registry=REGISTRY)
OPEN_FDS = Gauge('process_open_fds', 'Number of open file descriptors', registry=REGISTRY)
DB_POOL_SIZE = Gauge('db_pool_size', 'Database connection pool size', registry=REGISTRY)

def update_system_metrics():
    """Update system metrics."""
    try:
        PROCESS_MEMORY.set(process.memory_info().rss)
        PROCESS_CPU_USAGE.set(process.cpu_percent(interval=1) / psutil.cpu_count())
        OPEN_FDS.set(process.num_fds() if os.name != 'nt' else 0)
        logger.debug("System metrics updated")
    except Exception as e:
        logger.error(f"Error updating system metrics: {e}")

class APIVersion(str, Enum):
    V1 = "1.0"
    V2 = "2.0"

# Initialize FastAPI app
app = FastAPI()

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize processors and clients
text_processor = PDFTextProcessor()
db_client = None
max_retries = 3
retry_delay = 5  # seconds

def init_db_client():
    """Initialize the database client with retries."""
    global db_client
    for attempt in range(max_retries):
        try:
            db_client = ClickHouseClient(
                host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
                port=int(os.getenv('CLICKHOUSE_PORT', '9440')),
                username=os.getenv('CLICKHOUSE_USER', 'default'),
                password=os.getenv('CLICKHOUSE_PASSWORD', '')
            )
            logger.info("Successfully initialized ClickHouse client")
            DB_POOL_SIZE.set(1)  # Update metric after successful connection
            return
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} to initialize ClickHouse client failed: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to initialize ClickHouse client after all retries")

@app.middleware("http")
async def metrics_middleware(request, call_next):
    """Middleware to record request metrics."""
    start_time = time.time()
    
    response = await call_next(request)
    
    # Record request duration
    duration = time.time() - start_time
    endpoint = request.url.path
    method = request.method
    
    # Update metrics
    REQUEST_COUNT.labels(
        method=method,
        endpoint=endpoint,
        status=response.status_code
    ).inc()
    
    REQUEST_LATENCY.labels(
        method=method,
        endpoint=endpoint
    ).observe(duration)
    
    # Update system metrics periodically
    update_system_metrics()
    
    logger.debug(f"Request metrics recorded - Method: {method}, Endpoint: {endpoint}, Duration: {duration:.3f}s, Status: {response.status_code}")
    
    return response

@app.get("/metrics")
async def metrics():
    """Endpoint to expose metrics."""
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

@app.get("/test")
async def test():
    """Test endpoint."""
    logger.debug("Test endpoint called")
    return {"status": "ok"}

@app.on_event("startup")
async def startup():
    """Startup event handler."""
    logger.info("Application starting up")
    # Initialize database client
    init_db_client()
    # Initialize metrics
    update_system_metrics()
    logger.info("Initial metrics collected")

@app.get("/api/v1/health")
@app.get("/api/health")
async def health_check():
    """Check the health of the API and its dependencies"""
    global db_client
    
    # Try to reconnect if database is disconnected
    if db_client is None:
        init_db_client()
    
    # Test database connection
    db_status = "disconnected"
    db_error = None
    if db_client is not None:
        try:
            db_client._test_connection()
            db_status = "connected"
        except Exception as e:
            db_status = "error"
            db_error = str(e)
            db_client = None  # Reset client on error
    
    response = {
        "status": "healthy" if db_status == "connected" else "degraded",
        "components": {
            "api": "healthy",
            "database": {
                "status": db_status,
                "error": db_error
            }
        }
    }
    
    return JSONResponse(
        content=response,
        status_code=200 if db_status == "connected" else 207
    )

# Support both path-based and header-based versioning
@app.post("/api/v1/upload")  # Path-based versioning (backward compatible)
@app.post("/api/upload")     # Header-based versioning
async def upload_pdf(
    file: UploadFile,
    api_version: Optional[str] = Header(None, alias="X-API-Version")
):
    # Version handling logic
    if api_version and api_version != APIVersion.V1:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported API version. Supported versions: {[v.value for v in APIVersion]}"
        )
    
    try:
        # Check if database is available
        if db_client is None:
            # Try to reconnect
            init_db_client()
            if db_client is None:
                raise HTTPException(
                    status_code=503,
                    detail="Database service is currently unavailable"
                )
        
        # Check if the uploaded file is a PDF
        if not file.filename.endswith('.pdf'):
            raise HTTPException(status_code=400, detail="Only PDF files are allowed")
        
        # Check if file already exists
        existing_doc = db_client.get_document_by_filename(file.filename)
        if existing_doc:
            return JSONResponse(
                content={
                    "status": "exists",
                    "document_id": existing_doc['document_id'],
                    "message": "Document already exists",
                    "api_version": api_version or APIVersion.V1,
                    "analysis": existing_doc['analysis_result']
                },
                status_code=200
            )
        
        # Create a unique document ID
        document_id = str(uuid.uuid4())
        
        # Create a temporary file to store the uploaded PDF
        temp_file_path = f"temp_{document_id}.pdf"
        
        try:
            # Save the uploaded file temporarily
            content = await file.read()
            with open(temp_file_path, "wb") as temp_file:
                temp_file.write(content)
            
            # Extract text from PDF using PyMuPDF
            doc = fitz.open(temp_file_path)
            text = ""
            for page in doc:
                text += page.get_text()
            doc.close()
            
            # Process the extracted text
            analysis_result = text_processor.process_text(text)
            
            # Store in ClickHouse
            storage_success = db_client.store_document(
                document_id=document_id,
                filename=file.filename,
                content=text,
                analysis_result=analysis_result
            )
            
            if not storage_success:
                raise HTTPException(
                    status_code=500,
                    detail="Failed to store document in database"
                )
            
            return JSONResponse(
                content={
                    "status": "success",
                    "document_id": document_id,
                    "message": "PDF processed and stored successfully",
                    "api_version": api_version or APIVersion.V1,
                    "analysis": analysis_result
                },
                status_code=200
            )
            
        finally:
            # Clean up: remove temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing PDF: {str(e)}")

@app.get("/api/v1/document/{document_id}")
@app.get("/api/document/{document_id}")
async def get_document(
    document_id: str,
    api_version: Optional[str] = Header(None, alias="X-API-Version")
):
    # Version handling logic
    if api_version and api_version != APIVersion.V1:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported API version. Supported versions: {[v.value for v in APIVersion]}"
        )

    # Check if database is available
    if db_client is None:
        # Try to reconnect
        init_db_client()
        if db_client is None:
            raise HTTPException(
                status_code=503,
                detail="Database service is currently unavailable"
            )

    # Get document from ClickHouse
    document = db_client.get_document(document_id)
    if not document:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return {
        "document_id": document_id,
        "filename": document['filename'],
        "upload_timestamp": document['upload_timestamp'],
        "content": document['content'],
        "analysis": document['analysis_result'],
        "statistics": {
            "content_length": document['content_length'],
            "sensitive_info_count": document['sensitive_info_count'],
            "email_count": document['email_count'],
            "ssn_count": document['ssn_count']
        },
        "api_version": api_version or APIVersion.V1
    }

@app.get("/api/v1/statistics")
@app.get("/api/statistics")
async def get_statistics(
    api_version: Optional[str] = Header(None, alias="X-API-Version")
):
    """Get overall statistics about processed documents"""
    # Version handling logic
    if api_version and api_version != APIVersion.V1:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported API version. Supported versions: {[v.value for v in APIVersion]}"
        )
    
    # Check if database is available
    if db_client is None:
        # Try to reconnect
        init_db_client()
        if db_client is None:
            raise HTTPException(
                status_code=503,
                detail="Database service is currently unavailable"
            )
    
    stats = db_client.get_statistics()
    return {
        "statistics": stats,
        "api_version": api_version or APIVersion.V1
    }

@app.get("/api/v1/documents")
@app.get("/api/documents")
async def get_all_documents(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    api_version: Optional[str] = Header(None, alias="X-API-Version")
):
    """Get all documents with pagination support."""
    # Version handling logic
    if api_version and api_version != APIVersion.V1:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported API version. Supported versions: {[v.value for v in APIVersion]}"
        )
    
    # Check if database is available
    if db_client is None:
        # Try to reconnect
        init_db_client()
        if db_client is None:
            raise HTTPException(
                status_code=503,
                detail="Database service is currently unavailable"
            )
    
    # Get documents from ClickHouse
    documents = db_client.get_all_documents(limit=limit, offset=offset)
    
    return {
        "total": len(documents),
        "offset": offset,
        "limit": limit,
        "documents": documents,
        "api_version": api_version or APIVersion.V1
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
