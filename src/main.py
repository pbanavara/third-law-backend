from fastapi import FastAPI, UploadFile, HTTPException, Header, Query, BackgroundTasks
from fastapi.responses import JSONResponse, Response
from fastapi.middleware.cors import CORSMiddleware
import pypdfium2 as pdfium  # Replace PyMuPDF with pypdfium2
import uuid
import os
import time
from typing import Dict, Optional
from enum import Enum
from core.text_processor import PDFTextProcessor
from core.db_client import ClickHouseClient
from dotenv import load_dotenv
import psutil
import logging
import aiofiles
import asyncio
from contextlib import asynccontextmanager
from io import BytesIO

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class APIVersion(str, Enum):
    V1 = "1.0"
    V2 = "2.0"

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
            return
        except Exception as e:
            logger.error(f"Attempt {attempt + 1}/{max_retries} to initialize ClickHouse client failed: {str(e)}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("Failed to initialize ClickHouse client after all retries")

async def store_in_db_async(document_id: str, filename: str, content: str, analysis_result: Dict):
    """Async function to store document in database."""
    try:
        start_time = time.time()
        
        # Initialize database client if needed
        global db_client
        if db_client is None:
            try:
                init_db_client()
            except Exception as e:
                logger.error(f"Failed to initialize database client: {str(e)}")
                return
        
        if db_client is not None:
            storage_success = db_client.store_document(
                document_id=document_id,
                filename=filename,
                content=content,
                analysis_result=analysis_result
            )
            
            if not storage_success:
                logger.error(f"Failed to store document {document_id} in database")
            else:
                end_time = time.time()
                logger.info(f"Database storage completed in {(end_time - start_time):.3f}s")
        else:
            logger.error("Database client not available for storage")
    except Exception as e:
        logger.error(f"Error in database storage for document {document_id}: {str(e)}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events."""
    logger.info("Application starting up")
    # Initialize database client (optional - won't block startup)
    try:
        init_db_client()
    except Exception as e:
        logger.warning(f"Database initialization failed during startup: {str(e)}")
    yield
    logger.info("Application shutting down")

# Initialize FastAPI app
app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/test")
async def test():
    """Test endpoint."""
    return {"status": "ok"}

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

@app.post("/api/v1/upload")  # Path-based versioning (backward compatible)
@app.post("/api/upload")     # Header-based versioning
async def upload_pdf(
    file: UploadFile,
    api_version: Optional[str] = Header(None, alias="X-API-Version")
):
    start_time = time.time()
    
    try:
        # Check if the uploaded file is a PDF
        if not file.filename.endswith('.pdf'):
            raise HTTPException(status_code=400, detail="Only PDF files are allowed")
        
        # Generate document ID
        document_id = str(uuid.uuid4())
        
        # Read the file content
        content = await file.read()
        read_time = time.time()
        logger.info(f"File read completed in {(read_time - start_time):.3f}s")
        
        # Extract text from PDF (lightweight processing)
        pdf_text = ""
        try:
            pdf_document = pdfium.PdfDocument(BytesIO(content))
            # Only process first few pages for performance
            max_pages = min(3, len(pdf_document))
            for page_num in range(max_pages):
                page = pdf_document.get_page(page_num)
                text_page = page.get_textpage()
                pdf_text += text_page.get_text_range()
                text_page.close()
                page.close()
            pdf_document.close()
            text_extraction_time = time.time()
            logger.info(f"Text extraction completed in {(text_extraction_time - read_time):.3f}s")
        except Exception as e:
            logger.error(f"Error extracting text from PDF: {str(e)}")
            # Continue with empty text rather than failing
            pdf_text = ""
            text_extraction_time = time.time()
        
        # Process text for sensitive information (lightweight)
        analysis_result = text_processor.process_text(pdf_text)
        text_processing_time = time.time()
        logger.info(f"Text processing completed in {(text_processing_time - text_extraction_time):.3f}s")
        
        # Schedule database storage in background (non-blocking)
        try:
            asyncio.create_task(store_in_db_async(document_id, file.filename, pdf_text, analysis_result))
        except Exception as e:
            logger.error(f"Failed to schedule database storage: {str(e)}")
        
        end_time = time.time()
        logger.info(f"Request processing completed in {(end_time - start_time):.3f}s")
        
        return JSONResponse(
            content={
                "status": "success",
                "document_id": document_id,
                "message": "PDF processed successfully (storage in background).",
                "api_version": api_version or APIVersion.V1,
                "analysis": analysis_result,
                "timing": {
                    "read": round(read_time - start_time, 3),
                    "text_extraction": round(text_extraction_time - read_time, 3),
                    "text_processing": round(text_processing_time - text_extraction_time, 3),
                    "total": round(end_time - start_time, 3)
                }
            },
            status_code=200
        )
                
    except HTTPException:
        raise
    except Exception as e:
        end_time = time.time()
        logger.error(f"Error after {(end_time - start_time):.3f}s: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing file: {str(e)}")

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
    import multiprocessing

    # Use single worker for now to avoid potential issues
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8001,
        workers= multiprocessing.cpu_count(),
        loop="uvloop",
        timeout_keep_alive=30,
        log_level="info",
        reload=False  # Disable reload for production testing
    )
