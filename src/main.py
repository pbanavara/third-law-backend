from fastapi import FastAPI, UploadFile, HTTPException, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import uuid
import os
from typing import Dict, Optional
from enum import Enum
from core.text_processor import PDFTextProcessor
from core.db_client import ClickHouseClient
import time

class APIVersion(str, Enum):
    V1 = "1.0"
    V2 = "2.0"

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Initialize processors and clients
text_processor = PDFTextProcessor()
db_client = None
max_retries = 3
retry_delay = 5  # seconds

def init_db_client():
    global db_client
    for attempt in range(max_retries):
        try:
            db_client = ClickHouseClient(
                host="am2q8y22r3.us-west-2.aws.clickhouse.cloud",
                port=9440,  # ClickHouse Cloud native protocol port
                username="default",
                password="AQ3n3bXM~wDtq"
            )
            print("Successfully initialized ClickHouse client")
            return
        except Exception as e:
            print(f"Attempt {attempt + 1}/{max_retries} to initialize ClickHouse client failed: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                print("Failed to initialize ClickHouse client after all retries")

# Try to initialize the database client
init_db_client()

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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
