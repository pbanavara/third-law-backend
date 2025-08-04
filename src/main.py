from fastapi import FastAPI, UploadFile, HTTPException, Header
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import fitz  # PyMuPDF
import uuid
import os
from typing import Dict, Optional
from enum import Enum

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

# In-memory storage for document texts (in a real app, this would be a database)
document_store: Dict[str, str] = {}

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
            
            # Store the extracted text
            document_store[document_id] = text
            
            return JSONResponse(
                content={
                    "status": "success",
                    "document_id": document_id,
                    "message": "PDF processed successfully",
                    "api_version": api_version or APIVersion.V1
                },
                status_code=200
            )
            
        finally:
            # Clean up: remove temporary file
            if os.path.exists(temp_file_path):
                os.remove(temp_file_path)
                
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
    print(document_store)
    if document_id not in document_store:
        raise HTTPException(status_code=404, detail="Document not found")
    
    return {
        "document_id": document_id,
        "text": document_store[document_id],
        "api_version": api_version or APIVersion.V1
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
