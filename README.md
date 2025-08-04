# PDF Processing and Analysis Service

A FastAPI-based service that processes PDF documents, extracts text, analyzes for sensitive information, and stores results in ClickHouse.

## Features

- PDF text extraction using PyMuPDF
- Sensitive information detection (emails, SSNs)
- Document deduplication based on filename
- REST API with versioning
- ClickHouse integration for document storage
- Health check endpoints
- CORS support

## Tech Stack

- Python 3.9+
- FastAPI (Web Framework)
- PyMuPDF (PDF Processing)
- ClickHouse (Document Storage)
- Uvicorn (ASGI Server)

## Project Structure

```
third-law-backend/
├── src/
│   ├── core/
│   │   ├── __init__.py
│   │   ├── db_client.py      # ClickHouse client
│   │   ├── pdf_scanner.py    # PDF processing
│   │   └── text_processor.py # Text analysis
│   └── main.py              # FastAPI application
└── tests/                   # Test files
```

## API Endpoints

- `POST /api/v1/upload`: Upload and process PDF files
- `GET /api/v1/document/{document_id}`: Retrieve processed document
- `GET /api/v1/statistics`: Get processing statistics
- `GET /api/v1/health`: Service health check

## Setup

1. Create conda environment:
```bash
conda create -n third-law-interview python=3.9
conda activate third-law-interview
```

2. Install dependencies:
```bash
pip install fastapi uvicorn python-multipart pymupdf clickhouse-driver python-dotenv
```

3. Configure environment variables:
```bash
# Create .env file with:
CLICKHOUSE_HOST=your-host.clickhouse.cloud
CLICKHOUSE_PORT=9440
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your-password
CLICKHOUSE_SECURE=true
```

## Running the Service

```bash
# Start the service
python src/main.py
```

The service will be available at `http://localhost:8001`

## Deployment

The service is designed to run on Google Cloud VMs behind a Load Balancer:

1. VM Configuration:
   - Machine type: e2-medium (2 vCPU, 4GB memory) recommended
   - Ubuntu 20.04 LTS
   - Python 3.9+
   - Systemd service for process management

2. Load Balancer Setup:
   - HTTP(S) Load Balancer
   - Health check on `/api/v1/health`
   - SSL termination at load balancer

## Development

1. Clone the repository:
```bash
git clone <repository-url>
cd third-law-backend
```

2. Create conda environment and install dependencies (see Setup section)

3. Run the service locally:
```bash
python src/main.py
```

## Testing

To test the API:

```bash
# Upload a PDF
curl -X POST http://localhost:8001/api/v1/upload -F "file=@./your-file.pdf"

# Get document by ID
curl http://localhost:8001/api/v1/document/{document_id}

# Get statistics
curl http://localhost:8001/api/v1/statistics
```

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| CLICKHOUSE_HOST | ClickHouse server host | localhost |
| CLICKHOUSE_PORT | ClickHouse server port | 9440 |
| CLICKHOUSE_USER | ClickHouse username | default |
| CLICKHOUSE_PASSWORD | ClickHouse password | - |
| CLICKHOUSE_SECURE | Use secure connection | true | 