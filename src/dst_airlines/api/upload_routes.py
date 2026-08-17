import io
from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse

# Import the database function from your pure db file
from .db import insert_bulk_csv

router = APIRouter()

@router.post("/upload-eurocontrol/{table_name}")
async def upload_eurocontrol_data(table_name: str, file: UploadFile = File(...)):
    """Endpoint to receive a CSV file and bulk-save it into a PostgreSQL table."""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid format. Only CSV files are allowed.")
    
    try:
        contents = await file.read()
        csv_in_memory = io.StringIO(contents.decode('utf-8'))
        
        insert_bulk_csv(table_name, csv_in_memory)
        
        return JSONResponse(
            content={"message": f"File {file.filename} successfully inserted into table {table_name}!"}, 
            status_code=200
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database insertion error: {str(e)}")