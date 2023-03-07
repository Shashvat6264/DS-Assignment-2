from fastapi import Request, status
from fastapi.responses import JSONResponse
from .app import app
from .controllers import *


@app.exception_handler(UnauthorizedException)
async def unauthorized_exception_handler(request: Request, error: UnauthorizedException):
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        },
    )
    
@app.exception_handler(TopicDoesNotExist)
async def topicdoesnotexist_exception_handler(request: Request, error: TopicDoesNotExist):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        },
    )
    
@app.exception_handler(QueueEmpty)
async def queueempty_exception_handler(request: Request, error: QueueEmpty):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": "QEmpty"
        }
    )
    
@app.exception_handler(TopicAlreadyExists)
async def topicalreadyexists_exception_handler(request: Request, error: TopicAlreadyExists):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(PartitionAlreadyExists)
async def partitionalreadyexists_exception_handler(request: Request, error: PartitionAlreadyExists):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(PartitionDoesNotExist)
async def partitiondoesnotexist_exception_handler(request:Request, error: PartitionDoesNotExist):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(ManagerNotFound)
async def managernotfound_exception_handler(request:Request, error: PartitionDoesNotExist):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
