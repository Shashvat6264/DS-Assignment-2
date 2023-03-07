from fastapi import Request, status
from fastapi.responses import JSONResponse
from .app import app
from .controllers import *

@app.exception_handler(BrokerAlreadyPresent)
async def brokeralreadypresent_exception_handler(request: Request, error: BrokerAlreadyPresent):
    return JSONResponse(
        status_code=status.HTTP_208_ALREADY_REPORTED,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )

@app.exception_handler(TopicDoesNotExist)
async def topicdoesnotexist_exception_handler(request: Request, error: TopicDoesNotExist):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(TopicAlreadyExists)
async def topicdoesnotexist_exception_handler(request: Request, error: TopicAlreadyExists):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )

@app.exception_handler(PartitionNotInTopic)
async def partitionnotintopic_exception_handler(request: Request, error: PartitionNotInTopic):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(BrokerError)
async def brokererror_exception_handler(request: Request, error: BrokerError):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(BrokerDownError)
async def brokerdownerror_exception_handler(request: Request, error: BrokerDownError):
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )

@app.exception_handler(UnAuthorizedAccess)
async def unauthorizedaccess_exception_handler(request: Request, error: UnAuthorizedAccess):
    return JSONResponse(
        status_code=status.HTTP_401_UNAUTHORIZED,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )
    
@app.exception_handler(NoBrokerPresent)
async def nobrokerpresent_exception_handler(request: Request, error: NoBrokerPresent):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )

@app.exception_handler(QueueEmpty)
async def queueempty_exception_handler(request: Request, error: QueueEmpty):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )

@app.exception_handler(WriteInReadManager)
async def writeinreadmanager_exception_handler(request: Request, error: QueueEmpty):
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={
            "status": "failure",
            "message": f"{str(error)}"
        }
    )

