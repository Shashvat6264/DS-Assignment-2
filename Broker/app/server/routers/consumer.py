from fastapi import APIRouter, Body, status, Query

from ..system import main_dq
from ..utils import *

router = APIRouter()
    
@router.get('/consume', response_description="Consume message")
async def consume(partition_id: int = Query(default=...), consumer_id: int = Query(default=...)):
    message = await main_dq.dequeue(partition_id, consumer_id)
    return generic_Response(data = {
        "status": "success",
        "message": message
    }, status_code = status.HTTP_200_OK)
    