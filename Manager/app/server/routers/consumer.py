from typing import Union
from fastapi import APIRouter, Body, status, Query

from ..system import main_manager
from ..utils import *

router = APIRouter()

@router.post('/register')
@write_route
async def register(topic_name: str = Body(..., embed=True)):
    consumer_id = await main_manager.registerConsumer(topic_name)
    return generic_Response(data={
        "status": "success",
        "consumer_id": consumer_id
    }, status_code=status.HTTP_200_OK)

@router.get('/consume')
async def consumer(topic_name: str = Query(default=...), consumer_id: int = Query(default=...), partition_key: Union[int, None] = Query(default=None)):
    message = await main_manager.dequeue(topic_name, consumer_id, partition_key)
    return generic_Response(data={
        "status": "success",
        "message": message
    }, status_code=status.HTTP_200_OK)