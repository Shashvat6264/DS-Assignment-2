from typing import Union
from fastapi import APIRouter, Body, status

from ..system import main_manager
from ..utils import *

router = APIRouter()

@router.post('/register')
@write_route
async def register(topic_name: str = Body(..., embed=True)):
    producer_id = await main_manager.registerProducer(topic_name)
    return generic_Response(data={
        "status": "success",
        "producer_id": producer_id
    }, status_code=status.HTTP_200_OK)

@router.post('/produce')
@write_route
async def produce(topic_name: str = Body(), producer_id: int = Body(), message: str = Body(), partition_key: Union[int, None] = Body(default=None)):
    await main_manager.enqueue(topic_name, producer_id, message, partition_key)
    return generic_Response(data={
        "status": "success",
    }, status_code=status.HTTP_200_OK)