from fastapi import APIRouter, Body, status, Query
from .producer import router as producerRouter
from .consumer import router as consumerRouter

from ..system import main_dq
from ..utils import *

router = APIRouter()

@router.post('/partitions')
async def createPartition(topic_name: str = Body(), partition_id: int = Body()):
    await main_dq.createPartition(topic_name, partition_id)
    return generic_Response(data={
        "status": "success",
        "message": f"Partition {partition_id} created for topic {topic_name} successfully",
    }, status_code=status.HTTP_201_CREATED)

@router.get('/size')
async def getSize(partition_id: int = Query(default=...), consumer_id: int = Query(default=...)):
    partition_size = await main_dq.size(partition_id, consumer_id)
    return generic_Response(data={
        "status": "success",
        "size": partition_size
    }, status_code=status.HTTP_200_OK)
    

router.include_router(consumerRouter, tags=["consumer"], prefix='/consumer')
router.include_router(producerRouter, tags=["producer"], prefix='/producer')
