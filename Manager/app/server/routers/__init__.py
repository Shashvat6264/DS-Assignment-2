from typing import Union
from fastapi import APIRouter, Body, status, Query, Request
from .producer import router as producerRouter
from .consumer import router as consumerRouter

from ..system import main_manager, READ_ONLY, health_manager
from ..utils import *
from ..controllers import HeartBeat

router = APIRouter()

@router.post('/brokers')
@write_route
async def createBroker(broker_address: str = Body(..., embed=True), partitions: Union[list, None] = Body(None, embed=True)):
    await main_manager.addBroker(broker_address, partitions)
    return generic_Response(data={
        "status": "success",
        "message": f"Broker {broker_address} added successfully"
    }, status_code=status.HTTP_201_CREATED)

@router.get('/brokers')
async def listBrokers():
    brokers = await main_manager.listBrokers()
    return generic_Response(data={
        "status": "success",
        "brokers": brokers
    }, status_code=status.HTTP_200_OK)

@router.post('/topics')
@write_route
async def createTopic(topic_name: str = Body(), number_of_partitions: int = Body(default=3)):
    await main_manager.createTopic(topic_name, number_of_partitions)
    return generic_Response(data={
        "status": "success",
        "message": f"Topic {topic_name} created successfully"
    }, status_code=status.HTTP_201_CREATED)
    
@router.get('/topics')
async def listTopics():
    topics = await main_manager.listTopics()
    return generic_Response(data={
        "status": "success",
        "topics": topics
    }, status_code=status.HTTP_200_OK)

@router.get('/size')
async def getSize(topic_name: str = Query(default=...), consumer_id: int = Query(default=...)):
    topic_size = await main_manager.size(topic_name, consumer_id)
    return generic_Response(data={
        "status": "success",
        "size": topic_size
    }, status_code=status.HTTP_200_OK)
    
@router.get('/health')
async def health(request: Request):
    address = request.client.host
    beatType = request.query_params['type']
    key = request.query_params['key']
    health_manager.heartbeat(HeartBeat(address, beatType, key))
    return generic_Response(data={
        "status": "success"
    }, status_code=status.HTTP_200_OK)
    
    
router.include_router(consumerRouter, tags=["consumer"], prefix='/consumer')
router.include_router(producerRouter, tags=["producer"], prefix='/producer')