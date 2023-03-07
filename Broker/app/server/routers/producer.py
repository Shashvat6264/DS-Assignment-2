from fastapi import APIRouter, Body, status

from ..system import *
from ..utils import *

router = APIRouter()
    
@router.post('/produce', response_description="Produce message")
async def produce(partition_id: int = Body(), producer_id: int = Body(), message: str = Body()):
    await main_dq.enqueue(partition_id, producer_id, message)
    return generic_Response(data={
        "status": "success",
    }, status_code = status.HTTP_200_OK)
    