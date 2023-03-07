from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from .system import READ_ONLY
from .controllers import WriteInReadManager

from functools import wraps

def generic_Response(data, status_code):
    response = jsonable_encoder(data)
    return JSONResponse(response, status_code=status_code)

def write_route(func):
    @wraps(func)
    async def f(*args, **kwargs):
        if READ_ONLY:
            raise WriteInReadManager("This route cannot be requested in read-only manager")
        return await func(*args, **kwargs)
    return f