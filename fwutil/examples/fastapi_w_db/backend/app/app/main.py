import json
import logging
import time
from pathlib import Path

from app import crud
from app.api import deps
from app.api.api_v1.api import api_router
from app.core.config import settings
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request, logger
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from starlette.middleware.base import BaseHTTPMiddleware

# Define the base path of this script
BASE_PATH = Path(__file__).resolve().parent

# Create a Jinja2Templates instance for rendering HTML templates
TEMPLATES = Jinja2Templates(directory=str(BASE_PATH / "templates"))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("app")
logger.info("Logger created")

import asyncio
import json

from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send


class LogMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        if hasattr(request.state, "body"):
            body_json = request.state.body
        else:
            body_json = None
        logger.info(f"Incoming request: {request.method} {request.url} {body_json}")
        response = await call_next(request)
        return response


root_router = APIRouter()
app = FastAPI(title="App API", openapi_url=f"{settings.API_V1_STR}/openapi.json")

logger.info("FastAPI application created")


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content=jsonable_encoder({"detail": exc.errors(), "body": exc.body}),
    )


@app.middleware("http")
async def log_request(request: Request, call_next):
    logger.info(f"Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    return response


app.add_middleware(LogMiddleware)
logger.info("LogMiddleware added to the application")

# Set all CORS enabled origins
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS],
        allow_origin_regex=settings.BACKEND_CORS_ORIGIN_REGEX,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info("CORS middleware added to the application")


@root_router.get("/", status_code=200)
def root(
    request: Request,
    db: Session = Depends(deps.get_db),
) -> dict:
    """
    Root GET request handler.

    This function handles GET requests to the root ("/") endpoint.

    Args:
        request: The incoming HTTP request.
        db: A SQLAlchemy Session object for database operations.

    Returns:
        A dictionary that is used to render an HTML template.
    """
    return TEMPLATES.TemplateResponse("index.html", {"request": request})


@app.get("/ping")
async def pong():
    return {"ping": "pong!"}


@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    """
    Middleware to add a custom X-Process-Time header to all HTTP responses.

    Args:
        request: The incoming HTTP request.
        call_next: A function to call the next middleware or route handler.

    Returns:
        The HTTP response with the added header.
    """
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    return response


# Include the routers
app.include_router(api_router, prefix=settings.API_V1_STR)
app.include_router(root_router)

logger.info("Routers have been included in the application")

if __name__ == "__main__":
    # Use this for debugging purposes only
    import uvicorn

    logger.info("Starting uvicorn")
    uvicorn.run(app, host="0.0.0.0", port=8001, log_level="debug")
