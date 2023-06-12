"""
This module creates an API router for the FastAPI application and includes routes from the auth module.
"""

from app.api.api_v1.endpoints import auth
from fastapi import APIRouter

api_router = APIRouter()
"""
An instance of APIRouter to which we will include the routes from the auth module.
"""

api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
"""
Includes the routes defined in the auth module under the prefix "/auth". 
The routes from the auth module will be categorized under the tag "auth" in the API documentation.
"""
