import asyncpg
import os
import redis as redis_sync
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer

from api.auth import verify_token
from dotenv import load_dotenv

load_dotenv()

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/login")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME"),
}


async def get_db_pool() -> asyncpg.Pool:
    return await asyncpg.create_pool(**DB_CONFIG, min_size=2, max_size=10)


def get_redis_client() -> redis_sync.Redis:
    return redis_sync.Redis(host="localhost", port=6379, decode_responses=True)


async def get_current_user(token: str = Depends(oauth2_scheme)) -> dict:
    payload = verify_token(token)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return payload