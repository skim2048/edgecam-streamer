import json
from contextlib import asynccontextmanager

import cv2
import grpc
import numpy as np
from loguru import logger
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from src.rtsp import RTSPStreamer


STREAMER = RTSPStreamer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    with open("configs/sources.json", "r") as f:
        CAM = json.load(f)["cam"]
    location = CAM["101"]
    STREAMER.start_streaming(location)
    yield  # -----
    STREAMER.stop_streaming()


app = FastAPI(lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=[],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],)


@app.post('/start')
async def ping():
    try:
        return {"status": "start"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/stop')
async def stop():
    try:
        return {"status": "stop"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=12921,
    )