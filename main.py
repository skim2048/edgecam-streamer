import json
import asyncio
from contextlib import asynccontextmanager

import cv2
import grpc
import numpy as np
from loguru import logger
from pydantic import BaseModel
from fastapi import FastAPI, HTTPException
from fastapi.websockets import WebSocket
from fastapi.websockets import WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from src.rtsp import RTSPStreamer


STREAMER = RTSPStreamer()


@asynccontextmanager
async def lifespan(app: FastAPI):
    with open("tmp.json", "r") as f:
        cam = json.load(f)["cam"]
    STREAMER.start_streaming(cam["103"])
    yield  # -----
    STREAMER.stop_streaming()


app = FastAPI(lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],)


@app.websocket('/stream1')
async def websocket_endpoint(ws: WebSocket):

    async def _send():
        while True:
            frame = await asyncio.to_thread(STREAMER.websocket_queue.get)
            await ws.send_bytes(frame)
            await asyncio.sleep(0)

    await ws.accept()
    host = ws.client.host
    port = ws.client.port
    logger.info(f'{host}:{port} has been accepted!')

    try:
        await _send()
    except WebSocketDisconnect:
        logger.info('Connection has closed.')
    except asyncio.CancelledError:
        logger.info('Connection has cancelled.')
    except Exception as e:
        logger.exception(f'Unexpected error: {str(e)}')


if __name__ == '__main__':
    import uvicorn
    uvicorn.run(
        app='main:app',
        host='0.0.0.0',
        port=12921,
    )