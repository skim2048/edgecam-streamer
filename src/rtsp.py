import json
import time
import threading
# from datetime import datetime
from typing import Any
from dataclasses import dataclass, fields

import cv2
import grpc
import numpy as np
from loguru import logger
import gi
gi.require_version("Gst", "1.0")
from gi.repository import Gst, GLib

from src.task import SingleThreadTask
from src.buffer import EvictingQueue
from src.grpc import frame_pb2, frame_pb2_grpc


Gst.init(None)


with open("configs/gstreamer.json", "r") as f:
    _CHAIN = json.load(f)["chain"]
VIDEO = _CHAIN["video"]
AUDIO = _CHAIN["audio"]


@dataclass
class Pair():

    video: Any
    audio: Any

    @property
    def media_type(self) -> list[str]:
        return [f.name for f in fields(self)]
    
    def __getitem__(self, key: str) -> Any:
        return getattr(self, key)
    
    def __setitem__(self, key: str, value: Any):
        return setattr(self, key, value)


def draw_circle(img: np.ndarray):
    if img.ndim != 3:
        logger.warning("img.ndim != 3")
        return
    cy, cx = (np.array(img.shape[:2])/2).astype(int)
    cv2.circle(img, (cx, cy), 30, (0, 255, 0), 5)


def deidentify(stub, frame):
    response = stub.Deidentify(
        frame_pb2.Frame(shape=list(frame.shape), frame=frame.tobytes()), timeout=5.0)
    frame = np.frombuffer(response.frame, dtype=np.uint8).reshape(response.shape)
    return frame


def extract_codec(location: str, timeout: int=10) -> Pair:
    codec = Pair(None, None)
    lock = threading.Lock()
    cond = threading.Condition(lock)
    loop = GLib.MainLoop()

    def on_pad_added(ele, new_pad):
        struct = new_pad.query_caps(None).get_structure(0)
        media = struct.get_string("media")
        if media in codec.media_type:
            name = struct.get_string("encoding-name")
            with cond:
                codec[media] = name
                cond.notify_all()

    def wait_for_codec_extraction():
        with cond:
            start_t = time.time()
            while codec.video is None or codec.audio is None:
                elapsed = time.time() - start_t
                remaining = timeout - elapsed
                if remaining <= 0:
                    logger.warning("Timeout occurred during codec extraction.")
                    break
                cond.wait(timeout=remaining)
        loop.quit()

    pipe = Gst.parse_launch(f"rtspsrc name=src location={location} latency=200 ! fakesink")
    src = pipe.get_by_name('src')
    handler_id = src.connect('pad-added', on_pad_added)

    logger.info(f'Attempting to extract codecs within {timeout} seconds ...')
    pipe.set_state(Gst.State.PLAYING)

    wait_th = threading.Thread(target=wait_for_codec_extraction)
    wait_th.start()
    loop.run()
    wait_th.join()

    src.disconnect(handler_id)
    pipe.set_state(Gst.State.NULL)
    logger.info(f'Extracting complete: (video: {codec.video}, audio: {codec.audio})')
    return codec


def make_chain_desc(elements: list[str]) -> str:
    desc = ""
    for i, ele in enumerate(elements):
        name = f"name={ele}" if not i else ""
        desc += f"{ele} {name} ! "
    return desc


class VideoAudioDecoder():

    def __init__(self):
        self._pipe: Gst.Pipeline | None = None
        self._vq: EvictingQueue | None = None
        self._aq: EvictingQueue | None = None

        self._handlers: list = []

    @property
    def video_queue(self) -> EvictingQueue | None:
        return self._vq

    @property
    def audio_queue(self) -> EvictingQueue | None:
        return self._aq

    def _handle_bus_message(self, bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            logger.error(f"GStreamer: {err}: {dbg}")
            self.stop()
        elif msg.type == Gst.MessageType.EOS:
            logger.info("GStreamer: End of stream.")
            self.stop()

    def _handle_pad_added(self, src, new_pad, sink: Pair):
        struct = new_pad.query_caps(None).get_structure(0)
        media = struct.get_string("media")
        if media in sink.media_type:
            if not sink[media]:
                return
            sink_pad = sink[media].get_static_pad("sink")
            if not sink_pad.is_linked():
                new_pad.link(sink_pad)

    def _handle_new_sample(self, appsink):
        sample = appsink.emit("pull-sample")
        buf = sample.get_buffer()
        ret, map_info = buf.map(Gst.MapFlags.READ)
        if not ret:
            return Gst.FlowReturn.ERROR
        struct = sample.get_caps().get_structure(0)
        caps_name = struct.get_name()

        try:
            if caps_name == "video/x-raw":
                w = struct.get_value("width")
                h = struct.get_value("height")
                shape = (h * 3 // 2, w)
                frame = np.frombuffer(map_info.data, dtype=np.uint8)
                frame = frame.reshape(shape)
                blob = (frame, buf.pts, buf.dts, buf.duration)
                self._vq.put(blob)

            elif caps_name == "audio/x-raw":
                audio = map_info.data
                blob = (audio, buf.pts, buf.dts, buf.duration)
                self._aq.put(blob)
        except:
            logger.exception("Exception occurred while processing the sample:")
        finally:
            buf.unmap(map_info)

        return Gst.FlowReturn.OK

    def _create_pipeline(self, location: str, codec: Pair):
        sink = Pair(None, None)
        desc = f"rtspsrc name=rtspsrc location={location} latency=200 ! \n"

        if codec.video and codec.video in VIDEO:
            decode_set = VIDEO[codec.video]["decode_set"]
            desc += make_chain_desc(decode_set)
            desc += "videoconvert ! "
            desc += "appsink name=v_appsink emit-signals=true sync=false \n"
            self._vq = EvictingQueue()
            sink.video = decode_set[0]

        if codec.audio and codec.audio in AUDIO:
            decode_set = AUDIO[codec.audio]["decode_set"]
            desc += make_chain_desc(decode_set)
            desc += "audioconvert ! "
            desc += "audioresample ! "
            desc += "appsink name=a_appsink emit-signals=true sync=false \n"
            self._aq = EvictingQueue()
            sink.audio = decode_set[0]

        self._pipe = Gst.parse_launch(desc)
        if sink.video:
            sink.video = self._pipe.get_by_name(sink.video)
        if sink.audio:
            sink.audio = self._pipe.get_by_name(sink.audio)
        src = self._pipe.get_by_name("rtspsrc")
        handler_id = src.connect("pad-added", self._handle_pad_added, sink)
        self._handlers.append((src, handler_id))
        for name in ["v_appsink", "a_appsink"]:
            appsink = self._pipe.get_by_name(name)
            if appsink:
                handler_id = appsink.connect("new-sample", self._handle_new_sample)
                self._handlers.append((appsink, handler_id))

        bus = self._pipe.get_bus()
        bus.add_signal_watch()
        handler_id = bus.connect("message", self._handle_bus_message)        
        self._handlers.append((bus, handler_id))

    def start(self, location: str, codec: Pair):
        self.stop()
        self._create_pipeline(location, codec)
        self._pipe.set_state(Gst.State.PLAYING)

    def stop(self):
        for ele, handler_id in self._handlers:
            try:
                if isinstance(ele, Gst.Bus):
                    ele.disconnect(handler_id)
                    ele.remove_signal_watch()
                else:
                    ele.disconnect(handler_id)
            except Exception as e:
                logger.warning(f"Failed to release: {ele} ({handler_id}) → {e}")
        self._handlers.clear()

        if self._pipe:
            self._pipe.set_state(Gst.State.NULL)
            self._pipe = None
        self._vq = None
        self._aq = None


class VideoAudioEncoder():

    def __init__(self):
        self._pipe: Gst.Pipeline | None = None
        self._vsrc: Gst.Element | None = None
        self._asrc: Gst.Element | None = None

        self._handlers: list = []

    @property
    def video_appsrc(self) -> Gst.Element | None:
        return self._vsrc

    @property
    def audio_appsrc(self) -> Gst.Element | None:
        return self._asrc

    def _handle_bus_message(self, bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            logger.error(f"[Gst Error] {err}: {dbg}")
            self.stop()
        elif msg.type == Gst.MessageType.EOS:
            logger.info("[Gst EOS] End of stream")
            self.stop()

    def _create_pipeline(self, codec: Pair, caps_str: Pair):
        desc = ""

        if codec.video and codec.video in VIDEO:
            encode_set = VIDEO[codec.video]["encode_set"]
            desc += "appsrc name=vsrc is-live=true block=true format=time ! "
            desc += "queue ! "
            desc += "videoconvert ! "
            desc += make_chain_desc(encode_set)
            desc += "mux. \n"

        if codec.audio and codec.audio in AUDIO:
            encode_set = AUDIO[codec.audio]["encode_set"]
            desc += "appsrc name=asrc is-live=true block=true format=time ! "
            desc += "queue ! "
            desc += "audioconvert ! "
            desc += make_chain_desc(encode_set)
            desc += "mux. \n"

        desc += "matroskamux name=mux ! "
        # <-- TODO: FAKE SINK
        desc += "fakesink \n"
        # desc += f"filesink location={datetime.now().strftime('%Y%m%d_%H%M%S')}.mkv \n"
        # -->

        self._pipe = Gst.parse_launch(desc)
        self._vsrc = self._pipe.get_by_name("vsrc")
        if self._vsrc:
            self._vsrc.set_property("caps", Gst.Caps.from_string(caps_str.video))
        self._asrc = self._pipe.get_by_name("asrc")
        if self._asrc:
            self._asrc.set_property("caps", Gst.Caps.from_string(caps_str.audio))

        bus = self._pipe.get_bus()
        bus.add_signal_watch()
        handler_id = bus.connect("message", self._handle_bus_message)
        self._handlers.append((bus, handler_id))

    def start(self, codec: Pair, caps_str: Pair):
        self.stop()
        self._create_pipeline(codec, caps_str)
        self._pipe.set_state(Gst.State.PLAYING)

    def stop(self):
        for ele, handler_id in self._handlers:
            try:
                if isinstance(ele, Gst.Bus):
                    ele.disconnect(handler_id)
                    ele.remove_signal_watch()
                else:
                    ele.disconnect(handler_id)
            except Exception as e:
                logger.warning(f"Failed to release: {ele} ({handler_id}) → {e}")
        self._handlers.clear()

        if self._pipe:
            self._pipe.set_state(Gst.State.NULL)
            self._pipe = None
        self._vsrc = None
        self._asrc = None


class RTSPStreamer():

    def __init__(self):
        self._loop: GLib.MainLoop | None = None
        self._loop_th: threading.Thread | None = None

        self._decoder: VideoAudioDecoder | None = None
        self._encoder: VideoAudioEncoder | None = None

        self._caps_str = Pair(None, "audio/x-raw,format=S16LE,channels=1,rate=8000,layout=interleaved")
        self._caps_ready = threading.Event()

        self._frame_queue: EvictingQueue | None = None
        self._frame_processor: SingleThreadTask | None = None

        self._video_feeder: SingleThreadTask | None = None
        self._audio_feeder: SingleThreadTask | None = None

        self._ws_queue: EvictingQueue | None = None

        self._channel = None
        self._stub = None

    @property
    def websocket_queue(self) -> EvictingQueue | None:
        return self._ws_queue

    def _process_frames(self):
        blob = self._decoder.video_queue.get()
        frame_yuv, pts, dts, duration = blob
        frame_bgr = cv2.cvtColor(frame_yuv, cv2.COLOR_YUV2BGR_I420)
        if not self._caps_str.video:
            h, w = frame_bgr.shape[:2]
            self._caps_str.video = f"video/x-raw,format=I420,width={w},height={h}"
            self._caps_ready.set()
        # <-- TODO: DE-IDENTIFICATION
        # draw_circle(frame_bgr)
        frame_bgr = deidentify(self._stub, frame_bgr)
        # -->
        # <-- TODO: PUT WEBSOCKET QUEUE
        _, jpg_buffer = cv2.imencode(".jpg", frame_bgr)
        jpg_bytes = jpg_buffer.tobytes()
        self._ws_queue.put(jpg_bytes)
        # -->
        frame_yuv = cv2.cvtColor(frame_bgr, cv2.COLOR_BGR2YUV_I420)
        frame_bytes = frame_yuv.tobytes()
        blob = (frame_bytes, pts, dts, duration)
        self._frame_queue.put(blob)

    def _feed_video(self, appsrc):
        blob = self._frame_queue.get()
        frame_bytes, pts, dts, duration = blob
        buf = Gst.Buffer.new_allocate(None, len(frame_bytes), None)
        buf.fill(0, frame_bytes)
        buf.pts = pts
        buf.dts = dts
        buf.duration = duration
        appsrc.emit("push-buffer", buf)

    def _feed_audio(self, appsrc):
        blob = self._decoder.audio_queue.get()
        audio_bytes, pts, dts, duration = blob
        buf = Gst.Buffer.new_allocate(None, len(audio_bytes), None)
        buf.fill(0, audio_bytes)
        buf.pts = pts
        buf.dts = dts
        buf.duration = duration
        appsrc.emit("push-buffer", buf)

    def start_streaming(self, location: str):
        if self._loop is None:
            self._loop = GLib.MainLoop()
            self._loop_th = threading.Thread(target=self._loop.run)
            self._loop_th.start()

        codec = extract_codec(location)

        self._decoder = VideoAudioDecoder()
        self._decoder.start(location, codec)

        if self._decoder.video_queue:
            # < -- TODO: gRPC, WEBSOCKET
            self._channel = grpc.insecure_channel("0.0.0.0:12932")
            self._stub = frame_pb2_grpc.AnalyzerServiceStub(self._channel)
            self._ws_queue = EvictingQueue()
            # -->
            self._frame_queue = EvictingQueue()
            self._frame_processor = SingleThreadTask("frame_processor")
            self._frame_processor.start(self._process_frames)
        
        self._caps_ready.wait()

        self._encoder = VideoAudioEncoder()
        self._encoder.start(codec, self._caps_str)

        if self._encoder.video_appsrc:
            self._video_feeder = SingleThreadTask("video_feeder")
            self._video_feeder.start(
                self._feed_video, [self._encoder.video_appsrc])
        if self._encoder.audio_appsrc:
            self._audio_feeder = SingleThreadTask("audio_feeder")
            self._audio_feeder.start(
                self._feed_audio, [self._encoder.audio_appsrc])

    def stop_streaming(self):
        if self._video_feeder:
            self._video_feeder.stop()
            self._video_feeder = None
        if self._audio_feeder:
            self._audio_feeder.stop()
            self._audio_feeder = None
        if self._frame_processor:
            self._frame_processor.stop()
            self._frame_processor = None

        if self._loop:
            self._loop.quit()
            if self._loop_th:
                self._loop_th.join()
            self._loop = None
            self._loop_th = None

        if self._encoder:
            self._encoder.stop()
            self._encoder = None
        if self._decoder:
            self._decoder.stop()
            self._decoder = None

        if self._channel is not None:
            self._channel.close()
            self._channel = None
        self._stub = None
