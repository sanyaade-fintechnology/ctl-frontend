import zmq
import zmq.asyncio
import asyncio
from asyncio import ensure_future as create_task
import sys
import logging
import json
import argparse
from zmapi.codes import error
from zmapi.zmq import SockRecvPublisher
import uuid
from time import time, gmtime
from pprint import pprint, pformat

################################## CONSTANTS ##################################

MODULE_NAME = "frontend"

################################# EXCEPTIONS ##################################

class CodecException(Exception):
    pass

class DecodingException(Exception):
    pass

class InvalidArguments(Exception):
    pass

class CommandNotImplemented(Exception):
    pass

################################### GLOBALS ###################################

L = None

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()

################################### HELPERS ###################################

def ident_to_str(ident):
    return "/".join([x.decode("latin-1").replace("/", "\/") for x in ident])

###############################################################################

async def run_ctl_interceptor():
    L.info("run_ctl_interceptor running ...")
    while True:
        msg_parts = await g.sock_ctl.recv_multipart()
        try:
            ident, msg = split_message(msg_parts)
        except ValueError as err:
            L.error(str(err))
            continue
        if len(msg) == 0:
            # handle ping message
            await g.sock_deal.send_multipart(msg_parts)
            res = await g.sock_deal_pub.poll_for_pong()
            await g.sock_ctl.send_multipart(res)
            continue
        create_task(handle_msg_1(ident, msg))

def split_message(msg_parts):
    separator_idx = None
    for i, part in enumerate(msg_parts):
        if not part:
            separator_idx = i
            break
    if not separator_idx:
        raise ValueError("ident separator not found")
    ident = msg_parts[:separator_idx]
    msg = msg_parts[separator_idx+1]
    return ident, msg

async def handle_msg_1(ident, raw_msg):
    try:
        msg = parse_message(raw_msg)
        if "msg_id" not in msg:
            msg["msg_id"] = generate_msg_id()
        else:
            msg["msg_id"] = str(msg["msg_id"])
        res = await handle_msg_2(ident, msg)
    except DecodingException as e:
        L.exception("decoding exception:")
        await send_error(ident, msg["msg_id"], error.DECODE, str(e))
    except CodecException as e:
        L.exception("codec exception:")
        await send_error(ident, msg["msg_id"], error.CODEC)
    except InvalidArguments as e:
        L.exception("invalid arguments on message:")
        await send_error(ident, msg["msg_id"], error.ARGS, str(e))

async def send_error(ident, msg_id, ecode, msg=None):
    msg = error.gen_error(ecode, msg)
    msg["msg_id"] = msg_id
    msg = " " + json.dumps(msg)
    msg = msg.encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg])

def parse_message(msg):
    codec = int(msg[0])
    if codec != 32:
        raise CodecException()
    try:
        msg = msg.decode()
        msg = json.loads(msg)
    except Exception as e:
        raise DecodingException(e)
    return msg

async def handle_msg_2(ident, msg):
    cmd = msg.get("command")
    if not cmd:
        raise InvalidArguments("command missing")
    debug_str = "ident:{}, command:{}, msg_id:{}"
    debug_str = debug_str.format(ident_to_str(ident), cmd, msg["msg_id"])
    L.debug("> " + debug_str)
    msg_bytes = " " + json.dumps(msg)
    msg_bytes = msg_bytes.encode()
    await g.sock_deal.send_multipart(ident + [b"", msg_bytes])
    res = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    L.debug("< " + debug_str)
    await g.sock_ctl.send_multipart(res)

def generate_msg_id():
    return str(uuid.uuid4())

###############################################################################

def parse_args():
    desc = "input sanitizer for ctl sockets"
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument("ctl_addr_up",
                        help="address of the upstream ctl socket")
    parser.add_argument("ctl_addr_down",
                        help="ctl socket binding address")
    parser.add_argument("--log-level", default="INFO", help="logging level")
    args = parser.parse_args()
    try:
        args.log_level = int(args.log_level)
    except ValueError:
        pass
    return args

def build_logger(args):
    logging.root.setLevel(args.log_level)
    logger = logging.getLogger(__name__)
    logger.propagate = False
    logger.handlers.clear()
    fmt = "%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s"
    datefmt = "%H:%M:%S"
    formatter = logging.Formatter(fmt=fmt, datefmt=datefmt)
    # convert datetime to utc
    formatter.converter = gmtime
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger

def init_zmq_sockets(args):
    g.sock_deal = g.ctx.socket(zmq.DEALER)
    g.sock_deal.setsockopt_string(zmq.IDENTITY, MODULE_NAME)
    g.sock_deal.connect(args.ctl_addr_up)
    g.sock_deal_pub = SockRecvPublisher(g.ctx, g.sock_deal)
    g.sock_ctl = g.ctx.socket(zmq.ROUTER)
    g.sock_ctl.bind(args.ctl_addr_down)

def main():
    global L
    args = parse_args()
    L = build_logger(args)
    init_zmq_sockets(args)
    tasks = [
        create_task(g.sock_deal_pub.run()),
        create_task(run_ctl_interceptor()),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == "__main__":
    main()
