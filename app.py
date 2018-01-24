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
from zmapi.zmq.utils import ident_to_str, split_message
from zmapi.logging import setup_root_logger
from zmapi.exceptions import *
import uuid
from time import time, gmtime
from datetime import datetime
from pprint import pprint, pformat

################################## CONSTANTS ##################################

MODULE_NAME = "frontend"

EXCEPTION_TO_ECODE = {
    DecodingException: error.DECODE,
    CodecException: error.CODEC,
    InvalidArgumentsException: error.ARGS,
}

################################### GLOBALS ###################################

L = logging.root

class GlobalState:
    pass
g = GlobalState()
g.loop = asyncio.get_event_loop()
g.ctx = zmq.asyncio.Context()
g.startup_time = datetime.utcnow()
g.num_messages_handled = 0
g.num_msg_ids_added = 0
g.num_invalid_messages = 0

###############################################################################

PYTHON_TYPE_TO_ZMAPI_TYPE = {
    bool: "bool",
    int: "int",
    float: "float",
    str: "str",
    list: "list",
    dict: "record",
}

def check_spec(d, spec, optional):
    name = spec[0]
    dtype = spec[1]
    vmin = None
    vmax = None
    if len(spec) == 4:
        vmin = spec[2]
        vmax = spec[3]
    val = d.get(name, None)
    if optional and val is None:
        return
    if val is None:
        raise InvalidArgumentsException("missing field: {}".format(name))
    if not isinstance(val, dtype):
        raise InvalidArgumentsException(
                "field '{}' has wrong type, expected type: {}"
                .format(name, PYTHON_TYPE_TO_ZMAPI_TYPE[dtype]))
    if vmin is not None:
        if val < vmin:
            raise InvalidArgumentsException(
                    "field '{}' must have a value greater than {}"
                    .format(name, vmin))
    if vmax is not None:
        if val > vmax:
            raise InvalidArgumentsException(
                    "field '{}' must have a value less than {}"
                    .format(name, vmax))

def check_content(d, req_specs, opt_specs):
    for spec in req_specs:
        check_spec(d, spec, False)
    for spec in opt_specs:
        check_spec(d, spec, True)

# sanitize_* can be specs argument for check_content function or function
# defining the arguments checking procedure.

sanitize_modify_subscription = [
    [],
    [("ticker_id", str),
    ("order_book_speed", int, 0, 10),
    ("trades_speed", int, 0, 10),
    ("order_book_levels", int, 0, None),
    ("emit_quotes", bool)]
]

sanitize_unsubscribe = [[("ticker_id", str)]]

# sanitize_get_ticker_info = [[("ticker", dict)]]

sanitize_get_snapshot = [
    [("ticker_id", str)],
    [("daily_data", bool), ("order_book_levels", int, 0, None)]
]

sanitize_list_directory = [[("directory", str)]]

SANITIZERS = {}
def populate_sanitizers():
    for k in list(globals()):
        if k.startswith("sanitize_"):
            cmd = k[9:]
            v = globals()[k]
            if callable(v):
                SANITIZERS[cmd] = v
            else:
                req = v[0] if v[0] is not None else []
                opt = []
                if len(v) == 2:
                    opt = v[1] if v[1] is not None else []
                f = lambda x, req=req, opt=opt: \
                        check_content(x, req, opt)
                SANITIZERS[cmd] = f
populate_sanitizers()
            
###############################################################################

async def send_error(ident, msg_id, ecode, what=None):
    g.num_invalid_messages += 1
    if isinstance(what, Exception):
        what = "{}: {}".format(type(what).__name__, what)
    msg = error.gen_error(ecode, what)
    msg["msg_id"] = msg_id
    msg = " " + json.dumps(msg)
    msg = msg.encode()
    await g.sock_ctl.send_multipart(ident + [b"", msg])

def sanitize_msg(msg):
    sanitizer = SANITIZERS.get(msg["command"])
    if sanitizer:
        if "content" not in msg:
            raise InvalidArgumentsException("content missing")
        sanitizer(msg["content"])

async def handle_msg_2(ident, msg):
    cmd = msg.get("command")
    if not cmd:
        raise InvalidArgumentsException("command missing")
    sanitize_msg(msg)
    debug_str = "ident={}, command={}, msg_id={}"
    debug_str = debug_str.format(ident_to_str(ident), cmd, msg["msg_id"])
    L.debug("> " + debug_str)
    msg_bytes = " " + json.dumps(msg)
    msg_bytes = msg_bytes.encode()
    await g.sock_deal.send_multipart(ident + [b"", msg_bytes])
    msg_parts = await g.sock_deal_pub.poll_for_msg_id(msg["msg_id"])
    if cmd == "get_status":
        msg = json.loads(msg_parts[-1].decode())
        status = {
            "name": MODULE_NAME,
            "uptime": (datetime.utcnow() - g.startup_time).total_seconds(),
            "num_messages_handled": g.num_messages_handled,
            "num_msg_ids_added": g.num_msg_ids_added,
            "num_invalid_messages": g.num_invalid_messages,
        }
        msg["content"].insert(0, status)
        msg_bytes = (" " + json.dumps(msg)).encode()
        msg_parts[-1] = msg_bytes
    L.debug("< " + debug_str)
    await g.sock_ctl.send_multipart(msg_parts)


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

async def handle_msg_1(ident, raw_msg):
    g.num_messages_handled += 1
    try:
        msg = parse_message(raw_msg)
        if "msg_id" not in msg:
            msg["msg_id"] = str(uuid.uuid4())
            g.num_msg_ids_added += 1
        else:
            msg["msg_id"] = str(msg["msg_id"])
        msg_id = msg["msg_id"]
        res = await handle_msg_2(ident, msg)
    except Exception as e:
        L.exception("{} on msg_id: {}".format(type(e).__name__, msg_id))
        ecode = EXCEPTION_TO_ECODE.get(type(e), error.GENERIC)
        await send_error(ident, msg_id, ecode,
                         e if ecode == error.GENERIC else str(e))

async def run_ctl_interceptor():
    L.info("run_ctl_interceptor running ...")
    while True:
        msg_parts = await g.sock_ctl.recv_multipart()
        try:
            ident, msg = split_message(msg_parts)
        except ValueError as err:
            L.error(str(err))
            g.num_invalid_messages += 1
            continue
        if len(msg) == 0:
            # handle ping message
            await g.sock_deal.send_multipart(msg_parts)
            res = await g.sock_deal_pub.poll_for_pong()
            await g.sock_ctl.send_multipart(res)
            continue
        create_task(handle_msg_1(ident, msg))

###############################################################################

def parse_args():
    desc = "Input sanitizer for CTL sockets"
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

def setup_logging(args):
    setup_root_logger(args.log_level)

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
    setup_logging(args)
    init_zmq_sockets(args)
    tasks = [
        create_task(g.sock_deal_pub.run()),
        create_task(run_ctl_interceptor()),
    ]
    g.loop.run_until_complete(asyncio.gather(*tasks))

if __name__ == "__main__":
    main()
