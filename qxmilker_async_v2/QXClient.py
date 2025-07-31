from subprocess import TimeoutExpired
from typing import Callable, Dict, Any
import websockets
import certifi
import time
import json
import ssl

from .enums import *
from .utils import *

import socket

class QXClientAsync:

    # User-Agent is mandatory
    # origin is mandatory for making connection
    # But if origin is in headers, it has no effect.
    # It should be present in origin param in websockets.connect
    # I think the origin should be passed only on params....
    # Host is not needed at all. We can connect without any issue without host. But let it be.
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0",
        "Origin": "https://qxbroker.com",
        "Host": f"ws2.qxbroker.com",
    }
    uri =  "wss://ws2.qxbroker.com/socket.io/?EIO=3&transport=websocket"

    def __init__(self, ssid, network_timeout=3, current_request_index_multiplier=100):
        self.ssid = ssid
        self.network_timeout = network_timeout

        # Create ssl context for making connection
        self.__ssl_context = self.__create_ssl_context()

        # state vars
        self.opened = False
        self.__auth_state = AUTH_STATE.NOT_AUTHORIZED
        self.__state = STATE.INITIALIZED

        """
        network timeout disconnector disconnects after timeout mentioned in network_timeout var
        It calls self.__app.close() which means it would not be disconnected exactly at time mentioned in network_timeout 
        self.__app.close() will close immediately most of the time But sometimes it takes more time

        network timeout disconnector only disconnects if it is waiting for a message for server

        client waits for message when connection is opened ie it waits for the first 2 msgs
        After that we should send SSID and then it waits for AUTH STATUS. After AUTH STATUS is received the disconnector stops/

        network disconnector is also started when follow candle or get_candle is called.
        after follow candle is called the network timeout disconnetor will not stop, it will only reset.
        beacuse we need to wait for the realtime data. realtime candle comes every some milliseconds.
        """
        self.__network_timeout_disconnector = AsyncPeriodicFunction(self.network_timeout, self.__stop)


        """
        we need to send '42["tick"]' every 20 seconds to qxbroker otherwise connection will be closed by the server. 
        I dont know why 20 seconds(Copied from Cleiton).
        """
        self.__periodic_ticker = AsyncPeriodicFunction(20, self.__send_tick)

        # __handler is for saving user defined handler for various situations like on_open, on_network_timeout, ....
        # Dont do time consuming tasks in handlers!!!
        self.__handlers = {}

        # It is for forwarding the binary messages to respective callbacks for processing
        self.__forward_nxt_bin_msg = None

        # It is used for logging purposes
        self.__msg_trk_clbk = None

        # when follow candle is called the asset is added to this list for some purpose. refer follow_candle function for more info.
        self.__listening_instruments = []

        # It saves all asset's info from Quotex
        self.__all_assets = {}

        self.__lock = asyncio.Lock()
        self.__wss = None

        # event
        self.__auth_event = asyncio.Event()

        self.__coroutine_holder = []


        # reply for request comes along with the index we sent to the server.
        # Thus it helps to distinguish between mulitple replies from server when having concurrent connections in same acc.

        self.current_request_index = None

        # sometimes when having this multiplier as 100, some workers generated same indices. 
        # This caused replies from server indistinguishable. It is Because it runs so fast.
        # eg: time.time() = 1744771832.0352356
        # changing multiplier to 1000 will give more precision.
        # increase multiplier in order of 10, if this kind of error occurs.
        # This is one solution.
        # Otherwise add time.sleep(small_vals such as 0.01) to the get_candles function.
        self.current_request_index_multiplier = current_request_index_multiplier

    async def main(self):
        try:
            async with websockets.connect(
                uri=self.uri, host=self.headers['Host'], origin=self.headers['Origin'],
                ping_interval=24, ping_timeout=20, additional_headers=self.headers,
                ssl=self.__ssl_context, close_timeout=0, open_timeout=self.network_timeout
            ) as ws:
                # setup something in on_open
                await self.__on_open(ws)

                # read messages
                async for msg in ws:
                    await self.__on_message(ws, msg)

        except asyncio.CancelledError:
            pass

        except websockets.exceptions.ConnectionClosedError as e:
            """
            this is called when connection is closed with some error.
            raised when close frame is not received.
            qxbroker doesnt send close frame when self.__wss.close() is called
            so when close is called it always raises this error
            """
            pass

        except TimeoutError:
            # if network speed very low, open_timeout is triggered
            self.__auth_state = AUTH_STATE.AUTH_FAILURE
            self.__auth_event.set()

        except socket.gaierror:
            # if connect is called when there is no network it raises socket.gaierror
            self.__auth_state = AUTH_STATE.AUTH_FAILURE
            self.__auth_event.set()

        else:
            pass

        finally:
            self.__on_close()

            # Use when debugging
            if not self.__auth_event.is_set():
                self.__auth_state = AUTH_STATE.AUTH_FAILURE
                self.__auth_event.set()

    async def __on_open(self, wss):
        self.opened = True
        self.__wss = wss
        self.__state = STATE.RUNNING

        # notify
        if callback := self.__handlers.get("open_handler"): callback()

        # We need to start waiting for first 2 messages
        self.__network_timeout_disconnector.start()

    async def __on_message(self, wss, msg):
        self.__send_msg_to_clbk(msg, MESSAGE.RECVD)
        str_msg = str(msg)

        if isinstance(msg, bytes) and self.__forward_nxt_bin_msg:
            # Forward bin msgs to respective callbacks
            if self.__forward_nxt_bin_msg == self.proc_candle_data:
                self.__network_timeout_disconnector.reset()

            self.__forward_nxt_bin_msg(msg)
            self.__forward_nxt_bin_msg = None

        if str_msg == '0{"sid":"':
            # reset disconnector and wait for "40"
            self.__network_timeout_disconnector.reset()

        elif str_msg == '40':
            # send the ssid after "40"
            await self.__send_ssid()

            # reset disconnector and wait for AUTH STATUS
            self.__network_timeout_disconnector.reset()

        elif str_msg == '42["s_authorization"]':
            self.__auth_state = AUTH_STATE.AUTH_SUCCESS
            self.__auth_event.set()

            # If AUTH SUCCESS stop the timer. Start disconnector after we start waiting for another msg.
            self.__network_timeout_disconnector.stop()

            # Start the ticker
            self.__periodic_ticker.start()

            # Some payload is sent always after AUTH SUCCESS. So send it.
            await self.__send_initial_payload()

        elif "authorization/reject" in str_msg:
            """
            We dont need to close/stop anything because after this __on_close will be called automatically.
            """
            self.__auth_state = AUTH_STATE.AUTH_REJECTED
            self.__auth_event.set()

        elif '451-["instruments/list"' in str_msg:
            """ After this msg all asset's data in quotex will be sent as binary msg"""
            self.__forward_nxt_bin_msg = self.__proc_assets_data

        elif "history/load" in str_msg:
            """ After this msg, requested historical data will be sent as binary msg"""
            self.__forward_nxt_bin_msg = self.proc_candle_data

        elif '451-["history/list/v2"' in str_msg:
            """ After this msg, the latest data(unprocessed raw data) will be sent as binary msg"""
            self.__forward_nxt_bin_msg = self.proc_latest_data

        elif '451-["quotes/stream"' in str_msg:
            """ After this msg, current price of asset is sent as binary msg"""
            self.__forward_nxt_bin_msg = self.proc_live_data
            self.__network_timeout_disconnector.reset()

        elif str_msg == "41":
            """ After this msg, the Connection will be closed by the server """
            pass

    def __on_close(self):
        self.cleanup()

        # Call it after everything is closed
        if clbk := self.__handlers.get('closed_handler'): clbk()

    def __on_error(self, error):
        if clbk := self.__handlers.get("error_handler"): clbk(error)


    async def __stop(self):
        if clbk := self.__handlers.get('network_timeout_handler'): clbk()
        await self.stop()

    async def stop(self) -> None:
        # IDK why sometimes self.__periodic_ticker.stop() doesnt work
        # so implemented a method called kill. It works good
        self.__periodic_ticker.kill()
        self.__network_timeout_disconnector.kill()
        await self.__wss.close()
        self.__state = STATE.CLOSED

        # release the __auth_event
        if not self.__auth_event.is_set():
            self.__auth_state = AUTH_STATE.AUTH_FAILURE
            self.__auth_event.set()

    async def start(self):
        self.__coroutine_holder.append(asyncio.create_task(self.main()))
        await self.__auth_event.wait()
        return self.auth_state

    def cleanup(self):
        if self.__state != STATE.CLOSED:
            self.__network_timeout_disconnector.kill()
            self.__periodic_ticker.kill()
            self.__state = STATE.CLOSED

    async def __send_websocket_message(self, data):
        async with self.__lock:
            await self.__wss.send(data)

    async def __send_tick(self):
        msg = '42["tick"]'
        await self.__send_websocket_message(msg)
        self.__send_msg_to_clbk(msg, MESSAGE.SENT)

    async def __send_ssid(self):
        payload = {"session": self.ssid, "isDemo": 1, "tournamentId": 0}
        msg = f'42["authorization", {json.dumps(payload)}]'
        await self.__send_websocket_message(msg)
        self.__send_msg_to_clbk(msg, MESSAGE.SENT)

    async def __send_initial_payload(self):
        """ some msgs are sent everytime after AUTH_SUCCESS everytime. Send it for additional safety """
        msgs = ['42["tick"]','42["balance/list"]','42["indicator/list"]', '42["drawing/load"]','42["pending/list"]']
        for msg in msgs:
            await self.__send_websocket_message(msg)
            self.__send_msg_to_clbk(msg, MESSAGE.SENT)

    async def follow_candle(self, asset: str, period: int) -> None:
        msgs = [
            f'42["instruments/update", {{"asset": "{asset}", "period": {period}}}]'
        ]

        additional_msgs = [
            f'42["chart_notification/get", {{"asset": "{asset}", "version": "1.0.0"}}]',
            f'42["depth/unfollow", "{asset}"]',
            f'42["depth/follow", "{asset}"]'
        ]

        # Only on the first time these additional messages are sent
        if asset not in self.__listening_instruments:
            msgs = msgs + additional_msgs

            # Append it avoid sending additional messages on the second time
            self.__listening_instruments.append(asset)

        for msg in msgs:
            await self.__send_websocket_message(msg)
            self.__send_msg_to_clbk(msg, MESSAGE.SENT)

        # If we are not already waiting for a message, start disconnector
        if not self.__network_timeout_disconnector.is_running():
            self.__network_timeout_disconnector.start()

    async def get_candles(self, asset, end_time, timeframe):
        """
        fetches some candles(49 I think) until end time. End time inclusive
        follow_candle should be called before using this
        """

        # uuid4().time doesn't work
        self.current_request_index = int(time.time() * self.current_request_index_multiplier)

        # Additional precautionary step. Sometimes multiplier doesn't work.
        # This is necessary.
        time.sleep(0.02)
        
        payload = {
            "asset": asset,
            "index": self.current_request_index, # For distinguishing
            "offset": timeframe * 60 if timeframe >= 60 else 3000,
            "period": timeframe,
            "time": (datetime.datetime.fromtimestamp(end_time) + datetime.timedelta(seconds=timeframe)).timestamp()
        }
        msg = f'42["history/load", {json.dumps(payload)}]'
        await self.__send_websocket_message(msg)
        self.__send_msg_to_clbk(msg, MESSAGE.SENT)

        # If we are not already waiting for a message, start disconnector
        if not self.__network_timeout_disconnector.is_running():
            self.__network_timeout_disconnector.start()

    def __proc_assets_data(self, data):
        """ Method to process assets data """
        data = json.loads(data.decode()[1:])
        self.__all_assets = {"currency" : [], "stock": [], "commodity": [], "cryptocurrency": []}
        for itm in data:
            asset_data = {"asset": itm[1], "name": itm[2].strip(), "availability": itm[14]}
            self.__all_assets[itm[3]].append(asset_data)

        if clbk := self.__handlers.get("asset_data_update_handler"): clbk(self.__all_assets)

    def get_all_assets(self) -> Dict[str, Any]:
        """ Method to get all assets in quotex """
        return self.__all_assets

    def get_available_assets(self) -> Dict[str, Any]:
        """ Method to get currently available assets """
        available_assets = {}
        for asset_type, asset_data in self.__all_assets.items():
            available_assets[asset_type] = [itm for itm in self.__all_assets[asset_type] if itm['availability']]
        return available_assets

    def get_unavailable_assets(self) -> Dict[str, Any]:
        """ Method to get currently unavailable assets """
        available_assets = {}
        for asset_type, asset_data in self.__all_assets.items():
            available_assets[asset_type] = [itm for itm in self.__all_assets[asset_type] if not itm['availability']]
        return available_assets

    def proc_candle_data(self, data):
        # Override in base class
        raise Exception("Override in base class")

    def proc_latest_data(self, data):
        # Override in base class
        raise Exception("Override in base class")

    def proc_live_data(self, data):
        # Override in base class
        raise Exception("Override in base class")

    def set_msg_tracker(self, func: Callable[[str | bytes, MESSAGE], None]) -> None:
        self.__msg_trk_clbk = func

    def __send_msg_to_clbk(self, data, _type):
        """ sends all messages(SENT and RECVD) to user defined callback """
        if self.__msg_trk_clbk:
            self.__msg_trk_clbk(data, _type)

    def set_asset_data_update_handler(self, func: Callable[[Dict[str, Any]], None]) -> None:
        """ Assets data is updated every some interval, In that time the handler is called """
        self.__handlers['asset_data_update_handler'] = func

    def set_network_timeout_handler(self, func: Callable[[], None]) -> None:
        """ network_timeout_handler is called after __network_timeout_disconnector is timed out, before self.stop() """
        self.__handlers['network_timeout_handler'] = func

    def set_closed_handler(self, func: Callable[[], None]) -> None:
        """ closed_handler is called when connection is closed """
        self.__handlers['closed_handler'] = func

    def set_open_handler(self, func: Callable[[], None]) -> None:
        """ open_handler is called when connection is opened with the server """
        self.__handlers['open_handler'] = func

    def set_error_handler(self, func: Callable[[Exception], None]) -> None:
        """ error handler is called when error is occurred """
        self.__handlers['error_handler'] = func

    @property
    def auth_state(self) -> AUTH_STATE:
        return self.__auth_state

    @property
    def state(self) -> STATE:
        return self.__state

    @staticmethod
    def __create_ssl_context():
        # ssl context is mandatory for making connection with brokers.
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ssl_context.load_verify_locations(certifi.where())
        ssl_context.minimum_version = ssl.TLSVersion.TLSv1_3
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        return ssl_context
