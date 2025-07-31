from typing import Callable, Optional, Dict, Any, AsyncGenerator, Tuple
import datetime
import asyncio
import json
import time

from .QXClient import QXClientAsync
from .exceptions import AboveExtractableLimit, WebsocketClosed
from .utils import proc_raw_data_list, proc_raw_data_dict, dcd_tmp
from .enums import STATE


class QXMilkerAsync(QXClientAsync):
    def __init__(self, session_id: str, network_timeout: Optional[int] = 3) -> None:
        super().__init__(session_id, network_timeout)
        self.__wait = 1
        self.__candles_data = {}
        self.__new_end_time = None
        self.__live_data_handler = None
        self.__latest_candle_data = {"block": 1}

        self.segs = 0

    def proc_live_data(self, data):
        # first byte is unwanted thing
        data = json.loads(data.decode()[1:])[0]
        data = dict(zip(['asset', 'time', 'price', "tick"], data))
        if self.__live_data_handler:
            self.__live_data_handler(data)

    def proc_latest_data(self, data):
        # first byte is unwanted thing
        data = json.loads(data.decode()[1:])

        """
        if 2 instances for same account are running, then if follow_candle is called 1st instance, 
        History/list/v2 will come.
        History/list/v2 will also be received in instance 2. so we introduce a variable called block.
        Which only allows proc_latest_data to process data when it is called from the current instance.
        we need to check asset and period too.
        """
        if (self.__latest_candle_data['block'] or
            data['asset'] != self.__latest_candle_data.get('asset') or
            data['period'] != self.__latest_candle_data.get('timeframe')):
            return

        self.__latest_candle_data['data'] = proc_raw_data_list(data['history'], self.__latest_candle_data['timeframe'])
        if self.__latest_candle_data['timeframe'] >= 60:
            # data['candles'] is in descending order of time
            self.remove_excess(data['candles'][0][0], self.__latest_candle_data['data'])
            data['candles'].reverse()
            keys = ["time", "open", "close", "high", "low", "tick", "last_tick"]
            self.__latest_candle_data["data"] = ([dict(zip(keys, value)) for value in data['candles'][:-1]] +
                                                 self.__latest_candle_data['data'])

        self.__latest_candle_data['block'] = 1
        self.__wait = 0

    def proc_candle_data(self, data):
        timeframe = self.__candles_data['timeframe']
        data = json.loads(data.decode()[1:])

        """
        Protocol fix
        assume 2 instances of same acc are running. 
        If history/load is called in one instance, then the reply from server reaches both the running instances. 
        This applies to n instances. The reply for the request, an instance made will come along with the index with which it is requested.
        So eliminate other requests by checking index.
        """

        if data['index'] != self.current_request_index:
            return

        candle_data = data['data']

        if len(list(data['data'][0].keys())) != 9 and timeframe < 60:  # detect raw data   
            # with open(f"raw_seg{self.segs}.json", "w") as f:
            #     json.dump(candle_data, f, indent=4)
            #     self.segs += 1

            candle_data = proc_raw_data_dict(data['data'], timeframe)

        elif len(list(data['data'][0].keys())) == 9 and timeframe > 60:
            candle_data = [{k: v for k, v in entry.items() if k not in ['asset', 'symbol_id']} for entry in candle_data]

        # Additional check
        recvd_data_last_candle = candle_data[-1]
        recvd_data_last_candle_time = recvd_data_last_candle['time']
        recvd_data_last_candle_close = recvd_data_last_candle['close']
        endtime_check = self.__new_end_time if timeframe >= 60 else self.__new_end_time - 3 # subtract the offset
        if endtime_check != recvd_data_last_candle_time:
            print(f"Endtime check failed\nRequired: {endtime_check}, RECVD: {candle_data[-1]['time']}")
            print("Creating empty data")
            import datetime as dt
            print(f"last candle time: {dt.datetime.fromtimestamp(recvd_data_last_candle_time)}")
            print(f"endtime check: {dt.datetime.fromtimestamp(endtime_check)}")
            
            empty_data = [{
                "time": recvd_data_last_candle_time + (i * timeframe) ,
                "open": recvd_data_last_candle_close,
                "close": recvd_data_last_candle_close,
                "high": recvd_data_last_candle_close,
                "low": recvd_data_last_candle_close,
                "ticks": 0,
                "last_tick": None
            } for i in range(1, int((endtime_check - recvd_data_last_candle_time) / timeframe) + 1)]

            candle_data = candle_data + empty_data
        
        self.__candles_data['data'] = candle_data + self.__candles_data['data'] # Data will be in Ascending order
        self.__wait = 0

    async def extract_data_iter(self, asset: str, timeframe: int, start_time: float, end_time: float) \
            -> AsyncGenerator[Tuple[Any, Optional[float]], Any]:
        limit_for_extraction = self.calc_limit_for_extraction(timeframe)
        if end_time > limit_for_extraction:
            raise AboveExtractableLimit(f"Can't extract above {dcd_tmp(limit_for_extraction)}")

        tot_candles = int(((end_time - start_time) // timeframe) + 1)
        await self.follow_candle(asset, timeframe)
        self.__candles_data = {"asset": asset, "timeframe": timeframe, "data": []}

        """
        remember endtime is inclusive
        eg: end_time: 1733718600
        then get_candles will give raw data from some start to end like this
        say timeframe is 5s
        then end will be like this 1733718604.991 (upto last update for candle 1733718600)
        In this case no transition will be detected in proc_raw_data. so add some value, it should not exceed minimum timeframe.
        add 1 - 4 to end_time. Here 3 is used.
        This will give some extra data, here upto 1733718607.991
        Now we have transition
        """
        if timeframe < 60: end_time += 3  # For transition
        self.__new_end_time = end_time

        n_candles = 0
        current_length = 0
        while end_time > start_time:
            await self.get_candles(asset, end_time, timeframe)
            while self.__wait:
                # Wait until response for current request is received
                if self.state != STATE.RUNNING:
                    raise WebsocketClosed("Network error.")
                await asyncio.sleep(0.01)

            # We should do this first because user can call break after the yield statement
            # if this is after the yield statement, then the blocker will not be reset.
            # So when they call again, it will not wait until data comes, self.__candles_data['data'] will be empty.
            # That will cause error
            self.__wait = 1  # reset blocker

            end_time = self.__candles_data['data'][0]['time'] - timeframe

            current_length = min(len(self.__candles_data['data']), tot_candles)

            # Terminate at last iteration
            if current_length == tot_candles: break

            # yield current chunk of data and next end time
            yield self.__candles_data['data'][0:current_length - n_candles], end_time
            n_candles = current_length

            if timeframe < 60: end_time += 3  # add some time to have transition
            self.__new_end_time = end_time

        # remove the excess candles that are before out start time
        self.remove_excess(start_time, self.__candles_data['data'])

        # yield last chunk of data
        yield self.__candles_data['data'][0:current_length - n_candles], None

    async def extract_data(self, asset: str, timeframe: int, start_time: float, end_time: float,
                           progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        tot_candles = int(((end_time - start_time) // timeframe) + 1)
        n_candles = 0
        async for data in self.extract_data_iter(asset, timeframe, start_time, end_time):
            n_candles += len(data[0])
            if progress_callback:
                progress_callback(
                    n_candles if n_candles < tot_candles else tot_candles,
                    tot_candles
                )
        return self.__candles_data

    async def extract_latest_data(self, asset: str, timeframe: int) -> Dict[str, Any]:
        self.__latest_candle_data = {"asset": asset, "timeframe": timeframe, "data": [], "block": 0}
        await self.follow_candle(asset, timeframe)
        while self.__wait:
            if self.state != STATE.RUNNING:
                raise WebsocketClosed("Network error.")
            await asyncio.sleep(0.1)
        self.__wait = 1
        return {key: val for key, val in self.__latest_candle_data.items() if key != "block"}

    async def extract_day(self, _date: datetime.date, asset: str, timeframe: int,
                    progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        if _date == datetime.date.today():
            raise AboveExtractableLimit(f"Cannot extract today's data")

        start_time = datetime.datetime.combine(_date, datetime.time(0, 0, 0))
        end_time = datetime.datetime.combine(_date, datetime.time(23, 59, 59))
        end_time = end_time - datetime.timedelta(seconds=timeframe - 1)
        return await self.extract_data(asset, timeframe, start_time.timestamp(), end_time.timestamp(), progress_callback)

    async def extract_n_candles_before(self, end_time: datetime.datetime, asset: str, timeframe: int, n_candles: int,
                                 progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        end_time = end_time.timestamp()
        start_time = end_time - ((n_candles - 1) * timeframe)
        return await self.extract_data(asset, timeframe, start_time, end_time, progress_callback)

    async def extract_n_candles_after(self, start_time: datetime.datetime, asset: str, timeframe: int, n_candles: int,
                                progress_callback: Optional[Callable[[int, int], None]] = None) -> Dict[str, Any]:
        start_time_stmp = start_time.timestamp()
        end_time = start_time_stmp + ((n_candles - 1) * timeframe)

        limit_for_extraction = self.calc_limit_for_extraction(timeframe)
        if end_time > limit_for_extraction:
            av_cndl = self.get_n_candles_available_to_extract_after_t(start_time, timeframe)
            raise AboveExtractableLimit(f"Cannot extract more than {av_cndl} candles in this timeframe({timeframe})")

        return await self.extract_data(asset, timeframe, start_time_stmp, end_time, progress_callback)

    async def extract_day_iter(self, _date: datetime.date, asset: str, timeframe: int):
        if _date == datetime.date.today():
            raise AboveExtractableLimit(f"Cannot extract today's data")

        start_time = datetime.datetime.combine(_date, datetime.time(0, 0, 0))
        end_time = datetime.datetime.combine(_date, datetime.time(23, 59, 59))
        end_time = end_time - datetime.timedelta(seconds=timeframe - 1)

        # Use extract_data_iter to retrieve data in chunks
        async for item in self.extract_data_iter(
            asset=asset,
            timeframe=timeframe,
            start_time=start_time.timestamp(),
            end_time=end_time.timestamp()
        ):
            yield item

    async def extract_n_candles_before_iter(self, end_time: datetime.datetime, asset: str, timeframe: int, n_candles: int):
        end_time = end_time.timestamp()
        start_time = end_time - ((n_candles - 1) * timeframe)

        remaining_candles = n_candles
        async for data in self.extract_data_iter(
            asset=asset,
            timeframe=timeframe,
            start_time=start_time,
            end_time=end_time
        ):
            remaining_candles -= len(data[0])

            # yield the candlestick data, no of remaining_candle, new end_time
            yield data[0], remaining_candles, data[0][0]['time'] - timeframe

    async def extract_n_candles_after_iter(self, start_time: datetime.datetime, asset: str, timeframe: int, n_candles: int):
        start_time_stmp = start_time.timestamp()
        end_time = start_time_stmp + ((n_candles - 1) * timeframe)

        limit_for_extraction = self.calc_limit_for_extraction(timeframe)
        if end_time > limit_for_extraction:
            av_cndl = self.get_n_candles_available_to_extract_after_t(start_time, timeframe)
            raise AboveExtractableLimit(f"Cannot extract more than {av_cndl} candles in this timeframe({timeframe})")

        remaining_candles = n_candles
        async for data in self.extract_data_iter(
            asset=asset,
            timeframe=timeframe,
            start_time=start_time_stmp,
            end_time=end_time
        ):
            remaining_candles -= len(data[0])
            yield data[0], remaining_candles

    async def extract_candle(self, asset: str, timeframe: int, _time: float) -> Dict[str, Any]:
        data = await self.extract_data(asset, timeframe, _time, _time + timeframe)
        del data['data'][-1]
        return data

    def set_live_data_handler(self, func: Callable[[Dict[str, Any]], None]) -> None:
        self.__live_data_handler = func

    @staticmethod
    def remove_excess(start_time, candlestick_data):
        candlestick_data[:] = [
            itm for itm in candlestick_data
            if itm['time'] >= start_time
        ]

    @staticmethod
    def calc_limit_for_extraction(timeframe: int) -> int:
        tm_now = int(time.time())
        return tm_now - (tm_now % 300) - timeframe

    @staticmethod
    def get_n_candles_available_to_extract_after_t(start_time: datetime.datetime, timeframe: int) -> int:
        """ Calculate the no of candles we can extract after given time """
        limit_for_extraction = QXMilkerAsync.calc_limit_for_extraction(timeframe)
        return int((limit_for_extraction - start_time.timestamp()) / timeframe) + 1

    @staticmethod
    def calc_n_candles(start_time: float, end_time: float, timeframe: int):
        """ Calculates the no of candles inside the given interval """
        return int(((end_time - start_time) // timeframe) + 1)
