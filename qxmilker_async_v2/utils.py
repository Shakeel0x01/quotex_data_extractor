import datetime
import asyncio

class AsyncPeriodicFunction:
    def __init__(self, interval, function, *args, **kwargs):
        self.interval = interval
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.is_running_flag = False
        self.killed = False
        self.task = None

    async def __run(self):
        while self.is_running_flag and not self.killed:
            await asyncio.sleep(self.interval)
            await self.function(*self.args, **self.kwargs)

    def start(self):
        if not self.killed:
            if self.task is not None and not self.task.done(): self.stop()
            self.is_running_flag = True
            self.task = asyncio.create_task(self.__run())

    def stop(self):
        self.is_running_flag = False
        if self.task is not None:
            self.task.cancel()
            self.task = None

    def reset(self):
        if not self.killed:
            self.stop()
            self.start()

    def kill(self):
        if not self.killed:
            self.stop()
            self.killed = True

    def is_running(self):
        return self.is_running_flag

def proc_raw_data_dict(data: list, timeframe: int):
    "Input: list containing all the candles as dicts"

    candles = []
    prev_timestamp = int(data[0]['time'])
    prev_high = float("-inf")
    prev_low = float("inf")
    last_tick_time = None
    open_price = None
    last_price = None
    ticks = 0

    candle_end_time = prev_timestamp + (timeframe - prev_timestamp % timeframe)

    idx = 0
    while idx != len(data):
        timestamp = int(data[idx]['time'])
        price = data[idx]['price']
        tick = data[idx]['tick']

        if timestamp >= candle_end_time: # transition detected
            if not (candle_end_time - timeframe) <= prev_timestamp <= (candle_end_time) and idx != 0:
                candles.append(
                    {
                        "time": candle_end_time - timeframe,
                        "open": candles[-1]['close'],
                        "close": candles[-1]['close'],
                        "high": candles[-1]['close'],
                        "low": candles[-1]['close'],
                        "ticks": 0,
                        "last_tick": None
                    }
                )
                candle_end_time += timeframe
                continue

            # For previous candle
            candles.append({
                "time": candle_end_time - timeframe,
                "open": open_price,
                "close": last_price,
                "high": prev_high,
                "low": prev_low,
                "ticks": ticks,
                "last_tick": last_tick_time
            })

            # For new candle
            open_price = price
            last_tick_time = None
            prev_high = price
            prev_low = price
            ticks = 0

            candle_end_time += timeframe

        last_price = price
        # Update high and low
        prev_high = max(prev_high, price)
        prev_low = min(prev_low, price)

        # Update ticks and last_tick_time
        if tick:
            ticks += tick
            last_tick_time = data[idx]['time']

        prev_timestamp = timestamp

        idx += 1

    return candles[1:]

# def proc_raw_data_dict(data: list, timeframe: int):
#     "Input: list containing all the candles as dicts"

#     candles = []
#     prev_timestamp = int(data[0]['time'])
#     prev_high = float("-inf")
#     prev_low = float("inf")
#     last_tick_time = None
#     start_time = None
#     open_price = None
#     last_price = None
#     ticks = 0

#     candle_end_time = prev_timestamp + (timeframe - prev_timestamp % timeframe)
#     for itm in data[1:]:
#         timestamp = int(itm['time'])
#         price = itm['price']
#         tick = itm['tick']

#         if timestamp >= candle_end_time: # transition detected
#             # For previous candle
#             candles.append({
#                 "time": start_time,
#                 "open": open_price,
#                 "close": last_price,
#                 "high": prev_high,
#                 "low": prev_low,
#                 "ticks": ticks,
#                 "last_tick": last_tick_time
#             })

#             # For new candle
#             start_time = candle_end_time
#             open_price = price
#             last_tick_time = None
#             prev_high = price
#             prev_low = price
#             ticks = 0

#             candle_end_time += timeframe

#         last_price = price
#         # Update high and low
#         prev_high = max(prev_high, price)
#         prev_low = min(prev_low, price)

#         # Update ticks and last_tick_time
#         if tick:
#             ticks += tick
#             last_tick_time = itm['time']

#         prev_timestamp = timestamp

#     return candles[1:]

def proc_raw_data_list(data, timeframe):
    _data = []
    for itm in data:
        _data.append({"time": itm[0],"price": itm[1],"tick": itm[2]})
    return proc_raw_data_dict(_data, timeframe)

def dcd_tmp(tmp):
    return datetime.datetime.fromtimestamp(tmp).ctime()

def dcd_tmp_ms(tmp):
    dt = datetime.datetime.fromtimestamp(tmp)
    return dt.strftime("%a %b %d %H:%M:%S") + f":{int(dt.microsecond / 1000):03d}"