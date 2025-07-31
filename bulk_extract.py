from qxmilker_async_v2 import *
import argparse
import datetime
import asyncio
from rich.progress import (Progress, BarColumn, SpinnerColumn, TextColumn, Task, ProgressColumn, TimeElapsedColumn,
                           TimeRemainingColumn)
from rich.pretty import pprint
from rich import print
import json


parser = argparse.ArgumentParser(description="Extract data from Quotex")
parser.add_argument("-a", "--asset", type=str, help="Asset name", required=True)
parser.add_argument("-s", "--start", type=str, help="Start time (YYYY:MM:DD HH:MM:SS)", required=True)
parser.add_argument("-e", "--end", type=str, help="End time (YYYY:MM:DD HH:MM:SS)", required=True)
parser.add_argument("-t", "--timeframe", type=int, help="Timeframe in seconds", required=True)
parser.add_argument("-o", "--output", type=str, help="Output file", required=True)
parser.add_argument("-c", "--concurrency", type=int, help="Number of concurrent requests", default=10)
parser.add_argument("--ssid", type=str, help="Session ID", required=True)
args = parser.parse_args()

start = datetime.datetime.strptime(args.start, "%Y:%m:%d %H:%M:%S")
end = datetime.datetime.strptime(args.end, "%Y:%m:%d %H:%M:%S")

def get_time_ranges(start: float, end: float, parts: int, timeframe: int):
    ranges = []
    part_sz = (end - start) // parts
    timeframe_rounded_part_sz = part_sz + (timeframe - (part_sz % timeframe))

    _start = start
    while _start < end:
        _end = _start + timeframe_rounded_part_sz
        if _end > end:
            ranges.append((_start, end))
            break
        ranges.append((_start, _start + timeframe_rounded_part_sz))
        _start = _end + timeframe

    return ranges

def _msg_tracker(msg, tp):
    if len(msg) > 100: msg = msg[0:180]
    if tp == MESSAGE.SENT:
        print(f"[red]{tp}[/red]: [cyan]{msg}[/cyan]")
    else:
        print(f"[green]{tp}[/green]: [cyan]{msg}[/cyan]")

def merge_candlesticks(d1: list, d2: list):
    time_to_candle = {}

    for candle in d1:
        time_to_candle[candle["time"]] = candle
    for candle in d2:
        time_to_candle[candle["time"]] = candle

    return list(time_to_candle.values())

def dcd_tmp(tmp):
    return datetime.datetime.fromtimestamp(tmp).ctime()

class CandleColumn(ProgressColumn):
    def render(self, task: Task) -> str:
        return f"[cyan]{int(task.completed)}/{int(task.total)} candles"

async def fetch_data(asset, timeframe, start, end, ssid, prg_id, progress):
    retry_delay = 5

    next_end_time = end
    data = {
        'data': [],
        'timeframe': timeframe,
        'asset': asset
    }
    while 1: 
        milker = QXMilkerAsync(ssid, network_timeout=6)
        # milker.set_msg_tracker(_msg_tracker)

        if x:= await milker.start() in [AUTH_STATE.AUTH_REJECTED, AUTH_STATE.AUTH_FAILURE]:
            print(f"task{prg_id}: {x}")

        print(f"task{prg_id}: Auth success")

        from rich.pretty import pprint
        try:
            async for _data, _nxt_end_tm in milker.extract_data_iter(asset, timeframe, start, next_end_time):
                data['data'] = merge_candlesticks(_data, data['data'])
                progress.update(prg_id, advance=len(_data))
                next_end_time = _nxt_end_tm
            
        except Exception as e:
            print(f"{e} raised in task{prg_id}")
            await asyncio.sleep(retry_delay)
            continue

        pprint(f"{prg_id} start: {dcd_tmp(data['data'][0]['time'])}, end: {dcd_tmp(data['data'][-1]['time'])}")
        return data

async def main():
    time_ranges = get_time_ranges(start.timestamp(), end.timestamp(), args.concurrency, args.timeframe)
    tasks = []
    task_ids = []
    with Progress(
        SpinnerColumn(), TextColumn("[bold cyan]{task.description}"), BarColumn(), CandleColumn(), TimeElapsedColumn(),
        TimeRemainingColumn(),
    ) as progress:
        for idx, interval in enumerate(time_ranges):
            id_ = progress.add_task(
                f"task{idx}", total=QXMilkerAsync.calc_n_candles(interval[0], interval[1], args.timeframe)
            )
            tasks.append(fetch_data(args.asset, args.timeframe, interval[0], interval[1], args.ssid, id_, progress))
            task_ids.append(id_)

        data = await asyncio.gather(*tasks)

    out = {k:v for k,v in data[0].items() if k != "data"}
    out["data"] = [item for dct in data for item in dct["data"]]
    with open(args.output, "w") as f:
        json.dump(out, f, indent=4)
    print(f"Data saved to {args.output}")

asyncio.run(main())
