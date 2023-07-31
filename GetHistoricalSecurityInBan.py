# -*- coding: utf-8 -*-
"""
    :description: A Python Script To Fetch Securities In Ban period Files From NSE Website. # noqa: E501
    :license: MIT.
    :author: Shabbir Hasan
    :created: On Saturday July 29, 2023 19:56:53 GMT+05:30
"""
__author__ = "Shabbir Hasan"
__webpage__ = "https://github.com/TechfaneTechnologies"
__license__ = "MIT"

import io
import os
import sys
import ssl
import math
import json
import time
import asyncio
import platform
import datetime
from glob import glob
from time import sleep
from functools import partial
from json import JSONDecodeError
from typing import List, Dict, Tuple, Union, Optional
from datetime import date, datetime as dt, timedelta as td
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    from typing import Any
except ImportError:
    from typing_extensions import Any

try:
    import httpx
    import numpy as np
    import pandas as pd
except (ImportError, ModuleNotFoundError):
    os.system(
        f"{sys.executable}"
        + " -m pip install -U"
        + " numpy pandas httpx"
        + " httpx[http2] httpx[brotli] httpx[socks]"  # noqa: E501
    )
finally:
    import httpx
    import numpy as np
    import pandas as pd

if platform.system() != "Windows":
    try:
        import uvloop
    except (ImportError, ModuleNotFoundError):
        os.system(f"{sys.executable} -m pip install uvloop")
        import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

user_agent = httpx.get(
    "https://techfanetechnologies.github.io/"
    + "latest-user-agent/user_agents.json"  # noqa: E501
).json()[-2]

NSE = "www.nseindia.com"
NSEURL = "https://www.nseindia.com"
ARCHIVESNSE = "archives.nseindia.com"
ARCHIVESNSEURL = "https://archives.nseindia.com"

HEADERS = {
    "user-agent": user_agent,
    "authority": NSE,
    "referer": f"{NSEURL}/",
    "accept-encoding": "gzip, deflate, br",
}

ROUTES = {
    "reports": "/api/reports",
    "sec_ban": "/archives/fo/sec_ban/{archive_name}",
}
os.makedirs("DayWiseSecurityInBanCSVFiles", exist_ok=True)


def getListChunks(
    inputList: List[Any], chunkSize: int
) -> Tuple[Tuple[Any]]:  # noqa: E501
    return tuple(
        tuple(subArray.tolist())
        for subArray in np.array_split(
            inputList, math.ceil(len(inputList) / chunkSize)
        )  # noqa: E501
    )


def getNextBusinessDate() -> date:
    if dt.today().weekday() == 4:
        return (dt.today() + td(days=3)).date()
    elif dt.today().weekday() == 5:
        return (dt.today() + td(days=2)).date()
    else:
        return (dt.today() + td(days=1)).date()
        

async def log_request(request: httpx.Request) -> httpx.Request:
    global logger
    print(
        "\nRequest Event Hook: "
        + f"Method: {request.method}, URL: {request.url}, "
        + f"Headers: {request.headers}, Time: {time.asctime()}, "
        + f"|, {time.time()} <<||>> Waiting for response\n"
    )
    return request


async def log_response(response: httpx.Response) -> httpx.Response:
    await response.aread()
    print(
        "\nResponse Event Hook: "
        + f"Method: {response.request.method}, "
        + f"URL: {response.request.url}, "
        + f"Status: {response.status_code}, "
        + f"HttpVersion: {response.http_version}, "
        + f"Headers: {response.headers}, "
        + f"Time: {time.asctime()}, '|', {time.time()} <<||>> "
        + (
            f"ResponseText: {response.content.decode('utf-8')[:1000]}...Truncated "  # noqa E501
            + "Due To Very Large ResponseText Content.\n"  # noqa E501
        )
    )
    return response


async def raise_for_status(response: httpx.Response) -> None:
    await response.aread()
    if response.status_code not in (200, 404, 302):
        response.raise_for_status()


def generate_client(
    base_url: str = NSEURL,
) -> httpx.AsyncClient:
    ssl_context = ssl.create_default_context()
    return httpx.AsyncClient(
        verify=ssl_context,
        http2=True,
        base_url=base_url,
        transport=httpx.AsyncHTTPTransport(
            verify=ssl_context,
            http2=True,
            limits=httpx.Limits(
                max_keepalive_connections=30,
                max_connections=100,
                keepalive_expiry=300,
            ),
            retries=20,
        ),
        headers=HEADERS,
        timeout=httpx.Timeout(10.0, connect=60.0),
        limits=httpx.Limits(
            max_keepalive_connections=30,
            max_connections=100,
            keepalive_expiry=300,
        ),
        event_hooks={
            "request": [log_request],
            "response": [
                log_response,
                raise_for_status,
            ],
        },
        trust_env=True,
        default_encoding="utf-8",
    )


async def get(
    route: str,
    params: Dict[str, Any] = dict(),
    limit: asyncio.Semaphore = asyncio.Semaphore(2),
) -> Optional[httpx.Response]:
    global client
    async with limit:
        try:
            response = await client.get(
                url=(ROUTES[route] if route in ROUTES.keys() else route),
                params=params,
            )
        except (httpx.RequestError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
            print(f"HTTP Request Has Met With An Exception: {exc}, Retrying Agian...")
            client.cookies.clear()
            if client.base_url == NSEURL:
                client.headers.update({"referer": f"{NSEURL}/"})
                await get("/", {})
                client.headers.update({"referer": f"{NSEURL}/all-reports-derivatives"})
            await asyncio.sleep(30.0)
            return await get(route, params, limit)
        else:
            if response.status_code == 302:
                print(f"File is unavailbale for the selected date: {route[-12:][:-3]}")
            if response.status_code == 404:
                # {"error":"Not Found, file is unavailable","show":true}
                error_response = await response.json()
                assert error_response["show"] == True
                assert error_response["error"] == "Not Found, file is unavailable"
                print(f"File is unavailbale for the selected date: {route[-12:][:-3]}")
            else:
                return response


async def fetch(
    route_params: List[Union[Tuple[str, Dict[str, Any]], Tuple[str]]] = [
        ("/", {})
    ],  # noqa: E501
) -> List[Optional[httpx.Response]]:
    global client
    limit = asyncio.Semaphore(len(route_params))
    return await asyncio.gather(
        *[
            asyncio.ensure_future(
                get(
                    route=(ROUTES[route] if route in ROUTES.keys() else route),
                    params=params,
                    limit=limit,
                )
            )
            for route, params in [
                route_param + ({},)
                if (len(route_param) == 1 and isinstance(route_param, tuple))
                else route_param
                for route_param in route_params
            ]
        ]
    )

        
async def getHistoricalSecurityInBanCSV(
    date: Union[date, str] = getNextBusinessDate(),
    limit: asyncio.Semaphore = asyncio.Semaphore(2),
) -> Tuple[str, Optional[httpx.Response]]:
    global client
    await asyncio.sleep(3)
    if isinstance(date, str) and date.find("-") == -1 and len(date) == 10:
        date = dt.strptime(date, "%d%m%Y").date().strftime("%d-%b-%Y")
    query_params = {
        "archives": json.dumps(
            [
                {
                    "name": "F&O - Security in ban period",
                    "type": "archives",
                    "category": "derivatives",
                    "section": "equity",
                }
            ]
        ),
        "date": date.strftime("%d-%b-%Y") if isinstance(date, datetime.date) else date,
        "type": "equity",
        "mode": "single",
    }
    if isinstance(date, str) and date.find("-") != -1:
        date = dt.strptime(date, "%d-%b-%Y").date().strftime("%d%m%Y")
    archive_name = f'fo_secban_{date.strftime("%d%m%Y") if isinstance(date, datetime.date) else date}.csv'
    archive_csv = None
    if client.base_url == NSEURL:
        archiveRoute = "reports"
        archive_csv = await get(archiveRoute, query_params, limit)
    if client.base_url == ARCHIVESNSEURL:
        archiveRoute = ROUTES["sec_ban"].format(archive_name=archive_name)
        archive_csv = await get(archiveRoute, {}, limit)
    return archive_name, archive_csv


async def getMultipleHistoricalSecurityInBanCSV(
    dates: Tuple[Union[date, str]],
) -> List[Tuple[str, Optional[httpx.Response]]]:
    global client
    limit = asyncio.Semaphore(len(dates))
    return await asyncio.gather(
        *[
            asyncio.ensure_future(
                getHistoricalSecurityInBanCSV(
                    date=date,
                    limit=limit,
                )
            )
            for date in dates
        ]
    )


def process_csv(archive_name: str, archive_csv: Optional[httpx.Response]):
    date = archive_name[10:18]
    date = f"{date[:2]}-{date[2:4]}-{date[4:8]}"
    if archive_csv is not None and isinstance(archive_csv, httpx.Response):
        df = pd.read_csv(
            io.StringIO(archive_csv.content.decode("utf-8")),
            usecols=[0, 1],
            index_col=0,
            names=["Index", "SecurityInBan"],
            skiprows=1,
        )
        df.insert(0, "Date", date)
        df[["Date", "SecurityInBan"]].to_csv(
            f"DayWiseSecurityInBanCSVFiles/{archive_name}", index=False
        )


async def process_responses(
    responses: List[Tuple[str, Optional[httpx.Response]]],
) -> None:
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        result = [
            await loop.run_in_executor(
                pool, partial(process_csv, archive_name, archive_csv)
            )
            for archive_name, archive_csv in responses
        ]


async def getSecuritiesInBanPeriodFiles(
    fetch_at_once: int = 21,
) -> None:
    df = pd.bdate_range(
        start=date(2008, 1, 1),
        end=getNextBusinessDate(),
    ).to_frame(  # noqa: E501
        index=False, name="Date"
    )
    df["DateStr"] = df["Date"].dt.strftime("%d-%b-%Y")
    for dates in getListChunks(df.DateStr.to_list(), fetch_at_once):
        responses = await getMultipleHistoricalSecurityInBanCSV(dates)
        await process_responses(responses)
        await asyncio.sleep(np.random.uniform(1.317, 5.997))


async def getTodaysSecuritiesInBanPeriodFile() -> None:
    loop = asyncio.get_running_loop()
    responses = await getHistoricalSecurityInBanCSV()
    archive_name, archive_csv = responses
    await loop.run_in_executor(
        None,
        partial(process_csv, archive_name, archive_csv),
    )


def mergeAllSecuritiesInBanPeriodFiles(all_historical: bool = False) -> None:
    def readCsv(*args, **kwargs):
        kwargs.update(
            {
                "names": ["Date", "SecurityInBan"],
                "skiprows": 1,
                "index_col": 0,
                # "parse_dates": True,
                # "keep_date_col": True,
            }
        )
        return pd.read_csv(*args, **kwargs)

    if all_historical:
        pd.concat(map(readCsv, glob("DayWiseSecurityInBanCSVFiles/*.csv"))).sort_values(
            by=["Date"]
        ).to_csv(
            "HistoricalSecurityInBan.csv", index=True
        )  # noqa E501
    else:
        archive_file = f'DayWiseSecurityInBanCSVFiles/fo_secban_{getNextBusinessDate().strftime("%d%m%Y")}.csv'  # noqa E501
        print(readCsv(archive_file))
        main_file = "HistoricalSecurityInBan.csv"
        df = (
            pd.concat(
                map(
                    readCsv,
                    [
                        main_file,
                        archive_file,
                    ],
                )
            )
            # .sort_values(by=["Date"])
        )  # noqa E501
        print(df)
        df.to_csv(main_file, index=True)
        

async def fetchCsvFiles(all_historical: bool = False) -> None:
    global client
    if client.base_url == NSEURL:
        await get("/", {})
        client.headers.update(
            {"referer": f"{NSEURL}/all-reports-derivatives"}
        )  # noqa E501
    if client.base_url == ARCHIVESNSEURL:
        pass
    if all_historical:
        await getSecuritiesInBanPeriodFiles()
    else:
        await getTodaysSecuritiesInBanPeriodFile()


if __name__ == "__main__":
    # Prepare HTTPX Async Client with HTTP2 Protocol.
    client = generate_client(ARCHIVESNSEURL)

    # To Download All Historical Securities in Ban File,
    # Keep `all_historical = True` else to download latest
    # Updated Daily Data Keep it False
    all_historical = False

    if platform.system() != "Windows":
        # Fetch all of the Day Wise Security In Ban CSV Files
        with asyncio.Runner(loop_factory=uvloop.new_event_loop) as runner:
            csv_file = runner.run(fetchCsvFiles(all_historical))
    else:
        # Fetch all of the Day Wise Security In Ban CSV Files
        csv_file = asyncio.run(fetchCsvFiles(all_historical))

    # Now Pack Everything into One Comprehensive Security In Ban CSV File
    mergeAllSecuritiesInBanPeriodFiles(all_historical)
