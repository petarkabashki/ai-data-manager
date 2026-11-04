"""Exit 0 if the Gateway serves historical data, 1 if it does not.

A bare API handshake succeeds even while the historical-data farm is down, so
download monitors that only handshake keep restarting workers that cannot work.
This asks for five days of SPY daily bars with a spare client id.
"""

import sys
from datetime import datetime, timedelta, timezone

from utils.config import IBKR_HOST, IBKR_PORT
from utils.ibkr import HistoricalRequest, IbkrHistoricalClient, ibkr_bar_size

CLIENT_ID = 198
TIMEOUT = 25.0


def main() -> int:
    now = datetime.now(timezone.utc)
    try:
        with IbkrHistoricalClient(IBKR_HOST, IBKR_PORT, CLIENT_ID, timeout=TIMEOUT, max_consecutive_timeouts=99) as c:
            contract = c.resolve_us_stock_contract("SPY")
            bars = c.fetch_historical_bars(
                HistoricalRequest(contract, now, "5 D", ibkr_bar_size("1d"), "TRADES", False)
            )
    except Exception as exc:
        print(f"historical data unavailable: {exc}")
        return 1
    print(f"ok: {len(bars)} bars")
    return 0 if bars else 1


if __name__ == "__main__":
    sys.exit(main())
