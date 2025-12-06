# src/DataSupply/simulator_manager.py
"""The time drift problem is not solved."""
import asyncio
import logging
import time as time_module
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional
from zoneinfo import ZoneInfo

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ..utils.config import lake_data_dir
from ..utils.logger import setup_logger

logger = setup_logger(__name__, log_to_file=True)


class LocalReplayWebSocketManager:
    """
    Streamed parquet + metronome (tick scheduler) replay engine.
    - Stream parquet with pyarrow (row-batches) to avoid loading whole file.
    - Use a metronome loop: compute target_data_ts based on wall clock,
      and release all rows with timestamp <= target_data_ts in batches.
    """

    def __init__(
        self,
        replay_date: str,
        start_time: Optional[str] = None,
        batch_size: int = 64_000,
        metronome_sleep: float = 0.001,
    ):
        """
        Args:
            replay_date: YYYYMMDD
            start_time: HH:MM or HH:MM:SS (America/New_York)
            batch_size: pyarrow batch size for streaming read
            metronome_sleep: sleep time for metronome loop (seconds)
        """
        self.replay_date = replay_date
        self.start_time = start_time
        self.batch_size = batch_size
        self.metronome_sleep = metronome_sleep

        self.start_timestamp_ns = (
            self._parse_start_time(replay_date, start_time) if start_time else None
        )

        # Connection management (unified with Polygon/ThetaData interface)
        self.connections: Dict[object, set] = {}  # {websocket_client: set(stream_keys)}
        self.queues: Dict[str, asyncio.Queue] = {}  # {stream_key: Queue}
        self.tasks: Dict[str, asyncio.Task] = {}  # {symbol: replay_task}
        self.subscribed_streams: set = set()  # set of stream_keys
        self.subscribed_symbols: set = set()  # set of symbols with active replay tasks
        self.connected = True  # For compatibility

        # Data source
        self.lake_data_dir = Path(lake_data_dir)

        # Stats
        self.stats = {
            "total_messages": 0,
            "messages_per_symbol": {},
            "queue_full_count": {},
            "start_time": None,
            "end_time": None,
            "skipped_messages": {},
            "drift_stats": {},
        }

        logger.info(f"Initialized LocalReplayWebSocketManager for {replay_date}")
        if self.start_timestamp_ns:
            start_dt = datetime.fromtimestamp(
                self.start_timestamp_ns / 1e9, tz=ZoneInfo("America/New_York")
            )
            logger.info(f"  Start time: {start_time} -> {start_dt.isoformat()}")

    def _parse_start_time(self, replay_date: str, start_time_str: str) -> int:
        year = int(replay_date[:4])
        month = int(replay_date[4:6])
        day = int(replay_date[6:8])

        parts = start_time_str.split(":")
        hour = int(parts[0])
        minute = int(parts[1])
        second = int(parts[2]) if len(parts) > 2 else 0

        dt = datetime(
            year=year,
            month=month,
            day=day,
            hour=hour,
            minute=minute,
            second=second,
            tzinfo=ZoneInfo("America/New_York"),
        )
        timestamp_ns = int(dt.timestamp() * 1e9)
        logger.info(
            f"Parsed start time: {start_time_str} -> {dt.isoformat()} ({timestamp_ns} ns)"
        )
        return timestamp_ns

    def _parquet_path_for_date(self) -> Path:
        year = self.replay_date[:4]
        month = self.replay_date[4:6]
        date_iso = f"{year}-{month}-{self.replay_date[6:8]}"
        file_path = (
            self.lake_data_dir
            / "us_stocks_sip"
            / "quotes_v1"
            / year
            / month
            / f"{date_iso}.parquet"
        )
        return file_path

    async def _replay_symbol_task(self, symbol: str):
        """
        New metronome-driven replay for one symbol.
        Streaming-read parquet with pyarrow, filter symbol in each batch,
        and use metronome to release rows according to participant_timestamp.
        """
        try:
            file_path = self._parquet_path_for_date()
            if not file_path.exists():
                logger.error(f"Data file not found: {file_path}")
                q = self.queues.get(symbol)
                if q:
                    await q.put(None)
                return

            logger.info(f"Streaming parquet: {file_path} for {symbol}")
            pf = pq.ParquetFile(str(file_path))

            # Stats init
            self.stats["messages_per_symbol"].setdefault(symbol, 0)
            self.stats["skipped_messages"].setdefault(symbol, 0)

            # In absolute mode we set the baseline when we see the first eligible row
            first_data_ts_ns: Optional[int] = None
            wall_clock_start: Optional[float] = None

            # Buffer for rows waiting to be released (list of dicts)
            buffer = []

            # Define helper to process buffer up to target_ts (inclusive)
            async def flush_until(target_ts_ns: int):
                nonlocal buffer, first_data_ts_ns, wall_clock_start
                # release all buffered rows with ts <= target_ts_ns
                # buffer is kept sorted by participant_timestamp
                idx = 0
                n = len(buffer)
                while idx < n and buffer[idx]["participant_timestamp"] <= target_ts_ns:
                    row = buffer[idx]
                    ts = row["participant_timestamp"]

                    # skip rows earlier than provided start_time (if provided)
                    if self.start_timestamp_ns and ts < self.start_timestamp_ns:
                        self.stats["skipped_messages"][symbol] += 1
                        idx += 1
                        continue

                    # build payload
                    payload = {
                        "ev": "Q",
                        "sym": symbol,
                        "bx": row.get("bid_exchange") or "",
                        "ax": row.get("ask_exchange") or "",
                        "bp": float(row.get("bid_price") or 0.0),
                        "ap": float(row.get("ask_price") or 0.0),
                        "bs": int(row.get("bid_size") or 0),
                        "as": int(row.get("ask_size") or 0),
                        "t": int(ts),
                        "c": row.get("conditions") or [],
                        "z": int(row.get("tape") or 0),
                    }

                    # Send to all stream_keys for this symbol (e.g., Q.AAPL, T.AAPL)
                    # For now, simulator only supports Quote data, so we use "Q" event
                    stream_key = f"Q.{symbol}"
                    q = self.queues.get(stream_key)
                    if q:
                        await q.put(payload)
                        self.stats["total_messages"] += 1
                        self.stats["messages_per_symbol"][symbol] += 1

                    idx += 1

                # drop released rows
                if idx > 0:
                    buffer = buffer[idx:]

            # Iterate row batches from parquet (streaming)
            for batch in pf.iter_batches(batch_size=self.batch_size):
                # convert to polars or pyarrow table
                table = pa.Table.from_batches([batch])
                # convert to polars df for easy column access and filtering
                try:
                    pdf = pl.from_arrow(table)
                except Exception:
                    # fallback: build DataFrame from columns manually
                    pdf = pl.DataFrame(table.to_pydict())

                # Filter symbol early
                if "ticker" not in pdf.columns:
                    logger.error(
                        "No 'ticker' column in parquet; aborting symbol replay."
                    )
                    break

                pdf = pdf.filter(pl.col("ticker") == symbol)

                # Cast required columns to expected types and drop rows without participant_timestamp
                if "participant_timestamp" not in pdf.columns:
                    logger.error("No 'participant_timestamp' column; aborting.")
                    break

                # keep only required columns to reduce memory
                cols = [
                    "participant_timestamp",
                    "bid_price",
                    "ask_price",
                    "bid_size",
                    "ask_size",
                    "bid_exchange",
                    "ask_exchange",
                    "conditions",
                    "tape",
                ]
                # Some may be missing 鈥?add defaults
                for c in cols:
                    if c not in pdf.columns:
                        pdf = pdf.with_columns(pl.lit(None).alias(c))

                # Convert to python list of dicts sorted by timestamp within batch
                # cast participant_timestamp to int
                pdf = pdf.with_columns(pl.col("participant_timestamp").cast(pl.Int64))
                rows = [r for r in pdf.to_dicts()]

                # sort rows by timestamp ascending
                rows.sort(key=lambda r: int(r.get("participant_timestamp") or 0))

                # append to buffer (buffer remains sorted if incoming batches are near sorted)
                # but to be safe we merge-sort: simple extend + resort if buffer not empty and last ts > new first ts
                if buffer and rows:
                    if (
                        buffer[-1]["participant_timestamp"]
                        <= rows[0]["participant_timestamp"]
                    ):
                        buffer.extend(rows)
                    else:
                        # merge-sort quickly
                        merged = []
                        i = j = 0
                        A = buffer
                        B = rows
                        while i < len(A) and j < len(B):
                            if (
                                A[i]["participant_timestamp"]
                                <= B[j]["participant_timestamp"]
                            ):
                                merged.append(A[i])
                                i += 1
                            else:
                                merged.append(B[j])
                                j += 1
                        if i < len(A):
                            merged.extend(A[i:])
                        if j < len(B):
                            merged.extend(B[j:])
                        buffer = merged
                else:
                    buffer.extend(rows)

                # initialize baseline if needed
                if first_data_ts_ns is None:
                    # find first row that is >= start_timestamp_ns (if provided)
                    for r in buffer:
                        ts = r.get("participant_timestamp")
                        if ts is None:
                            continue
                        if self.start_timestamp_ns and ts < self.start_timestamp_ns:
                            # skip for now, but do not set baseline
                            continue
                        first_data_ts_ns = ts
                        wall_clock_start = time_module.perf_counter()
                        logger.info(
                            f"馃晲 Absolute sync baseline for {symbol}: first_data_ts_ns={first_data_ts_ns}, wall_start={wall_clock_start:.6f}"
                        )
                        break

                # Metronome loop: release whatever should be released now
                # compute current target timestamp
                if first_data_ts_ns is not None:
                    elapsed_wall = time_module.perf_counter() - wall_clock_start
                    target_ts = int(first_data_ts_ns + elapsed_wall * 1e9)
                    # flush
                    if buffer:
                        await flush_until(target_ts)
                    # If buffer is empty, still sleep short
                    await asyncio.sleep(self.metronome_sleep)

            # after all batches consumed, we may still have buffer rows to emit
            if buffer:
                if first_data_ts_ns is not None:
                    # keep flushing until buffer empty; target_ts moves forward with wall clock
                    while buffer:
                        elapsed_wall = time_module.perf_counter() - wall_clock_start
                        target_ts = int(first_data_ts_ns + elapsed_wall * 1e9)
                        await flush_until(target_ts)
                        await asyncio.sleep(self.metronome_sleep)

            # Done for symbol - no completion signal needed for unified interface
            # Queues remain open for potential reconnection

            # Optionally compute drift stats (we can track samples during run; omitted here for brevity)

            logger.info(f"鉁?Completed replay for {symbol}")

        except asyncio.CancelledError:
            logger.info(f"馃洃 Replay cancelled for {symbol}")
            q = self.queues.get(symbol)
            if q:
                await q.put(None)
            raise
        except Exception as e:
            logger.exception(f"鉂?Error replaying {symbol}: {e}")
            q = self.queues.get(symbol)
            if q:
                await q.put(None)
            raise

    async def subscribe(self, websocket_client, symbols: list[str], events=["Q"]):
        """Subscribe to symbols (unified interface like Polygon/ThetaData)."""
        if websocket_client not in self.connections:
            self.connections[websocket_client] = set()

        for symbol in symbols:
            for ev in events:
                stream_key = f"{ev}.{symbol}"

                # Create queue for this stream_key
                if stream_key not in self.queues:
                    self.queues[stream_key] = asyncio.Queue()
                    self.stats["queue_full_count"][stream_key] = 0

                # Add to client subscriptions
                self.connections[websocket_client].add(stream_key)
                self.subscribed_streams.add(stream_key)

                # Start replay task (once per symbol)
                if symbol not in self.subscribed_symbols:
                    task = asyncio.create_task(self._replay_symbol_task(symbol))
                    self.tasks[symbol] = task
                    self.subscribed_symbols.add(symbol)
                    logger.info(f"Started replay for {symbol}")

                logger.info(f"Subscribed to {stream_key}")

    async def unsubscribe(self, websocket_client, symbol: str, events=["Q"]):
        """Unsubscribe from symbol (unified interface)."""
        if websocket_client not in self.connections:
            return

        for ev in events:
            stream_key = f"{ev}.{symbol}"
            self.connections[websocket_client].discard(stream_key)

            # Check if still needed by other clients
            still_needed = any(
                stream_key in streams for streams in self.connections.values()
            )

            if not still_needed:
                # Cancel replay task
                if symbol in self.tasks:
                    self.tasks[symbol].cancel()
                    del self.tasks[symbol]

                if symbol in self.subscribed_symbols:
                    self.subscribed_symbols.remove(symbol)

                self.subscribed_streams.discard(stream_key)
                self.queues.pop(stream_key, None)
                logger.info(f"Unsubscribed from {stream_key}")

    async def disconnect(self, websocket_client):
        """Clean up when client disconnects (unified interface)."""
        client_streams = self.connections.pop(websocket_client, set())

        # Extract unique symbols
        symbols = set(sk.split(".")[1] for sk in client_streams if "." in sk)

        for symbol in symbols:
            # Check if any stream for this symbol is still needed
            still_needed = any(
                any(sk.endswith(f".{symbol}") for sk in streams)
                for streams in self.connections.values()
            )

            if not still_needed:
                if symbol in self.tasks:
                    self.tasks[symbol].cancel()
                    del self.tasks[symbol]

                if symbol in self.subscribed_symbols:
                    self.subscribed_symbols.remove(symbol)

                # Remove all stream_keys for this symbol
                for sk in list(self.subscribed_streams):
                    if sk.endswith(f".{symbol}"):
                        self.subscribed_streams.discard(sk)
                        self.queues.pop(sk, None)

                logger.info(f"Auto-unsubscribed: {symbol}")

    async def stream_forever(self):
        """No-op for simulator (replay tasks started per symbol in subscribe)."""
        pass

    async def wait_for_completion(self):
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        logger.info("All replay tasks completed")

    def get_stats(self) -> Dict:
        return {
            **self.stats,
            "active_symbols": len(self.subscribed_symbols),
            "active_connections": len(self.connections),
            "active_streams": len(self.subscribed_streams),
            "queue_sizes": {sk: self.queues[sk].qsize() for sk in self.queues},
        }


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Ticker Simulator and Test Suite")
    parser.add_argument(
        "--ticker", default="AAPL", help="Ticker symbol to replay (manual mode)"
    )
    parser.add_argument(
        "--replay-date", default="20251113", help="Replay date in YYYYMMDD format"
    )
    parser.add_argument(
        "--start-time",
        default=None,
        help="Start time in HH:MM or HH:MM:SS format (America/New_York timezone)",
    )
    args = parser.parse_args()

    async def manual_test():
        logging.basicConfig(level=logging.INFO)
        messages = []

        async def on_message(symbol: str, payload: dict):
            messages.append(payload)
            dt = datetime.fromtimestamp(
                payload["t"] / 1e9, tz=ZoneInfo("America/New_York")
            )
            print(
                f"馃摠 [{len(messages)}] {symbol} @ {dt.strftime('%H:%M:%S.%f')[:-3]}: "
                f"bid={payload['bp']:.2f}@{payload['bs']} ask={payload['ap']:.2f}@{payload['as']}"
            )

        manager = LocalReplayWebSocketManager(
            replay_date=args.replay_date, start_time=args.start_time
        )

        print("\n馃幀 Starting manual replay:")
        print(f"   Ticker: {args.ticker}")
        print(f"   Date: {args.replay_date}")
        if args.start_time:
            print(f"   Start Time: {args.start_time}")
        print()

        await manager.subscribe(
            client_id="manual_client", symbols=[args.ticker], callback=on_message
        )
        await manager.wait_for_completion()

        stats = manager.get_stats()
        print("\n馃搳 Replay completed:")
        print(f"   Total messages: {len(messages)}")
        print(stats)

    asyncio.run(manual_test())
