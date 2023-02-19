from typing import Union, List
from binance.websocket.websocket_client import BinanceWebsocketClient


class UMFuturesWebsocketClient(BinanceWebsocketClient):
    def __init__(self, stream_url="wss://fstream.binance.com"):
        super().__init__(stream_url)

    def agg_trade(self, symbol: str, id: int, callback, **kwargs):
        """Aggregate Trade Streams

        The Aggregate Trade Streams push market trade information that is aggregated for a single taker order every 100 milliseconds.
        Only market trades will be aggregated, which means the insurance fund trades and ADL trades won't be aggregated.

        Stream Name: <symbol>@aggTrade

        https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams

        Update Speed: 100ms
        """
        self.live_subscribe(f"{symbol.lower()}@aggTrade", id, callback, **kwargs)

    def mark_price(self, symbol: str, id: int, speed: int, callback, **kwargs):
        """Mark Price Streams

        Mark price and funding rate for all symbols pushed every 3 seconds or every second.

        Stream Name: <symbol>@markPrice or <symbol>@markPrice@1s

        https://binance-docs.github.io/apidocs/futures/en/#mark-price-stream

        Update Speed: 3000ms or 1000ms
        """
        self.live_subscribe(
            f"{symbol.lower()}@markPrice@{speed}s", id, callback, **kwargs
        )

    def kline(self, symbol: str, id: int, interval: str, callback, **kwargs):
        """Kline/Candlestick Streams

        The Kline/Candlestick Stream push updates to the current klines/candlestick every 250 milliseconds (if existing)

        Stream Name: <symbol>@kline_<interval>

        https://binance-docs.github.io/apidocs/futures/en/#kline-candlestick-streams

        interval:
        m -> minutes; h -> hours; d -> days; w -> weeks; M -> months

        - 1m
        - 3m
        - 5m
        - 15m
        - 30m
        - 1h
        - 2h
        - 4h
        - 6h
        - 8h
        - 12h
        - 1d
        - 3d
        - 1w
        - 1M

        Update Speed: 250ms
        """

        self.live_subscribe(
            f"{symbol.lower()}@kline_{interval}", id, callback, **kwargs
        )

    def continuous_kline(
        self, pair: str, id: int, contractType: str, interval: str, callback, **kwargs
    ):
        """Continuous Kline/Candlestick Streams

        The Kline/Candlestick Stream push updates to Kline/candlestick bars for a specific contract type. every 250 milliseconds

        Stream Name: <pair>_<contractType>@continuousKline_<interval>

        https://binance-docs.github.io/apidocs/futures/en/#continuous-contract-kline-candlestick-streams

        interval:
        m -> minutes; h -> hours; d -> days; w -> weeks; M -> months

        - 1m
        - 3m
        - 5m
        - 15m
        - 30m
        - 1h
        - 2h
        - 4h
        - 6h
        - 8h
        - 12h
        - 1d
        - 3d
        - 1w
        - 1M

        Update Speed: 250ms
        """

        self.live_subscribe(
            f"{pair.lower()}_{contractType}@continuousKline_{interval}",
            id,
            callback,
            **kwargs,
        )

    def mini_ticker(self, id: int, callback, symbol=None, **kwargs):
        """Individual symbol or all symbols mini ticker

        24hr rolling window mini-ticker statistics.
        These are NOT the statistics of the UTC day, but a 24hr rolling window for the previous 24hrs

        Stream Name: <symbol>@miniTicker or
        Stream Name: !miniTicker@arr

        https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-mini-ticker-stream
        https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-ticker-streams

        Update Speed: 500ms for individual symbol, 1000ms for all market symbols
        """

        if symbol is None:
            self.live_subscribe("!miniTicker@arr", id, callback, **kwargs)
        else:
            self.live_subscribe(f"{symbol.lower()}@miniTicker", id, callback, **kwargs)

    def ticker(self, id: int, callback, symbol=None, **kwargs):
        """Individual symbol or all symbols ticker

        24hr rolling window ticker statistics for a single symbol.
        These are NOT the statistics of the UTC day, but a 24hr rolling window from requestTime to 24hrs before.

        Stream Name: <symbol>@ticker or
        Stream Name: !ticker@arr

        https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-ticker-streams
        https://binance-docs.github.io/apidocs/futures/en/#all-market-tickers-streams

        Update Speed: 500ms for individual symbol, 1000ms for all market symbols
        """

        if symbol is None:
            self.live_subscribe("!ticker@arr", id, callback, **kwargs)
        else:
            self.live_subscribe(f"{symbol.lower()}@ticker", id, callback, **kwargs)

    def book_ticker(self, id: int, callback, symbol=None, **kwargs):
        """Individual symbol or all book ticker

        Pushes any update to the best bid or ask's price or quantity in real-time for a specified symbol.

        Stream Name: <symbol>@bookTicker or
        Stream Name: !bookTicker

        https://binance-docs.github.io/apidocs/futures/en/#individual-symbol-book-ticker-streams
        https://binance-docs.github.io/apidocs/futures/en/#all-book-tickers-stream

        Update Speed: Real-time
        """

        if symbol is None:
            self.live_subscribe("!bookTicker", id, callback, **kwargs)
        else:
            self.live_subscribe(f"{symbol.lower()}@bookTicker", id, callback, **kwargs)

    def liquidation_order(self, id: int, callback, symbol=None, **kwargs):
        """The Liquidation Order Snapshot Streams push force liquidation order information for specific symbol.
        The All Liquidation Order Snapshot Streams push force liquidation order information for all symbols in the market.

        For each symbol, only the latest one liquidation order within 1000ms will be pushed as the snapshot. If no liquidation happens in the interval of 1000ms, no stream will be pushed.

        Stream Name: <symbol>@forceOrder or
        Stream Name: !forceOrder@arr

        https://binance-docs.github.io/apidocs/futures/en/#liquidation-order-streams
        https://binance-docs.github.io/apidocs/futures/en/#all-market-liquidation-order-streams

        Update Speed: 1000ms
        """
        if symbol is None:
            self.live_subscribe("!forceOrder@arr", id, callback, **kwargs)
        else:
            self.live_subscribe(f"{symbol.lower()}@forceOrder", id, callback, **kwargs)

    def partial_book_depth(self, symbols: Union[str, List[str]], id: int, level: int, speed: int, callback, **kwargs):
        """Subscribe to partial book depth streams for a symbol.

        Valid levels are 5, 10, or 20. The update speed can be 250ms, 500ms, or 100ms.

        Stream names: <symbol>@depth<levels> OR <symbol>@depth<levels>@500ms OR <symbol>@depth<levels>@100ms
        See https://binance-docs.github.io/apidocs/futures/en/#partial-book-depth-streams for more information.

        Args:
            symbol (Union[str, List[str]]): A single symbol or a list of symbols to subscribe to.
            id (int): The subscription ID.
            level (int): The depth level to subscribe to.
            speed (int): The update speed in milliseconds.
            callback: The callback function to call when an update is received.
            **kwargs: Additional arguments to pass to the callback function.

        """
        if isinstance(symbols, list):
            streams = [f"{s.lower()}@depth{level}@{speed}ms" for s in symbols]
        else:
            streams = f"{symbols.lower()}@depth{level}@{speed}ms"

        self.live_subscribe(streams, id, callback, **kwargs)


    def diff_book_depth(self, symbol: str, id: int, speed, callback, **kwargs):
        """Diff. Depth Stream
        Order book price and quantity depth updates used to locally manage an order book.

        Stream Name: <symbol>@depth OR <symbol>@depth@500ms OR<symbol>@depth@100ms

        https://binance-docs.github.io/apidocs/futures/en/#diff-book-depth-streams

        Update Speed: 250ms, 500ms or 100ms
        """

        self.live_subscribe(
            f"{symbol.lower()}@depth@{speed}ms", id, callback, **kwargs
        )

    def composite_index(self, symbol: str, id: int, callback, **kwargs):
        """Composite Index Info Stream
        Composite index information for index symbols pushed every second.

        Stream Name: <symbol>@compositeIndex

        https://binance-docs.github.io/apidocs/futures/en/#composite-index-symbol-information-streams

        Update Speed: 1000ms
        """

        self.live_subscribe(f"{symbol.lower()}@compositeIndex", id, callback, **kwargs)

    def user_data(self, listen_key: str, id: int, callback, **kwargs):
        """listen to user data by provided listenkey"""
        self.live_subscribe(listen_key, id, callback, **kwargs)
