//  ---------------------------------------------------------------------------

import hyperliquidRest from '../hyperliquid.js';
import { NotSupported, ExchangeError, UnsubscribeError } from '../base/errors.js';
import Client from '../base/ws/Client.js';
import { Int, Str, Market, OrderBook, Trade, OHLCV, Order, Dict, Strings, Ticker, Tickers, type Num, OrderType, OrderSide, type OrderRequest, Bool, Balances, Position } from '../base/types.js';
import { ArrayCache, ArrayCacheByTimestamp, ArrayCacheBySymbolById, ArrayCacheBySymbolBySide } from '../base/ws/Cache.js';

//  ---------------------------------------------------------------------------

export default class hyperliquid extends hyperliquidRest {
    describe (): any {
        return this.deepExtend (super.describe (), {
            'has': {
                'ws': true,
                'cancelOrderWs': true,
                'cancelOrdersWs': true,
                'createOrderWs': true,
                'createOrdersWs': true,
                'editOrderWs': true,
                'watchBalance': true,
                'watchBidsAsks': true,
                'watchMyTrades': true,
                'watchOHLCV': true,
                'watchOrderBook': true,
                'watchOrders': true,
                'watchTicker': true,
                'watchTickers': true,
                'watchTrades': true,
                'watchTradesForSymbols': false,
                'watchPosition': false,
                'unWatchBalance': true,
                'unWatchBidsAsks': true,
                'watchPositions': true,
                'unWatchPositions': true,
                'unWatchOrderBook': true,
                'unWatchTickers': true,
                'unWatchTrades': true,
                'unWatchOHLCV': true,
                'unWatchMyTrades': true,
                'unWatchOrders': true,
            },
            'urls': {
                'api': {
                    'ws': {
                        'public': 'wss://api.hyperliquid.xyz/ws',
                    },
                },
                'test': {
                    'ws': {
                        'public': 'wss://api.hyperliquid-testnet.xyz/ws',
                    },
                },
            },
            'options': {
            },
            'streaming': {
                'ping': this.ping,
                'keepAlive': 20000,
            },
            'exceptions': {
                'ws': {
                    'exact': {
                    },
                },
            },
        });
    }

    /**
     * @method
     * @name hyperliquid#createOrdersWs
     * @description create a list of trade orders using WebSocket post request
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#place-an-order
     * @param {Array} orders list of orders to create, each object should contain the parameters required by createOrder, namely symbol, type, side, amount, price and params
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} an [order structure]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async createOrdersWs (orders: OrderRequest[], params = {}) {
        await this.loadMarkets ();
        const url = this.urls['api']['ws']['public'];
        const ordersRequest = this.createOrdersRequest (orders, params);
        const wrapped = this.wrapAsPostAction (ordersRequest);
        const request = this.safeDict (wrapped, 'request', {});
        const requestId = this.safeString (wrapped, 'requestId');
        const response = await this.watch (url, requestId, request, requestId);
        const responseOjb = this.safeDict (response, 'response', {});
        const data = this.safeDict (responseOjb, 'data', {});
        const statuses = this.safeList (data, 'statuses', []);
        return this.parseOrders (statuses, undefined);
    }

    /**
     * @method
     * @name hyperliquid#createOrderWs
     * @description create a trade order using WebSocket post request
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#place-an-order
     * @param {string} symbol unified symbol of the market to create an order in
     * @param {string} type 'market' or 'limit'
     * @param {string} side 'buy' or 'sell'
     * @param {float} amount how much of currency you want to trade in units of base currency
     * @param {float} [price] the price at which the order is to be fulfilled, in units of the quote currency, ignored in market orders
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.timeInForce] 'Gtc', 'Ioc', 'Alo'
     * @param {bool} [params.postOnly] true or false whether the order is post-only
     * @param {bool} [params.reduceOnly] true or false whether the order is reduce-only
     * @param {float} [params.triggerPrice] The price at which a trigger order is triggered at
     * @param {string} [params.clientOrderId] client order id, (optional 128 bit hex string e.g. 0x1234567890abcdef1234567890abcdef)
     * @param {string} [params.slippage] the slippage for market order
     * @param {string} [params.vaultAddress] the vault address for order
     * @returns {object} an [order structure]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async createOrderWs (symbol: string, type: OrderType, side: OrderSide, amount: number, price: Num = undefined, params = {}) {
        await this.loadMarkets ();
        const [ order, globalParams ] = this.parseCreateEditOrderArgs (undefined, symbol, type, side, amount, price, params);
        const orders = await this.createOrdersWs ([ order as any ], globalParams);
        const ordersLength = orders.length;
        if (ordersLength === 0) {
            // not sure why but it is happening sometimes
            return this.safeOrder ({});
        }
        const parsedOrder = orders[0];
        return parsedOrder;
    }

    /**
     * @method
     * @name hyperliquid#editOrderWs
     * @description edit a trade order
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/exchange-endpoint#modify-multiple-orders
     * @param {string} id cancel order id
     * @param {string} symbol unified symbol of the market to create an order in
     * @param {string} type 'market' or 'limit'
     * @param {string} side 'buy' or 'sell'
     * @param {float} amount how much of currency you want to trade in units of base currency
     * @param {float} [price] the price at which the order is to be fulfilled, in units of the quote currency, ignored in market orders
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.timeInForce] 'Gtc', 'Ioc', 'Alo'
     * @param {bool} [params.postOnly] true or false whether the order is post-only
     * @param {bool} [params.reduceOnly] true or false whether the order is reduce-only
     * @param {float} [params.triggerPrice] The price at which a trigger order is triggered at
     * @param {string} [params.clientOrderId] client order id, (optional 128 bit hex string e.g. 0x1234567890abcdef1234567890abcdef)
     * @param {string} [params.vaultAddress] the vault address for order
     * @returns {object} an [order structure]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async editOrderWs (id: string, symbol: string, type: string, side: string, amount: Num = undefined, price: Num = undefined, params = {}) {
        await this.loadMarkets ();
        const market = this.market (symbol);
        const url = this.urls['api']['ws']['public'];
        const [ order, globalParams ] = this.parseCreateEditOrderArgs (id, symbol, type, side, amount, price, params);
        const postRequest = this.editOrdersRequest ([ order as any ], globalParams);
        const wrapped = this.wrapAsPostAction (postRequest);
        const request = this.safeDict (wrapped, 'request', {});
        const requestId = this.safeString (wrapped, 'requestId');
        const response = await this.watch (url, requestId, request, requestId);
        // response is the same as in this.editOrder
        const responseObject = this.safeDict (response, 'response', {});
        const dataObject = this.safeDict (responseObject, 'data', {});
        const statuses = this.safeList (dataObject, 'statuses', []);
        const first = this.safeDict (statuses, 0, {});
        const parsedOrder = this.parseOrder (first, market);
        return parsedOrder;
    }

    /**
     * @method
     * @name hyperliquid#cancelOrdersWs
     * @description cancel multiple orders using WebSocket post request
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests
     * @param {string[]} ids list of order ids to cancel
     * @param {string} symbol unified symbol of the market the orders were made in
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string[]} [params.clientOrderId] list of client order ids to cancel instead of order ids
     * @param {string} [params.vaultAddress] the vault address for order cancellation
     * @returns {object[]} a list of [order structures]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async cancelOrdersWs (ids: string[], symbol: Str = undefined, params = {}) {
        this.checkRequiredCredentials ();
        await this.loadMarkets ();
        const request = this.cancelOrdersRequest (ids, symbol, params);
        const url = this.urls['api']['ws']['public'];
        const wrapped = this.wrapAsPostAction (request);
        const wsRequest = this.safeDict (wrapped, 'request', {});
        const requestId = this.safeString (wrapped, 'requestId');
        const response = await this.watch (url, requestId, wsRequest, requestId);
        const responseObj = this.safeDict (response, 'response', {});
        const data = this.safeDict (responseObj, 'data', {});
        const statuses = this.safeList (data, 'statuses', []);
        const orders = [];
        for (let i = 0; i < statuses.length; i++) {
            const status = statuses[i];
            orders.push (this.safeOrder ({
                'info': status,
                'status': status,
            }));
        }
        return orders as Order[];
    }

    /**
     * @method
     * @name hyperliquid#cancelOrderWs
     * @description cancel a single order using WebSocket post request
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests
     * @param {string} id order id to cancel
     * @param {string} symbol unified symbol of the market the order was made in
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.clientOrderId] client order id to cancel instead of order id
     * @param {string} [params.vaultAddress] the vault address for order cancellation
     * @returns {object} an [order structure]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async cancelOrderWs (id: string, symbol: Str = undefined, params = {}) {
        const orders = await this.cancelOrdersWs ([ id ], symbol, params);
        return this.safeDict (orders, 0) as Order;
    }

    /**
     * @method
     * @name hyperliquid#watchOrderBook
     * @description watches information on open orders with bid (buy) and ask (sell) prices, volumes and other data
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified symbol of the market to fetch the order book for
     * @param {int} [limit] the maximum amount of order book entries to return
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} A dictionary of [order book structures]{@link https://docs.ccxt.com/?id=order-book-structure} indexed by market symbols
     */
    async watchOrderBook (symbol: string, limit: Int = undefined, params = {}): Promise<OrderBook> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const messageHash = 'orderbook:' + symbol;
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'subscribe',
            'subscription': {
                'type': 'l2Book',
                'coin': market['swap'] ? market['baseName'] : market['id'],
            },
        };
        const message = this.extend (request, params);
        const orderbook = await this.watch (url, messageHash, message, messageHash);
        return orderbook.limit ();
    }

    /**
     * @method
     * @name hyperliquid#unWatchOrderBook
     * @description unWatches information on open orders with bid (buy) and ask (sell) prices, volumes and other data
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified symbol of the market to fetch the order book for
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} A dictionary of [order book structures]{@link https://docs.ccxt.com/?id=order-book-structure} indexed by market symbols
     */
    async unWatchOrderBook (symbol: string, params = {}): Promise<any> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const subMessageHash = 'orderbook:' + symbol;
        const messageHash = 'unsubscribe:' + subMessageHash;
        const url = this.urls['api']['ws']['public'];
        const id = this.nonce ().toString ();
        const request: Dict = {
            'id': id,
            'method': 'unsubscribe',
            'subscription': {
                'type': 'l2Book',
                'coin': market['swap'] ? market['baseName'] : market['id'],
            },
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, messageHash);
    }

    handleOrderBook (client, message) {
        //
        //     {
        //         "channel": "l2Book",
        //         "data": {
        //             "coin": "BTC",
        //             "time": 1710131872708,
        //             "levels": [
        //                 [
        //                     {
        //                         "px": "68674.0",
        //                         "sz": "0.97139",
        //                         "n": 4
        //                     }
        //                 ],
        //                 [
        //                     {
        //                         "px": "68675.0",
        //                         "sz": "0.04396",
        //                         "n": 1
        //                     }
        //                 ]
        //             ]
        //         }
        //     }
        //
        const entry = this.safeDict (message, 'data', {});
        const coin = this.safeString (entry, 'coin');
        const marketId = this.coinToMarketId (coin);
        const market = this.market (marketId);
        const symbol = market['symbol'];
        const rawData = this.safeList (entry, 'levels', []);
        const data: Dict = {
            'bids': this.safeList (rawData, 0, []),
            'asks': this.safeList (rawData, 1, []),
        };
        const timestamp = this.safeInteger (entry, 'time');
        const snapshot = this.parseOrderBook (data, symbol, timestamp, 'bids', 'asks', 'px', 'sz');
        if (!(symbol in this.orderbooks)) {
            const ob = this.orderBook (snapshot);
            this.orderbooks[symbol] = ob;
        }
        const orderbook = this.orderbooks[symbol];
        orderbook.reset (snapshot);
        const messageHash = 'orderbook:' + symbol;
        client.resolve (orderbook, messageHash);
    }

    /**
     * @method
     * @name hyperliquid#watchTicker
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @description watches a price ticker, a statistical calculation with the information calculated over the past 24 hours for a specific market
     * @param {string} symbol unified symbol of the market to fetch the ticker for
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.channel] 'allMids' to subscribe only the mids channel (last/close only, no volumes); default subscribes allMids + allDexsAssetCtxs
     * @returns {object} a [ticker structure]{@link https://docs.ccxt.com/?id=ticker-structure}
     */
    async watchTicker (symbol: string, params = {}): Promise<Ticker> {
        const market = this.market (symbol);
        symbol = market['symbol'];
        // try to infer dex from market
        const dexName = this.safeString (this.safeDict (market, 'info', {}), 'dex');
        if (dexName) {
            params = this.extend (params, { 'dex': dexName });
        }
        const tickers = await this.watchTickers ([ symbol ], params);
        return tickers[symbol];
    }

    /**
     * @method
     * @name hyperliquid#watchTickers
     * @description watches a price ticker, a statistical calculation with the information calculated over the past 24 hours for all markets of a specific list
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string[]} symbols unified symbol of the market to fetch the ticker for
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.channel] 'allMids' to subscribe only the mids channel (last/close only, no volumes); default subscribes allMids + allDexsAssetCtxs
     * @param {string} [params.dex] for for hip3 tokens subscription, eg: 'xyz' or 'flx`, if symbols are provided we will infer it from the first symbol's market
     * @returns {object} a [ticker structure]{@link https://docs.ccxt.com/?id=ticker-structure}
     */
    async watchTickers (symbols: Strings = undefined, params = {}): Promise<Tickers> {
        await this.loadMarkets ();
        symbols = this.marketSymbols (symbols, undefined, true);
        let messageHash = 'tickers';
        const url = this.urls['api']['ws']['public'];
        let defaultDex = this.safeString (params, 'dex');
        const firstSymbol = this.safeString (symbols, 0);
        if (firstSymbol !== undefined) {
            const market = this.market (firstSymbol);
            const dexName = this.safeString (this.safeDict (market, 'info', {}), 'dex');
            if (dexName !== undefined) {
                defaultDex = dexName;
            }
        }
        let channel = 'default';
        [ channel, params ] = this.handleOptionAndParams (params, 'watchTickers', 'channel', channel);
        if (channel === 'webData2') {
            throw new NotSupported (this.id + ' watchTickers: hyperliquid removed the webData2 channel; use the default (allMids + allDexsAssetCtxs) or channel=allMids');
        }
        // steady-state discipline (mirrors watchBidsAsks): only send a subscribe frame
        // for a channel that isn't already subscribed on this client; a call where
        // every channel is already subscribed sends nothing and only awaits the
        // single shared future below — never allocates a fresh Future.race wrapper
        // per already-steady channel (see watchBidsAsks for the Go FutureRace leak
        // this avoids)
        const client = this.client (url);
        if (defaultDex !== undefined) {
            params = this.omit (params, 'dex');
            messageHash = 'tickers:' + defaultDex;
            const subscribeHash = 'allMids:' + defaultDex;
            if (!(subscribeHash in client.subscriptions)) {
                const dexRequest: Dict = {
                    'method': 'subscribe',
                    'subscription': {
                        'type': 'allMids',
                        'dex': defaultDex,
                    },
                };
                this.watchMultiple (url, [ messageHash ], this.extend (dexRequest, params), [ subscribeHash ]);
            }
        } else {
            if (!('allMids' in client.subscriptions)) {
                const midsRequest: Dict = {
                    'method': 'subscribe',
                    'subscription': {
                        'type': 'allMids',
                    },
                };
                this.watchMultiple (url, [ messageHash ], this.extend (midsRequest, params), [ 'allMids' ]);
            }
            if ((channel !== 'allMids') && !('allDexsAssetCtxs' in client.subscriptions)) {
                const ctxsRequest: Dict = {
                    'method': 'subscribe',
                    'subscription': {
                        'type': 'allDexsAssetCtxs',
                    },
                };
                this.watchMultiple (url, [ messageHash ], this.extend (ctxsRequest, params), [ 'allDexsAssetCtxs' ]);
            }
        }
        const tickers = await this.watchMultiple (url, [ messageHash ], undefined, undefined);
        if (this.newUpdates) {
            return this.filterByArrayTickers (tickers, 'symbol', symbols);
        }
        return this.tickers;
    }

    /**
     * @method
     * @name hyperliquid#unWatchTickers
     * @description unWatches a price ticker, a statistical calculation with the information calculated over the past 24 hours for all markets of a specific list
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string[]} symbols unified symbol of the market to fetch the ticker for
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} a [ticker structure]{@link https://docs.ccxt.com/?id=ticker-structure}
     */
    async unWatchTickers (symbols: Strings = undefined, params = {}): Promise<any> {
        await this.loadMarkets ();
        symbols = this.marketSymbols (symbols, undefined, true);
        const url = this.urls['api']['ws']['public'];
        let defaultDex = this.safeString (params, 'dex');
        const firstSymbol = this.safeString (symbols, 0);
        if (firstSymbol !== undefined) {
            const market = this.market (firstSymbol);
            const dexName = this.safeString (this.safeDict (market, 'info', {}), 'dex');
            if (dexName !== undefined) {
                defaultDex = dexName;
            }
        }
        if (defaultDex !== undefined) {
            params = this.omit (params, 'dex');
            const messageHash = 'unsubscribe:allMids:' + defaultDex;
            const request: Dict = {
                'method': 'unsubscribe',
                'subscription': {
                    'type': 'allMids',
                    'dex': defaultDex,
                },
            };
            return await this.watchMultiple (url, [ messageHash ], this.extend (request, params), [ messageHash ]);
        }
        // conditional, per-channel awaits (mirrors unWatchBidsAsks): each channel's
        // unwatch resolves only on ITS OWN ack, so a two-channel unwatch cannot be
        // reported complete after just the first channel's ack while the other is
        // still subscribed. Unlike the (symmetric) subscribe path, only send/await a
        // channel's unsubscribe frame if it is actually subscribed — sending an
        // unsubscribe for a channel that was never subscribed (eg. after
        // channel='allMids', only 'allMids' was ever sent) gets no ack from
        // hyperliquid and would hang the await forever instead of no-opping.
        const client = this.client (url);
        let result: any = true;
        if ('allMids' in client.subscriptions) {
            const midsHash = 'unsubscribe:allMids';
            const midsRequest: Dict = {
                'method': 'unsubscribe',
                'subscription': {
                    'type': 'allMids',
                },
            };
            result = await this.watchMultiple (url, [ midsHash ], this.extend (midsRequest, params), [ midsHash ]);
        }
        if ('allDexsAssetCtxs' in client.subscriptions) {
            const ctxsHash = 'unsubscribe:allDexsAssetCtxs';
            const ctxsRequest: Dict = {
                'method': 'unsubscribe',
                'subscription': {
                    'type': 'allDexsAssetCtxs',
                },
            };
            result = await this.watchMultiple (url, [ ctxsHash ], this.extend (ctxsRequest, params), [ ctxsHash ]);
        }
        return result;
    }

    /**
     * @method
     * @name hyperliquid#watchBidsAsks
     * @description watches best bid & ask (top of book) for symbols via the per-coin bbo channel
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string[]} symbols unified symbols; REQUIRED to be present in loaded markets (an unknown coin id makes hyperliquid close the whole connection)
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} a dictionary of [ticker structures]{@link https://docs.ccxt.com/?id=ticker-structure} — resolves on the next bbo update for ANY subscribed coin (not necessarily one of the requested symbols); filtered to the requested symbols, which may yield an empty dict for that particular resolution
     */
    async watchBidsAsks (symbols: Strings = undefined, params = {}): Promise<Tickers> {
        await this.loadMarkets ();
        symbols = this.marketSymbols (symbols, undefined, true);
        if (symbols === undefined) {
            symbols = this.symbols;
        }
        const url = this.urls['api']['ws']['public'];
        // shared messageHash across every symbol and every call (mirrors watchTickers'
        // 'tickers' hash): this method is called in a hot loop (100-300 calls/s over
        // 542 symbols in the ECK consumer), and the generated Go client's FutureRace
        // appends a subscriber channel to every underlying future it races without ever
        // unlinking losers — per-symbol messageHashes meant every call raced N futures,
        // leaking a blocked goroutine/link per quiet symbol on every single call. A
        // single shared hash means every call awaits the SAME underlying future, so
        // there is nothing to race and nothing to leak.
        const messageHash = 'bidsasks';
        const client = this.client (url);
        for (let i = 0; i < symbols.length; i++) {
            const market = this.market (symbols[i]);
            const coin = market['swap'] ? market['baseName'] : market['id'];
            const subscribeHash = 'bbo:' + coin;
            if (subscribeHash in client.subscriptions) {
                // steady state: already subscribed, nothing to send here — every call
                // (new or repeat) awaits the single shared future below instead
                continue;
            }
            const request: Dict = {
                'method': 'subscribe',
                'subscription': {
                    'type': 'bbo',
                    'coin': coin,
                },
            };
            // fire-and-forget: sends this coin's own subscribe frame (hyperliquid
            // requires one frame per coin) and marks it in client.subscriptions; the
            // returned per-coin future is intentionally discarded here (only the
            // single shared future awaited below is ever awaited by any call). This
            // mirrors the codebase-wide `this.spawn (...)` idiom (used bare/uncaught
            // across most ws exchanges) of discarding a Future whose only job is a
            // side effect. Empirically verified (scratchpad test, not shipped): if
            // the shared 'bidsasks' future rejects (e.g. a connection reset) while
            // one of these discarded futures is still pending, Node raises an
            // unhandled rejection for it — the same class of risk every bare
            // `this.spawn (...)` call in this codebase already accepts. The window is
            // bounded to first-subscribe/resubscribe bursts (the `continue` above
            // means steady-state calls never reach this branch at all), and no
            // closure-free way to silence a discarded promise exists anywhere in
            // ts/src/pro today (checked) short of duplicating base watchMultiple's
            // send/connect plumbing here, which is a larger and riskier change than
            // this bounded, precedented tradeoff.
            this.watchMultiple (url, [ messageHash ], this.extend (request, params), [ subscribeHash ]);
        }
        const newTickers = await this.watchMultiple (url, [ messageHash ], undefined, undefined);
        if (this.newUpdates) {
            return this.filterByArrayTickers (newTickers, 'symbol', symbols);
        }
        return this.filterByArrayTickers (this.bidsasks, 'symbol', symbols);
    }

    /**
     * @method
     * @name hyperliquid#unWatchBidsAsks
     * @description unsubscribes the per-coin bbo subscriptions for the given symbols
     * @param {string[]} symbols unified symbols
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {boolean} true when every requested coin's unsubscribe was acknowledged
     */
    async unWatchBidsAsks (symbols: Strings = undefined, params = {}): Promise<any> {
        await this.loadMarkets ();
        symbols = this.marketSymbols (symbols, undefined, true);
        if (symbols === undefined) {
            symbols = this.symbols;
        }
        const url = this.urls['api']['ws']['public'];
        // per-coin hash, sequential awaits: each coin's unwatch resolves only on ITS
        // own ack, so a multi-symbol unwatch cannot be reported complete after just
        // the FIRST coin's ack while the rest are still subscribed
        let result = undefined;
        for (let i = 0; i < symbols.length; i++) {
            const market = this.market (symbols[i]);
            const coin = market['swap'] ? market['baseName'] : market['id'];
            const messageHash = 'unsubscribe:bbo:' + coin;
            const request: Dict = {
                'method': 'unsubscribe',
                'subscription': {
                    'type': 'bbo',
                    'coin': coin,
                },
            };
            result = await this.watchMultiple (url, [ messageHash ], this.extend (request, params), [ messageHash ]);
        }
        return result;
    }

    handleBidAsk (client: Client, message) {
        //
        //     {
        //         "channel": "bbo",
        //         "data": {
        //             "coin": "BTC",
        //             "time": 1783946989607,
        //             "bbo": [
        //                 { "px": "62642.0", "sz": "3.12025", "n": 4 },
        //                 { "px": "62643.0", "sz": "18.27745", "n": 53 }
        //             ]
        //         }
        //     }
        //
        const data = this.safeDict (message, 'data', {});
        const parsedTicker = this.parseWsBidAsk (data);
        const symbol = parsedTicker['symbol'];
        this.bidsasks[symbol] = parsedTicker;
        const result: Dict = {};
        result[symbol] = parsedTicker;
        client.resolve (result, 'bidsasks');
    }

    parseWsBidAsk (data, market: Market = undefined) {
        const coin = this.safeString (data, 'coin');
        const marketId = this.resolveWsCoin (coin);
        market = this.safeMarket (marketId, market);
        const timestamp = this.safeInteger (data, 'time');
        const bbo = this.safeList (data, 'bbo', []);
        const bid = this.safeDict (bbo, 0, {});
        const ask = this.safeDict (bbo, 1, {});
        return this.safeTicker ({
            'symbol': market['symbol'],
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'bid': this.safeNumber (bid, 'px'),
            'bidVolume': this.safeNumber (bid, 'sz'),
            'ask': this.safeNumber (ask, 'px'),
            'askVolume': this.safeNumber (ask, 'sz'),
            'info': data,
        }, market);
    }

    /**
     * @method
     * @name hyperliquid#watchMyTrades
     * @description watches information on multiple trades made by the user
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified market symbol of the market orders were made in
     * @param {int} [since] the earliest time in ms to fetch orders for
     * @param {int} [limit] the maximum number of order structures to retrieve
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.user] user address, will default to this.walletAddress if not provided
     * @returns {object[]} a list of [order structures]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async watchMyTrades (symbol: Str = undefined, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Trade[]> {
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('watchMyTrades', params);
        await this.loadMarkets ();
        let messageHash = 'myTrades';
        if (symbol !== undefined) {
            symbol = this.symbol (symbol);
            messageHash += ':' + symbol;
        }
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'subscribe',
            'subscription': {
                'type': 'userFills',
                'user': userAddress,
            },
        };
        const message = this.extend (request, params);
        const trades = await this.watch (url, messageHash, message, messageHash);
        if (this.newUpdates) {
            limit = trades.getLimit (symbol, limit);
        }
        return this.filterBySymbolSinceLimit (trades, symbol, since, limit, true);
    }

    /**
     * @method
     * @name hyperliquid#unWatchMyTrades
     * @description unWatches information on multiple trades made by the user
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified market symbol of the market orders were made in
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.user] user address, will default to this.walletAddress if not provided
     * @returns {object[]} a list of [order structures]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async unWatchMyTrades (symbol: Str = undefined, params = {}): Promise<any> {
        await this.loadMarkets ();
        if (symbol !== undefined) {
            throw new NotSupported (this.id + ' unWatchMyTrades does not support a symbol argument, unWatch from all markets only');
        }
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('unWatchMyTrades', params);
        const messageHash = 'unsubscribe:myTrades';
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'unsubscribe',
            'subscription': {
                'type': 'userFills',
                'user': userAddress,
            },
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, messageHash);
    }

    resolveWsCoin (name: Str) {
        // spot markets are identified only by numeric universe index ("@107") on WS
        // frames, while REST spotMeta (and the market ids built from it) key spot
        // markets by their display name — spotIndexMap (populated in fetchSpotMarkets)
        // bridges "@107" back to the market id fetchSpotMarkets actually assigned
        if ((name !== undefined) && (name.indexOf ('@') >= 0)) {
            const spotIndexMap = this.safeDict (this.options, 'spotIndexMap', {});
            const mapped = this.safeString (spotIndexMap, name);
            if (mapped !== undefined) {
                return mapped;
            }
        }
        return this.coinToMarketId (name);
    }

    handleWsTickers (client: Client, message) {
        // allMids: {"channel":"allMids","data":{"mids":{"BTC":"62642.5","@107":"63.85",...},"dex":"..."} }
        const data = this.safeDict (message, 'data', {});
        const mids = this.safeDict (data, 'mids', {});
        const keys = Object.keys (mids);
        for (let i = 0; i < keys.length; i++) {
            const name = keys[i];
            if (name.indexOf ('#') >= 0) {
                continue; // builder-dex synthetic entries — not loadable markets
            }
            const marketId = this.resolveWsCoin (name);
            const marketType = (name.indexOf ('@') >= 0) || (name.indexOf ('/') >= 0) ? 'spot' : 'swap';
            const market = this.safeMarket (marketId, undefined, undefined, marketType);
            const symbol = market['symbol'];
            if (!(symbol in this.markets)) {
                continue;
            }
            const mid = this.safeNumber (mids, name);
            this.mergeWsTicker (symbol, market, {
                'last': mid,
                'close': mid,
            });
        }
        let messageHash = 'tickers';
        const dexMessage = this.safeString (data, 'dex');
        if (dexMessage !== undefined) {
            messageHash += ':' + dexMessage;
        }
        client.resolve (this.tickers, messageHash);
        return true;
    }

    handleAllDexsAssetCtxs (client: Client, message) {
        // {"channel":"allDexsAssetCtxs","data":{"ctxs":[["",[<ctx>,...]],["dexName",[...]]]}}
        const data = this.safeDict (message, 'data', {});
        const ctxsByDex = this.safeList (data, 'ctxs', []);
        for (let d = 0; d < ctxsByDex.length; d++) {
            const pair = this.safeList (ctxsByDex, d, []);
            const dexName = this.safeString (pair, 0, '');
            if (dexName !== '') {
                continue; // main dex only — builder dexes are not loaded as markets
            }
            const ctxs = this.safeList (pair, 1, []);
            const meta = this.safeDict (this.options, 'perpUniverse', {});
            const universe = this.safeList (meta, 'universe', []);
            if (universe.length !== ctxs.length) {
                // self-heal: perpUniverse (captured once in fetchSwapMarkets) has drifted
                // from the live universe (eg. hyperliquid listed/delisted a perp) — kick
                // off a markets reload so fetchSwapMarkets recaptures options['perpUniverse'].
                // Guarded + rate-limited: only fire (and log) on the false→true transition
                // so a run of stale frames while the reload is in flight produces exactly
                // one reload and one log line, not one per frame. Bare this.spawn (...) is
                // the established fire-and-forget idiom in this codebase (see watchBidsAsks).
                if (!this.safeBool (this.options, 'perpUniverseReloading', false)) {
                    this.options['perpUniverseReloading'] = true;
                    this.log ('hyperliquid allDexsAssetCtxs length mismatch: universe=' + universe.length.toString () + ' ctxs=' + ctxs.length.toString () + ' — dropping frame, reloading markets');
                    this.spawn (this.loadMarkets, true);
                }
                return true; // order cannot be trusted; do not mis-assign volumes
            }
            if (this.safeBool (this.options, 'perpUniverseReloading', false)) {
                // a subsequent frame's length now matches perpUniverse — the reload (or a
                // race with another trigger) resolved the drift; clear the guard so a
                // future mismatch can trigger a fresh reload
                this.options['perpUniverseReloading'] = false;
            }
            for (let i = 0; i < ctxs.length; i++) {
                const universeEntry = this.safeDict (universe, i, {});
                const coin = this.safeString (universeEntry, 'name');
                const marketId = this.coinToMarketId (coin);
                const market = this.safeMarket (marketId, undefined, undefined, 'swap');
                const symbol = market['symbol'];
                if (!(symbol in this.markets)) {
                    continue;
                }
                const ctx = this.safeDict (ctxs, i, {});
                const mid = this.safeNumber (ctx, 'midPx');
                const prev = this.safeNumber (ctx, 'prevDayPx');
                let change = undefined;
                let percentage = undefined;
                if ((mid !== undefined) && (prev !== undefined) && (prev > 0)) {
                    change = mid - prev;
                    percentage = (change / prev) * 100;
                }
                this.mergeWsTicker (symbol, market, {
                    'quoteVolume': this.safeNumber (ctx, 'dayNtlVlm'),
                    'baseVolume': this.safeNumber (ctx, 'dayBaseVlm'),
                    'previousClose': prev,
                    'open': prev,
                    'change': change,
                    'percentage': percentage,
                });
            }
        }
        client.resolve (this.tickers, 'tickers');
        return true;
    }

    mergeWsTicker (symbol: string, market, update: Dict) {
        // field-level merge: a partial update must never erase fresher fields — allMids
        // and allDexsAssetCtxs own disjoint field groups (last/close vs the volume +
        // change fields), and both arrive ordered on one socket, so no per-field
        // timestamp guard is needed (see brief §4.2 rationale)
        //
        // builds a NEW object instead of mutating the previously stored ticker in
        // place: in the generated Go, the WS reader goroutine calls this on every
        // frame while a consumer goroutine may still hold a reference to the object
        // previously returned via this.tickers[symbol] — mutating that shared object
        // is a data race. Copying into a fresh dict and only ever replacing the map
        // entry keeps every previously handed-out reference an immutable snapshot.
        const previous = this.safeDict (this.tickers, symbol) as Dict;
        const base = (previous === undefined) ? (this.parseWsTicker ({}, market) as Dict) : previous;
        const fresh: Dict = {};
        const baseKeys = Object.keys (base);
        for (let i = 0; i < baseKeys.length; i++) {
            const key = baseKeys[i];
            fresh[key] = base[key];
        }
        const updateKeys = Object.keys (update);
        for (let i = 0; i < updateKeys.length; i++) {
            const key = updateKeys[i];
            const value = update[key];
            if (value !== undefined) {
                fresh[key] = value;
            }
        }
        fresh['symbol'] = symbol;
        fresh['timestamp'] = this.milliseconds ();
        fresh['datetime'] = this.iso8601 (fresh['timestamp']);
        this.tickers[symbol] = fresh as Ticker;
    }

    parseWsTicker (rawTicker, market: Market = undefined): Ticker {
        return this.parseTicker (rawTicker, market);
    }

    handleMyTrades (client: Client, message) {
        //
        //     {
        //         "channel": "userFills",
        //         "data": {
        //             "isSnapshot": true,
        //             "user": "0x15f43d1f2dee81424afd891943262aa90f22cc2a",
        //             "fills": [
        //                 {
        //                     "coin": "BTC",
        //                     "px": "72528.0",
        //                     "sz": "0.11693",
        //                     "side": "A",
        //                     "time": 1710208712815,
        //                     "startPosition": "0.11693",
        //                     "dir": "Close Long",
        //                     "closedPnl": "-0.81851",
        //                     "hash": "0xc5adaf35f8402750c218040b0a7bc301130051521273b6f398b3caad3e1f3f5f",
        //                     "oid": 7484888874,
        //                     "crossed": true,
        //                     "fee": "2.968244",
        //                     "liquidationMarkPx": null,
        //                     "tid": 567547935839686,
        //                     "cloid": null
        //                 }
        //             ]
        //         }
        //     }
        //
        const entry = this.safeDict (message, 'data', {});
        if (this.myTrades === undefined) {
            const limit = this.safeInteger (this.options, 'tradesLimit', 1000);
            this.myTrades = new ArrayCacheBySymbolById (limit);
        }
        const trades = this.myTrades;
        const symbols: Dict = {};
        const data = this.safeList (entry, 'fills', []);
        const dataLength = data.length;
        if (dataLength === 0) {
            return;
        }
        for (let i = 0; i < data.length; i++) {
            const rawTrade = data[i];
            const parsed = this.parseWsTrade (rawTrade);
            const symbol = parsed['symbol'];
            symbols[symbol] = true;
            trades.append (parsed);
        }
        const keys = Object.keys (symbols);
        for (let i = 0; i < keys.length; i++) {
            const currentMessageHash = 'myTrades:' + keys[i];
            client.resolve (trades, currentMessageHash);
        }
        // non-symbol specific
        const messageHash = 'myTrades';
        client.resolve (trades, messageHash);
    }

    /**
     * @method
     * @name hyperliquid#watchTrades
     * @description watches information on multiple trades made in a market
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified market symbol of the market trades were made in
     * @param {int} [since] the earliest time in ms to fetch trades for
     * @param {int} [limit] the maximum number of trade structures to retrieve
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object[]} a list of [trade structures]{@link https://docs.ccxt.com/?id=trade-structure}
     */
    async watchTrades (symbol: string, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Trade[]> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const messageHash = 'trade:' + symbol;
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'subscribe',
            'subscription': {
                'type': 'trades',
                'coin': market['swap'] ? market['baseName'] : market['id'],
            },
        };
        const message = this.extend (request, params);
        const trades = await this.watch (url, messageHash, message, messageHash);
        if (this.newUpdates) {
            limit = trades.getLimit (symbol, limit);
        }
        return this.filterBySinceLimit (trades, since, limit, 'timestamp', true);
    }

    /**
     * @method
     * @name hyperliquid#unWatchTrades
     * @description unWatches information on multiple trades made in a market
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified market symbol of the market trades were made in
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object[]} a list of [trade structures]{@link https://docs.ccxt.com/?id=trade-structure}
     */
    async unWatchTrades (symbol: string, params = {}): Promise<any> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const subMessageHash = 'trade:' + symbol;
        const messageHash = 'unsubscribe:' + subMessageHash;
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'unsubscribe',
            'subscription': {
                'type': 'trades',
                'coin': market['swap'] ? market['baseName'] : market['id'],
            },
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, messageHash);
    }

    handleTrades (client: Client, message) {
        //
        //     {
        //         "channel": "trades",
        //         "data": [
        //             {
        //                 "coin": "BTC",
        //                 "side": "A",
        //                 "px": "68517.0",
        //                 "sz": "0.005",
        //                 "time": 1710125266669,
        //                 "hash": "0xc872699f116e012186620407fc08a802015e0097c5cce74710697f7272e6e959",
        //                 "tid": 981894269203506
        //             }
        //         ]
        //     }
        //
        const entry = this.safeList (message, 'data', []);
        const first = this.safeDict (entry, 0, {});
        const coin = this.safeString (first, 'coin');
        const marketId = this.coinToMarketId (coin);
        const market = this.market (marketId);
        const symbol = market['symbol'];
        if (!(symbol in this.trades)) {
            const limit = this.safeInteger (this.options, 'tradesLimit', 1000);
            const stored = new ArrayCache (limit);
            this.trades[symbol] = stored;
        }
        const trades = this.trades[symbol];
        for (let i = 0; i < entry.length; i++) {
            const data = this.safeDict (entry, i);
            const trade = this.parseWsTrade (data);
            trades.append (trade);
        }
        const messageHash = 'trade:' + symbol;
        client.resolve (trades, messageHash);
    }

    parseWsTrade (trade: Dict, market: Market = undefined): Trade {
        //
        // fetchMyTrades
        //
        //     {
        //         "coin": "BTC",
        //         "px": "72528.0",
        //         "sz": "0.11693",
        //         "side": "A",
        //         "time": 1710208712815,
        //         "startPosition": "0.11693",
        //         "dir": "Close Long",
        //         "closedPnl": "-0.81851",
        //         "hash": "0xc5adaf35f8402750c218040b0a7bc301130051521273b6f398b3caad3e1f3f5f",
        //         "oid": 7484888874,
        //         "crossed": true,
        //         "fee": "2.968244",
        //         "liquidationMarkPx": null,
        //         "tid": 567547935839686,
        //         "cloid": null
        //     }
        //
        // fetchTrades
        //
        //     {
        //         "coin": "BTC",
        //         "side": "A",
        //         "px": "68517.0",
        //         "sz": "0.005",
        //         "time": 1710125266669,
        //         "hash": "0xc872699f116e012186620407fc08a802015e0097c5cce74710697f7272e6e959",
        //         "tid": 981894269203506
        //     }
        //
        const timestamp = this.safeInteger (trade, 'time');
        const price = this.safeString (trade, 'px');
        const amount = this.safeString (trade, 'sz');
        const coin = this.safeString (trade, 'coin');
        const marketId = this.coinToMarketId (coin);
        market = this.safeMarket (marketId, undefined);
        const symbol = market['symbol'];
        const id = this.safeString (trade, 'tid');
        let side = this.safeString (trade, 'side');
        if (side !== undefined) {
            side = (side === 'A') ? 'sell' : 'buy';
        }
        const fee = this.safeString (trade, 'fee');
        return this.safeTrade ({
            'info': trade,
            'timestamp': timestamp,
            'datetime': this.iso8601 (timestamp),
            'symbol': symbol,
            'id': id,
            'order': this.safeString (trade, 'oid'),
            'type': undefined,
            'side': side,
            'takerOrMaker': undefined,
            'price': price,
            'amount': amount,
            'cost': undefined,
            'fee': { 'cost': fee, 'currency': 'USDC' },
        }, market);
    }

    /**
     * @method
     * @name hyperliquid#watchOHLCV
     * @description watches historical candlestick data containing the open, high, low, close price, and the volume of a market
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified symbol of the market to fetch OHLCV data for
     * @param {string} timeframe the length of time each candle represents
     * @param {int} [since] timestamp in ms of the earliest candle to fetch
     * @param {int} [limit] the maximum amount of candles to fetch
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {int[][]} A list of candles ordered as timestamp, open, high, low, close, volume
     */
    async watchOHLCV (symbol: string, timeframe: string = '1m', since: Int = undefined, limit: Int = undefined, params = {}): Promise<OHLCV[]> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'subscribe',
            'subscription': {
                'type': 'candle',
                'coin': market['swap'] ? market['baseName'] : market['id'],
                'interval': timeframe,
            },
        };
        const messageHash = 'candles:' + timeframe + ':' + symbol;
        const message = this.extend (request, params);
        const ohlcv = await this.watch (url, messageHash, message, messageHash);
        if (this.newUpdates) {
            limit = ohlcv.getLimit (symbol, limit);
        }
        return this.filterBySinceLimit (ohlcv, since, limit, 0, true);
    }

    /**
     * @method
     * @name hyperliquid#unWatchOHLCV
     * @description watches historical candlestick data containing the open, high, low, close price, and the volume of a market
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified symbol of the market to fetch OHLCV data for
     * @param {string} timeframe the length of time each candle represents
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {int[][]} A list of candles ordered as timestamp, open, high, low, close, volume
     */
    async unWatchOHLCV (symbol: string, timeframe: string = '1m', params = {}): Promise<any> {
        await this.loadMarkets ();
        const market = this.market (symbol);
        symbol = market['symbol'];
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'unsubscribe',
            'subscription': {
                'type': 'candle',
                'coin': market['swap'] ? market['baseName'] : market['id'],
                'interval': timeframe,
            },
        };
        const subMessageHash = 'candles:' + timeframe + ':' + symbol;
        const messagehash = 'unsubscribe:' + subMessageHash;
        const message = this.extend (request, params);
        return await this.watch (url, messagehash, message, messagehash);
    }

    handleOHLCV (client: Client, message) {
        //
        //     {
        //         channel: 'candle',
        //         data: {
        //             t: 1710146280000,
        //             T: 1710146339999,
        //             s: 'BTC',
        //             i: '1m',
        //             o: '71400.0',
        //             c: '71411.0',
        //             h: '71422.0',
        //             l: '71389.0',
        //             v: '1.20407',
        //             n: 20
        //         }
        //     }
        //
        const data = this.safeDict (message, 'data', {});
        const base = this.safeString (data, 's');
        const marketId = this.coinToMarketId (base);
        const symbol = this.safeSymbol (marketId);
        const timeframe = this.safeString (data, 'i');
        if (!(symbol in this.ohlcvs)) {
            this.ohlcvs[symbol] = {};
        }
        if (!(timeframe in this.ohlcvs[symbol])) {
            const limit = this.safeInteger (this.options, 'OHLCVLimit', 1000);
            const stored = new ArrayCacheByTimestamp (limit);
            this.ohlcvs[symbol][timeframe] = stored;
        }
        const ohlcv = this.ohlcvs[symbol][timeframe];
        const parsed = this.parseOHLCV (data);
        ohlcv.append (parsed);
        const messageHash = 'candles:' + timeframe + ':' + symbol;
        client.resolve (ohlcv, messageHash);
    }

    handleWsPost (client: Client, message: Dict) {
        //    {
        //         channel: "post",
        //         data: {
        //             id: <number>,
        //             response: {
        //                  type: "info" | "action" | "error",
        //                  payload: { ... }
        //         }
        //    }
        const data = this.safeDict (message, 'data');
        const id = this.safeString (data, 'id');
        const response = this.safeDict (data, 'response');
        const payload = this.safeDict (response, 'payload');
        client.resolve (payload, id);
    }

    /**
     * @method
     * @name hyperliquid#watchBalance
     * @description watch balance and get the amount of funds available for trading or funds locked in orders
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.dex] for for hip3 tokens subscription, eg: 'xyz' or 'flx'
     * @returns {object} a [balance structure]{@link https://docs.ccxt.com/?id=balance-structure}
     */
    async watchBalance (params = {}): Promise<Balances> {
        await this.loadMarkets ();
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('watchBalance', params);
        let type = undefined;
        [ type, params ] = this.handleMarketTypeAndParams ('watchBalance', undefined, params);
        let isUnifiedEnabled = undefined;
        [ isUnifiedEnabled, params ] = await this.isUnifiedEnabled ('watchBalance', userAddress, false, params);
        const dex = this.safeString (params, 'dex');
        const isSpot = ((type === 'spot') || isUnifiedEnabled) && (dex === undefined);
        const topic = (isSpot) ? 'spotState' : 'clearinghouseState';
        const messageHash = topic + '::balance';
        const url = this.urls['api']['ws']['public'];
        const subscription = {
            'type': topic,
            'user': userAddress,
        };
        if (isSpot) {
            if (isUnifiedEnabled) {
                subscription['isPortfolioMargin'] = true;
            }
        } else {
            if (dex !== undefined) {
                subscription['dex'] = dex;
            }
        }
        const request: Dict = {
            'method': 'subscribe',
            'subscription': subscription,
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, topic);
    }

    /**
     * @method
     * @name hyperliquid#unWatchBalance
     * @description unWatches balance
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} status of the unwatch request
     */
    async unWatchBalance (params = {}): Promise<any> {
        await this.loadMarkets ();
        const url = this.urls['api']['ws']['public'];
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('unWatchBalance', params);
        let type = undefined;
        [ type, params ] = this.handleMarketTypeAndParams ('unWatchBalance', undefined, params);
        let isUnifiedEnabled = undefined;
        [ isUnifiedEnabled, params ] = await this.isUnifiedEnabled ('unWatchBalance', userAddress, false, params);
        const dex = this.safeString (params, 'dex');
        const isSpot = ((type === 'spot') || isUnifiedEnabled) && (dex === undefined);
        const topic = (isSpot) ? 'spotState' : 'clearinghouseState';
        const messageHash = 'unsubscribe' + ':' + topic;
        const request: Dict = {
            'method': 'unsubscribe',
            'subscription': {
                'type': topic,
                'user': userAddress,
            },
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, messageHash);
    }

    handleBalance (client: Client, message) {
        //
        // spot
        // {
        //     "channel": "spotState",
        //     "data": {
        //         "user": "0xeeeeexxxxeeeee",
        //         "spotState": {
        //             "balances": [
        //                 {
        //                     "coin": "USDH",
        //                     "token": 360,
        //                     "total": "0.0",
        //                     "hold": "0.0",
        //                     "entryNtl": "0.0"
        //                 }
        //             ],
        //             "tokenToAvailableAfterMaintenance": [
        //                 [
        //                     0,
        //                     "56.1"
        //                 ]
        //             ]
        //         }
        //     }
        // }
        // swap
        // {
        //     "channel": "clearinghouseState",
        //     "data": {
        //         "dex": "",
        //         "user": "0xeeeeexxxxeeeee",
        //         "clearinghouseState": {
        //             "marginSummary": {
        //                 "accountValue": "0.0",
        //                 "totalNtlPos": "0.0",
        //                 "totalRawUsd": "0.0",
        //                 "totalMarginUsed": "0.0"
        //             },
        //             "crossMarginSummary": {
        //                 "accountValue": "0.0",
        //                 "totalNtlPos": "0.0",
        //                 "totalRawUsd": "0.0",
        //                 "totalMarginUsed": "0.0"
        //             },
        //             "crossMaintenanceMarginUsed": "0.0",
        //             "withdrawable": "0.0",
        //             "assetPositions": [],
        //             "time": 1776000003409
        //         }
        //     }
        // }
        //
        if (this.balance === undefined) {
            this.balance = {};
        }
        const topic = this.safeValue (message, 'channel');
        const messageHash = topic + '::balance';
        let info = undefined;
        let rawBalances = [];
        let account = undefined;
        let timestamp = undefined;
        const data = this.safeValue (message, 'data', []);
        if (topic === 'spotState') {
            const spotState = this.safeDict (data, 'spotState');
            rawBalances = this.safeList (spotState, 'balances');
            account = 'spot';
            info = rawBalances;
        }
        if (topic === 'clearinghouseState') {
            account = 'swap';
            const clearinghouseState = this.safeDict (data, 'clearinghouseState');
            rawBalances.push (clearinghouseState);
            info = clearinghouseState;
            timestamp = this.safeInteger (clearinghouseState, 'time');
            this.handlePositions (client, message);
        }
        for (let i = 0; i < rawBalances.length; i++) {
            this.parseWsBalance (rawBalances[i], account);
        }
        if (this.safeValue (this.balance, account) === undefined) {
            this.balance[account] = {};
        }
        this.balance[account]['info'] = info;
        this.balance[account]['timestamp'] = timestamp;
        this.balance[account]['datetime'] = this.iso8601 (timestamp);
        this.balance[account] = this.safeBalance (this.balance[account]);
        client.resolve (this.balance[account], messageHash);
    }

    parseWsBalance (balance, accountType = undefined) {
        //
        // spot
        //     {
        //         "coin": "USDH",
        //         "token": 360,
        //         "total": "0.0",
        //         "hold": "0.0",
        //         "entryNtl": "0.0"
        //     }
        // swap
        //     {
        //         "marginSummary": {
        //             "accountValue": "0.0",
        //             "totalNtlPos": "0.0",
        //             "totalRawUsd": "0.0",
        //             "totalMarginUsed": "0.0"
        //         },
        //         "crossMarginSummary": {
        //             "accountValue": "0.0",
        //             "totalNtlPos": "0.0",
        //             "totalRawUsd": "0.0",
        //             "totalMarginUsed": "0.0"
        //         },
        //         "crossMaintenanceMarginUsed": "0.0",
        //         "withdrawable": "0.0",
        //         "assetPositions": [],
        //         "time": 1776000003409
        //     }
        //
        const account = this.account ();
        const currencyId = this.safeString (balance, 'coin');
        let code = undefined;
        if (currencyId === undefined) {
            code = 'USDC';
            const marginSummary = this.safeDict (balance, 'marginSummary', {});
            account['free'] = this.safeString (balance, 'withdrawable');
            account['used'] = this.safeString (marginSummary, 'totalMarginUsed');
            account['total'] = this.safeString (marginSummary, 'accountValue');
        } else {
            code = this.safeCurrencyCode (currencyId);
            account['used'] = this.safeString (balance, 'hold');
            account['total'] = this.safeString (balance, 'total');
        }
        if (accountType !== undefined) {
            if (this.safeValue (this.balance, accountType) === undefined) {
                this.balance[accountType] = {};
            }
            this.balance[accountType][code] = account;
        } else {
            this.balance[code] = account;
        }
    }

    /**
     * @method
     * @name hyperliquid#watchPositions
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @description watch all open positions
     * @param {string[]} [symbols] list of unified market symbols
     * @param {int} [since] the earliest time in ms to fetch positions for
     * @param {int} [limit] the maximum number of positions to retrieve
     * @param {object} params extra parameters specific to the exchange API endpoint
     * @returns {object[]} a list of [position structure]{@link https://docs.ccxt.com/en/latest/manual.html#position-structure}
     */
    async watchPositions (symbols: Strings = undefined, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Position[]> {
        await this.loadMarkets ();
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('watchPositions', params);
        const topic = 'clearinghouseState';
        let messageHash = topic + '::positions';
        if (!this.isEmpty (symbols)) {
            symbols = this.marketSymbols (symbols);
            messageHash += '::' + symbols.join (',');
        }
        const url = this.urls['api']['ws']['public'];
        const subscription = {
            'type': topic,
            'user': userAddress,
        };
        const dexName = this.getDexFromSymbols ('watchPositions', symbols);
        if (dexName !== undefined) {
            subscription['dex'] = dexName;
        }
        const request: Dict = {
            'method': 'subscribe',
            'subscription': subscription,
        };
        const message = this.extend (request, params);
        const client = this.client (url);
        this.setPositionsCache (client, symbols);
        const cache = this.positions;
        const newPositions = await this.watch (url, messageHash, message, topic);
        if (this.newUpdates) {
            return newPositions;
        }
        return this.filterBySymbolsSinceLimit (cache, symbols, since, limit, true);
    }

    setPositionsCache (client: Client, symbols: Strings = undefined) {
        if (this.positions !== undefined) {
            return;
        }
        this.positions = new ArrayCacheBySymbolBySide ();
    }

    handlePositions (client, message) {
        if (this.positions === undefined) {
            this.positions = new ArrayCacheBySymbolBySide ();
        }
        const cache = this.positions;
        const data = this.safeDict (message, 'data', {});
        const clearinghouseState = this.safeDict (data, 'clearinghouseState', {});
        const newPositions = [];
        const rawPositions = this.safeList (clearinghouseState, 'assetPositions', []);
        for (let i = 0; i < rawPositions.length; i++) {
            const rawPosition = rawPositions[i];
            const position = this.parsePosition (rawPosition);
            newPositions.push (position);
            cache.append (position);
        }
        const baseMessageHash = 'clearinghouseState::positions';
        const messageHashes = this.findMessageHashes (client, baseMessageHash);
        for (let i = 0; i < messageHashes.length; i++) {
            const messageHash = messageHashes[i];
            const parts = messageHash.split ('::');
            const symbolsString = this.safeString (parts, 2);
            if (symbolsString === undefined) {
                continue;
            }
            const symbols = symbolsString.split (',');
            const positions = this.filterByArray (newPositions, 'symbol', symbols, false);
            if (!this.isEmpty (positions)) {
                client.resolve (positions, messageHash);
            }
        }
        client.resolve (newPositions, baseMessageHash);
    }

    /**
     * @method
     * @name hyperliquid#unWatchPositions
     * @description unWatches all open positions
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string[]} [symbols] list of unified market symbols
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @returns {object} status of the unwatch request
     */
    async unWatchPositions (symbols: Strings = undefined, params = {}): Promise<any> {
        await this.loadMarkets ();
        if (!this.isEmpty (symbols)) {
            throw new NotSupported (this.id + ' unWatchPositions() does not support a symbol parameter, you must unwatch all orders');
        }
        const messageHash = 'unsubscribe:clearinghouseState';
        const url = this.urls['api']['ws']['public'];
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('unWatchPositions', params);
        const request: Dict = {
            'method': 'unsubscribe',
            'subscription': {
                'type': 'clearinghouseState',
                'user': userAddress,
            },
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, messageHash);
    }

    /**
     * @method
     * @name hyperliquid#watchOrders
     * @description watches information on multiple orders made by the user
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified market symbol of the market orders were made in
     * @param {int} [since] the earliest time in ms to fetch orders for
     * @param {int} [limit] the maximum number of order structures to retrieve
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.user] user address, will default to this.walletAddress if not provided
     * @returns {object[]} a list of [order structures]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async watchOrders (symbol: Str = undefined, since: Int = undefined, limit: Int = undefined, params = {}): Promise<Order[]> {
        await this.loadMarkets ();
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('watchOrders', params);
        let market = undefined;
        let messageHash = 'order';
        if (symbol !== undefined) {
            market = this.market (symbol);
            symbol = market['symbol'];
            messageHash = messageHash + ':' + symbol;
        }
        const url = this.urls['api']['ws']['public'];
        const request: Dict = {
            'method': 'subscribe',
            'subscription': {
                'type': 'orderUpdates',
                'user': userAddress,
            },
        };
        const message = this.extend (request, params);
        const orders = await this.watch (url, messageHash, message, messageHash);
        if (this.newUpdates) {
            limit = orders.getLimit (symbol, limit);
        }
        return this.filterBySymbolSinceLimit (orders, symbol, since, limit, true);
    }

    /**
     * @method
     * @name hyperliquid#unWatchOrders
     * @description unWatches information on multiple orders made by the user
     * @see https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
     * @param {string} symbol unified market symbol of the market orders were made in
     * @param {object} [params] extra parameters specific to the exchange API endpoint
     * @param {string} [params.user] user address, will default to this.walletAddress if not provided
     * @returns {object[]} a list of [order structures]{@link https://docs.ccxt.com/?id=order-structure}
     */
    async unWatchOrders (symbol: Str = undefined, params = {}): Promise<any> {
        await this.loadMarkets ();
        if (symbol !== undefined) {
            throw new NotSupported (this.id + ' unWatchOrders() does not support a symbol argument, unWatch from all markets only');
        }
        const messageHash = 'unsubscribe:order';
        const url = this.urls['api']['ws']['public'];
        let userAddress = undefined;
        [ userAddress, params ] = this.handlePublicAddress ('unWatchOrders', params);
        const request: Dict = {
            'method': 'unsubscribe',
            'subscription': {
                'type': 'orderUpdates',
                'user': userAddress,
            },
        };
        const message = this.extend (request, params);
        return await this.watch (url, messageHash, message, messageHash);
    }

    handleOrder (client: Client, message) {
        //
        //     {
        //         channel: 'orderUpdates',
        //         data: [
        //             {
        //                 order: {
        //                     coin: 'BTC',
        //                     side: 'B',
        //                     limitPx: '30000.0',
        //                     sz: '0.001',
        //                     oid: 7456484275,
        //                     timestamp: 1710163596492,
        //                     origSz: '0.001'
        //                 },
        //                 status: 'open',
        //                 statusTimestamp: 1710163596492
        //             }
        //         ]
        //     }
        //
        const data = this.safeList (message, 'data', []);
        if (this.orders === undefined) {
            const limit = this.safeInteger (this.options, 'ordersLimit', 1000);
            this.orders = new ArrayCacheBySymbolById (limit);
        }
        const dataLength = data.length;
        if (dataLength === 0) {
            return;
        }
        const stored = this.orders;
        const messageHash = 'order';
        const marketSymbols: Dict = {};
        for (let i = 0; i < data.length; i++) {
            const rawOrder = data[i];
            const order = this.parseOrder (rawOrder);
            stored.append (order);
            const symbol = this.safeString (order, 'symbol');
            marketSymbols[symbol] = true;
        }
        const keys = Object.keys (marketSymbols);
        for (let i = 0; i < keys.length; i++) {
            const symbol = keys[i];
            const innerMessageHash = messageHash + ':' + symbol;
            client.resolve (stored, innerMessageHash);
        }
        client.resolve (stored, messageHash);
    }

    handleErrorMessage (client: Client, message): Bool {
        //
        //    {
        //      "channel": "post",
        //      "data": {
        //        "id": 1,
        //        "response": {
        //          "type": "action",
        //          "payload": {
        //            "status": "ok",
        //            "response": {
        //              "type": "order",
        //              "data": {
        //                "statuses": [
        //                  {
        //                    "error": "Order price cannot be more than 80% away from the reference price"
        //                  }
        //                ]
        //              }
        //            }
        //          }
        //        }
        //      }
        //    }
        //
        //    {
        //         "channel": "error",
        //         "data": "Error parsing JSON into valid websocket request: { \"type\": \"allMids\" }"
        //     }
        //
        const channel = this.safeString (message, 'channel', '');
        if (channel === 'error') {
            const retMsg = this.safeString (message, 'data', '');
            const bracketIndex = retMsg.indexOf ('{');
            if (bracketIndex < 0) {
                // no embedded request — cannot attribute; log, do NOT blanket-reject
                this.log ('hyperliquid unattributable ws error frame: ' + retMsg);
                return true;
            }
            const lastBrace = retMsg.lastIndexOf ('}');
            if (lastBrace < bracketIndex) {
                this.log ('hyperliquid unattributable ws error frame: ' + retMsg);
                return true;
            }
            const embedded = retMsg.slice (bracketIndex, lastBrace + 1);
            const request = this.parseJson (embedded); // returns undefined on invalid JSON (never throws)
            if (request === undefined) {
                this.log ('hyperliquid unparseable ws error frame: ' + retMsg);
                return true;
            }
            const subscription = this.safeDict (request, 'subscription', {});
            const subType = this.safeString (subscription, 'type', '');
            const coin = this.safeString (subscription, 'coin');
            const method = this.safeString (request, 'method');
            let messageHash = undefined;
            let subscribeHash = undefined;
            if (subType === 'bbo') {
                if (coin === undefined) {
                    this.log ('hyperliquid ws error frame for bbo without coin: ' + retMsg);
                    return true;
                }
                if (method === 'unsubscribe') {
                    messageHash = 'unsubscribe:bbo:' + coin;
                    subscribeHash = messageHash;
                } else {
                    // subscribe (or method absent/malformed): shared hash, matches
                    // watchBidsAsks — every call resolves via the same 'bidsasks' future
                    messageHash = 'bidsasks';
                    subscribeHash = 'bbo:' + coin;
                }
            } else if ((subType === 'allMids') || (subType === 'allDexsAssetCtxs') || (subType === 'webData2')) {
                const dex = this.safeString (subscription, 'dex');
                if (method === 'unsubscribe') {
                    // matches whatever hash unWatchTickers actually awaits: default path
                    // 'unsubscribe:allMids' / 'unsubscribe:allDexsAssetCtxs', dex path
                    // 'unsubscribe:allMids:'+dex — reject THAT pending future, not the
                    // shared subscribe-side 'tickers'/'tickers:dex' one
                    messageHash = 'unsubscribe:' + subType;
                    if (dex !== undefined) {
                        messageHash = messageHash + ':' + dex;
                    }
                    subscribeHash = messageHash;
                } else {
                    // subscribe (or method absent/malformed): shared 'tickers'/'tickers:dex'
                    // hash, matches watchTickers
                    messageHash = 'tickers';
                    subscribeHash = subType;
                    if (dex !== undefined) {
                        messageHash = 'tickers:' + dex;
                        subscribeHash = subType + ':' + dex;
                    }
                }
            } else if (subType === 'l2Book') {
                if (coin === undefined) {
                    this.log ('hyperliquid ws error frame for l2Book without coin: ' + retMsg);
                    return true;
                }
                const marketId = this.resolveWsCoin (coin);
                const market = this.safeMarket (marketId);
                messageHash = 'orderbook:' + market['symbol'];
                subscribeHash = messageHash;
            }
            if (subscribeHash !== undefined && (subscribeHash in client.subscriptions)) {
                delete client.subscriptions[subscribeHash];
            }
            // the tickers subscription may be registered under the messageHash itself
            // (legacy watchTickers registration) — clean both candidate keys so a
            // subsequent watch call re-sends the subscribe frame instead of hanging
            if (messageHash !== undefined && (messageHash in client.subscriptions)) {
                delete client.subscriptions[messageHash];
            }
            if (messageHash !== undefined) {
                const error = new ExchangeError (this.id + ' ' + retMsg);
                client.reject (error, messageHash);
            } else {
                this.log ('hyperliquid ws error for unmapped subscription type ' + subType + ': ' + retMsg);
            }
            return true;
        }
        const data = this.safeDict (message, 'data', {});
        let id = this.safeString (message, 'id');
        if (id === undefined) {
            id = this.safeString (data, 'id');
        }
        const response = this.safeDict (data, 'response', {});
        const payload = this.safeDict (response, 'payload', {});
        const status = this.safeString (payload, 'status');
        if (status !== undefined && status !== 'ok') {
            const errorMsg = new ExchangeError (this.id + ' ' + this.json (payload));
            client.reject (errorMsg, id);
            return true;
        }
        const type = this.safeString (payload, 'type');
        if (type === 'error') {
            const error = new ExchangeError (this.id + ' ' + this.json (payload));
            client.reject (error, id);
            return true;
        }
        try {
            this.handleErrors (0, '', '', '', {}, this.json (payload), payload, {}, {});
        } catch (e) {
            client.reject (e, id);
            return true;
        }
        return false;
    }

    handleOrderBookUnsubscription (client: Client, subscription: Dict) {
        //
        //        "subscription":{
        //           "type":"l2Book",
        //           "coin":"BTC",
        //           "nSigFigs":5,
        //           "mantissa":null
        //        }
        //
        const coin = this.safeString (subscription, 'coin');
        const marketId = this.coinToMarketId (coin);
        const symbol = this.safeSymbol (marketId);
        const subMessageHash = 'orderbook:' + symbol;
        const messageHash = 'unsubscribe:' + subMessageHash;
        this.cleanUnsubscription (client, subMessageHash, messageHash);
        if (symbol in this.orderbooks) {
            delete this.orderbooks[symbol];
        }
    }

    handleTradesUnsubscription (client: Client, subscription: Dict) {
        //
        const coin = this.safeString (subscription, 'coin');
        const marketId = this.coinToMarketId (coin);
        const symbol = this.safeSymbol (marketId);
        const subMessageHash = 'trade:' + symbol;
        const messageHash = 'unsubscribe:' + subMessageHash;
        this.cleanUnsubscription (client, subMessageHash, messageHash);
        if (symbol in this.trades) {
            delete this.trades[symbol];
        }
    }

    handleTickersUnsubscription (client: Client, subscription: Dict) {
        //
        //        "subscription":{
        //           "type":"allMids"
        //        }
        //  or, dex-scoped:
        //        "subscription":{
        //           "type":"allMids",
        //           "dex":"flx"
        //        }
        //  or:
        //        "subscription":{
        //           "type":"allDexsAssetCtxs"
        //        }
        //
        const type = this.safeString (subscription, 'type');
        const dex = this.safeString (subscription, 'dex');
        const subMessageHash = (dex !== undefined) ? (type + ':' + dex) : type;
        const messageHash = 'unsubscribe:' + subMessageHash;
        this.cleanUnsubscription (client, subMessageHash, messageHash);
        // the shared 'tickers'/'tickers:dex' watch future is fed by TWO independent
        // subscribe hashes in the default (non-dex) case — only reject it once BOTH
        // are gone, mirroring handleBidAskUnsubscription's "any remaining bbo:*"
        // check for the shared 'bidsasks' future
        const tickersMessageHash = (dex !== undefined) ? ('tickers:' + dex) : 'tickers';
        let hasRemainingTickersSubscription = false;
        const subscriptionKeys = Object.keys (client.subscriptions);
        for (let i = 0; i < subscriptionKeys.length; i++) {
            const key = subscriptionKeys[i];
            if (dex !== undefined) {
                if (key === ('allMids:' + dex)) {
                    hasRemainingTickersSubscription = true;
                    break;
                }
            } else if ((key === 'allMids') || (key === 'allDexsAssetCtxs')) {
                hasRemainingTickersSubscription = true;
                break;
            }
        }
        if (!hasRemainingTickersSubscription && (tickersMessageHash in client.futures)) {
            client.reject (new UnsubscribeError (this.id + ' ' + tickersMessageHash), tickersMessageHash);
        }
        if (dex === undefined) {
            // default path: this.tickers is shared with spot markets and other dexes
            // that may still be actively refreshed elsewhere — unsubscribing only ONE
            // of the two default channels must not wipe caches others still rely on
            return;
        }
        // dex-scoped unwatch: nothing else refreshes this dex's mids, safe (and
        // correct) to purge only that dex's tickers
        const symbols = Object.keys (this.tickers);
        for (let i = 0; i < symbols.length; i++) {
            const symbol = symbols[i];
            // safeMarket (symbol) treats its argument as a market id, not a unified
            // symbol — passing a unified symbol through it risks a lookup miss that
            // falls back to a synthetic/incomplete market object. Look the loaded
            // market up directly by symbol instead, and skip symbols with none.
            const market = this.safeDict (this.markets, symbol);
            if (market === undefined) {
                continue;
            }
            const marketDex = this.safeString (this.safeDict (market, 'info', {}), 'dex');
            if (marketDex === dex) {
                delete this.tickers[symbol];
            }
        }
    }

    handleBidAskUnsubscription (client: Client, subscription: Dict) {
        //
        //        "subscription":{
        //           "type":"bbo",
        //           "coin":"BTC"
        //        }
        //
        const coin = this.safeString (subscription, 'coin');
        const marketId = this.resolveWsCoin (coin);
        const symbol = this.safeSymbol (marketId);
        const subMessageHash = 'bbo:' + coin;
        const messageHash = 'unsubscribe:bbo:' + coin;
        this.cleanUnsubscription (client, subMessageHash, messageHash);
        // do NOT touch the shared 'bidsasks' watch future here: other coins may still
        // be streaming through it, so a single-coin unwatch must not reject/resolve
        // the whole shared stream
        if (symbol in this.bidsasks) {
            delete this.bidsasks[symbol];
        }
        let hasRemainingBboSubscription = false;
        const subscriptionKeys = Object.keys (client.subscriptions);
        for (let i = 0; i < subscriptionKeys.length; i++) {
            if (subscriptionKeys[i].startsWith ('bbo:')) {
                hasRemainingBboSubscription = true;
                break;
            }
        }
        if (!hasRemainingBboSubscription && ('bidsasks' in client.futures)) {
            // last bbo subscription gone ⇒ nothing can ever resolve the shared watch future
            client.reject (new UnsubscribeError (this.id + ' bidsasks'), 'bidsasks');
        }
    }

    handleOHLCVUnsubscription (client: Client, subscription: Dict) {
        const coin = this.safeString (subscription, 'coin');
        const marketId = this.coinToMarketId (coin);
        const symbol = this.safeSymbol (marketId);
        const interval = this.safeString (subscription, 'interval');
        const timeframe = this.findTimeframe (interval);
        const subMessageHash = 'candles:' + timeframe + ':' + symbol;
        const messageHash = 'unsubscribe:' + subMessageHash;
        this.cleanUnsubscription (client, subMessageHash, messageHash);
        if (symbol in this.ohlcvs) {
            if (timeframe in this.ohlcvs[symbol]) {
                delete this.ohlcvs[symbol][timeframe];
            }
        }
    }

    handleOrderUnsubscription (client: Client, subscription: Dict) {
        const subHash = 'order';
        const unSubHash = 'unsubscribe:' + subHash;
        this.cleanUnsubscription (client, subHash, unSubHash, true);
        const topicStructure = {
            'topic': 'orders',
        };
        this.cleanCache (topicStructure);
    }

    handleMyTradesUnsubscription (client: Client, subscription: Dict) {
        const subHash = 'myTrades';
        const unSubHash = 'unsubscribe:' + subHash;
        this.cleanUnsubscription (client, subHash, unSubHash, true);
        const topicStructure = {
            'topic': 'myTrades',
        };
        this.cleanCache (topicStructure);
    }

    handlePositionsUnsubscription (client: Client, subscription: Dict) {
        const subHash = 'clearinghouseState';
        const unSubHash = 'unsubscribe:' + subHash;
        this.cleanUnsubscription (client, subHash, unSubHash, true);
        const topicStructure = {
            'topic': 'positions',
        };
        this.cleanCache (topicStructure);
        // clean swap balance if it existed
        if ('swap' in this.balance) {
            delete this.balance['swap'];
        }
    }

    handleSpotBalanceUnsubscription (client: Client, subscription: Dict) {
        const subHash = 'spotState';
        const unSubHash = 'unsubscribe:' + subHash;
        this.cleanUnsubscription (client, subHash, unSubHash, true);
        if ('spot' in this.balance) {
            delete this.balance['spot'];
        }
    }

    handleSubscriptionResponse (client: Client, message) {
        // {
        //     "channel":"subscriptionResponse",
        //     "data":{
        //        "method":"unsubscribe",
        //        "subscription":{
        //           "type":"l2Book",
        //           "coin":"BTC",
        //           "nSigFigs":5,
        //           "mantissa":null
        //        }
        //     }
        // }
        //
        //  {
        //      "channel":"subscriptionResponse",
        //      "data":{
        //         "method":"unsubscribe",
        //         "subscription":{
        //            "type":"trades",
        //            "coin":"PURR/USDC"
        //         }
        //      }
        //  }
        //
        const data = this.safeDict (message, 'data', {});
        const method = this.safeString (data, 'method');
        if (method === 'unsubscribe') {
            const subscription = this.safeDict (data, 'subscription', {});
            const type = this.safeString (subscription, 'type');
            if (type === 'l2Book') {
                this.handleOrderBookUnsubscription (client, subscription);
            } else if (type === 'trades') {
                this.handleTradesUnsubscription (client, subscription);
            } else if ((type === 'allMids') || (type === 'allDexsAssetCtxs')) {
                this.handleTickersUnsubscription (client, subscription);
            } else if (type === 'bbo') {
                this.handleBidAskUnsubscription (client, subscription);
            } else if (type === 'candle') {
                this.handleOHLCVUnsubscription (client, subscription);
            } else if (type === 'orderUpdates') {
                this.handleOrderUnsubscription (client, subscription);
            } else if (type === 'userFills') {
                this.handleMyTradesUnsubscription (client, subscription);
            } else if (type === 'clearinghoustState') {
                this.handlePositionsUnsubscription (client, subscription);
            } else if (type === 'spotState') {
                this.handleSpotBalanceUnsubscription (client, subscription);
            }
        }
    }

    handleMessage (client: Client, message) {
        //
        // {
        //     "channel":"subscriptionResponse",
        //     "data":{
        //        "method":"unsubscribe",
        //        "subscription":{
        //           "type":"l2Book",
        //           "coin":"BTC",
        //           "nSigFigs":5,
        //           "mantissa":null
        //        }
        //     }
        // }
        //
        if (this.handleErrorMessage (client, message)) {
            return;
        }
        const topic = this.safeString (message, 'channel', '');
        const methods: Dict = {
            'pong': this.handlePong,
            'trades': this.handleTrades,
            'l2Book': this.handleOrderBook,
            'bbo': this.handleBidAsk,
            'candle': this.handleOHLCV,
            'orderUpdates': this.handleOrder,
            'userFills': this.handleMyTrades,
            'allMids': this.handleWsTickers,
            'allDexsAssetCtxs': this.handleAllDexsAssetCtxs,
            'post': this.handleWsPost,
            'subscriptionResponse': this.handleSubscriptionResponse,
            'clearinghouseState': this.handleBalance,
            'spotState': this.handleBalance,
        };
        const exacMethod = this.safeValue (methods, topic);
        if (exacMethod !== undefined) {
            exacMethod.call (this, client, message);
            return;
        }
        const keys = Object.keys (methods);
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            if (topic.indexOf (keys[i]) >= 0) {
                const method = methods[key];
                method.call (this, client, message);
                return;
            }
        }
    }

    ping (client: Client) {
        return {
            'method': 'ping',
        };
    }

    handlePong (client: Client, message) {
        //
        //   {
        //       "channel": "pong"
        //   }
        //
        client.lastPong = this.safeInteger (message, 'pong', this.milliseconds ());
        return message;
    }

    requestId (): number {
        const requestId = this.sum (this.safeInteger (this.options, 'requestId', 0), 1);
        this.options['requestId'] = requestId;
        return requestId;
    }

    wrapAsPostAction (request: Dict): Dict {
        const requestId = this.requestId ();
        return {
            'requestId': requestId,
            'request': {
                'method': 'post',
                'id': requestId,
                'request': {
                    'type': 'action',
                    'payload': request,
                },
            },
        };
    }
}
