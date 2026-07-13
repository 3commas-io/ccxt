


// CORE-647 Task 4 (spec §6.1): fixture-driven unit tests for the hyperliquid WS
// handlers touched by Tasks 1-3 (bbo / allMids / allDexsAssetCtxs / error-frame
// scoped reject). This fork's ts/src/pro/test/Exchange/ suite is a *live*
// black-box harness (calls exchange.watchXxx() against a real connection) —
// there is no fixture/unit-level location for feeding a captured frame straight
// into exchange.handleMessage(), so this is a standalone tsx-runnable script
// (wired via the "test-hyperliquid-ws" npm script) rather than an extension of
// that live suite.
//
// Frame shapes below are copied from the doc comments directly above each
// handler in ts/src/pro/hyperliquid.ts (handleBidAsk, handleWsTickers,
// handleAllDexsAssetCtxs) and, for the error frame, from the exact live frame
// captured during Task 1's smoke test (see .superpowers/sdd/task-1-report.md)
// since the illustrative doc-comment example omits the nested "subscription"
// object that handleErrorMessage actually parses.

import ccxt, { ExchangeError } from '../../../ccxt.js';
import Client from '../../base/ws/Client.js';

// Stub WS client: a real base/ws/Client subclass (so it satisfies handleMessage's
// `client: Client` typing) with resolve/reject overridden to just record calls
// instead of driving real Futures — the four scenario groups below only assert
// on *which* hash was resolved/rejected and with what value, never on Future
// resolution timing.
class MockClient extends Client {
    resolveCalls: { result: any, hash: string | undefined }[] = [];
    rejectCalls: { error: any, hash: string | undefined }[] = [];

    constructor () {
        super ('mock://hyperliquid-ws-handler-test', () => {}, () => {}, () => {}, () => {});
    }

    resolve (result: any, messageHash?: string) {
        this.resolveCalls.push ({ 'result': result, 'hash': messageHash });
        return result;
    }

    reject (result: any, messageHash?: string) {
        this.rejectCalls.push ({ 'error': result, 'hash': messageHash });
        return result;
    }
}

function buildExchange () {
    const exchange = new (ccxt as any).pro.hyperliquid ();
    // two swap (perp) markets + one spot market — enough to exercise both of
    // resolveWsCoin's branches (coinToMarketId for perps, spotIndexMap for the
    // "@N" spot WS ids) and the "!(symbol in this.markets)" gating that
    // handleWsTickers / handleAllDexsAssetCtxs both apply before touching a symbol
    exchange.setMarkets ([
        { 'id': '0', 'symbol': 'BTC/USDC:USDC', 'base': 'BTC', 'quote': 'USDC', 'settle': 'USDC', 'baseName': 'BTC', 'type': 'swap', 'spot': false, 'swap': true, 'contract': true },
        { 'id': '1', 'symbol': 'ETH/USDC:USDC', 'base': 'ETH', 'quote': 'USDC', 'settle': 'USDC', 'baseName': 'ETH', 'type': 'swap', 'spot': false, 'swap': true, 'contract': true },
        { 'id': 'PURR/USDC', 'symbol': 'PURR/USDC', 'base': 'PURR', 'quote': 'USDC', 'baseName': 'PURR', 'type': 'spot', 'spot': true, 'swap': false, 'contract': false },
    ]);
    // spotIndexMap: normally captured once in fetchSpotMarkets, bridging WS "@107"
    // ids back to the market id fetchSpotMarkets assigned (see resolveWsCoin's doc
    // comment) — seeded directly here since markets are stubbed, not fetched
    exchange.options['spotIndexMap'] = { '@107': 'PURR/USDC' };
    // perpUniverse: normally captured once in fetchSwapMarkets, zipped by index
    // against allDexsAssetCtxs' main-dex ctxs array (see that handler's doc comment)
    exchange.options['perpUniverse'] = { 'universe': [ { 'name': 'BTC' }, { 'name': 'ETH' } ] };
    return exchange;
}

let failures = 0;

function check (label: string, condition: boolean) {
    if (!condition) {
        failures += 1;
        console.error ('FAIL: ' + label);
    } else {
        console.log ('ok - ' + label);
    }
}

// ---------------------------------------------------------------------------
// Group 1: bbo frames — perp (BTC) coin form + spot ("@107") coin form
// ---------------------------------------------------------------------------
function testBidAsk () {
    const exchange = buildExchange ();
    const client = new MockClient ();
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
    const perpFrame = {
        'channel': 'bbo',
        'data': {
            'coin': 'BTC',
            'time': 1783946989607,
            'bbo': [
                { 'px': '62642.0', 'sz': '3.12025', 'n': 4 },
                { 'px': '62643.0', 'sz': '18.27745', 'n': 53 },
            ],
        },
    };
    exchange.handleMessage (client, perpFrame);
    const btc = exchange.bidsasks['BTC/USDC:USDC'];
    check ('bbo perp: this.bidsasks[BTC/USDC:USDC] populated', btc !== undefined);
    check ('bbo perp: bid = 62642.0', (btc !== undefined) && (btc['bid'] === 62642.0));
    check ('bbo perp: ask = 62643.0', (btc !== undefined) && (btc['ask'] === 62643.0));
    check ('bbo perp: client.resolve called with the shared "bidsasks" hash', client.resolveCalls.some ((c) => c.hash === 'bidsasks'));

    // spot coin identified only by numeric universe index on WS ("@107") —
    // resolveWsCoin bridges it via spotIndexMap to the spot market id ("PURR/USDC")
    const spotFrame = {
        'channel': 'bbo',
        'data': {
            'coin': '@107',
            'time': 1783946989608,
            'bbo': [
                { 'px': '0.20', 'sz': '100', 'n': 2 },
                { 'px': '0.21', 'sz': '50', 'n': 1 },
            ],
        },
    };
    exchange.handleMessage (client, spotFrame);
    const purr = exchange.bidsasks['PURR/USDC'];
    check ('bbo spot @107: this.bidsasks[PURR/USDC] populated via spotIndexMap', purr !== undefined);
    check ('bbo spot @107: bid = 0.20', (purr !== undefined) && (purr['bid'] === 0.20));
    check ('bbo spot @107: ask = 0.21', (purr !== undefined) && (purr['ask'] === 0.21));
}

// ---------------------------------------------------------------------------
// Group 2: allMids — "@N" spot keys + "#NNNN" synthetic keys skipped + merge invariant
// ---------------------------------------------------------------------------
function testAllMids () {
    const exchange = buildExchange ();
    const client = new MockClient ();
    // pre-seed a fuller ticker for BTC (as allDexsAssetCtxs would already have
    // populated it) to prove allMids' last/close-only update does not erase it
    exchange.tickers['BTC/USDC:USDC'] = exchange.safeTicker ({
        'symbol': 'BTC/USDC:USDC',
        'quoteVolume': 999,
        'baseVolume': 12,
    });
    //
    // allMids: {"channel":"allMids","data":{"mids":{"BTC":"62642.5","@107":"63.85",...}}}
    //
    const frame = {
        'channel': 'allMids',
        'data': {
            'mids': {
                'BTC': '62642.5',
                '@107': '63.85',
                '#12345': '1.23', // builder-dex synthetic entry — must be skipped, not a loadable market
            },
        },
    };
    exchange.handleMessage (client, frame);
    const btc = exchange.tickers['BTC/USDC:USDC'];
    check ('allMids: BTC last updated', btc['last'] === 62642.5);
    check ('allMids: BTC close updated', btc['close'] === 62642.5);
    check ('allMids: pre-seeded quoteVolume NOT erased by the last/close-only merge', btc['quoteVolume'] === 999);
    check ('allMids: pre-seeded baseVolume NOT erased by the last/close-only merge', btc['baseVolume'] === 12);
    const purr = exchange.tickers['PURR/USDC'];
    check ('allMids: spot "@107" key updates PURR/USDC via spotIndexMap', (purr !== undefined) && (purr['last'] === 63.85));
    check ('allMids: "#" builder-dex synthetic key skipped (no stray ticker entry)', !('#12345' in exchange.tickers));
    check ('allMids: client.resolve called with the "tickers" hash', client.resolveCalls.some ((c) => c.hash === 'tickers'));
}

// ---------------------------------------------------------------------------
// Group 3: allDexsAssetCtxs — happy path (volumes merged) + length-mismatch (dropped)
// ---------------------------------------------------------------------------
async function testAllDexsAssetCtxs () {
    const exchange = buildExchange ();
    const client = new MockClient ();
    //
    // {"channel":"allDexsAssetCtxs","data":{"ctxs":[["",[<ctx>,...]],["dexName",[...]]]}}
    //
    const happyFrame = {
        'channel': 'allDexsAssetCtxs',
        'data': {
            'ctxs': [
                [ '', [
                    { 'dayNtlVlm': '9450588.2273', 'dayBaseVlm': '120.5', 'midPx': '62642.5', 'prevDayPx': '61000.0' }, // BTC (universe[0])
                    { 'dayNtlVlm': '500000.0', 'dayBaseVlm': '80.2', 'midPx': '2369.45', 'prevDayPx': '2381.5' }, // ETH (universe[1])
                ] ],
                [ 'someBuilderDex', [ { 'dayNtlVlm': '1.0' } ] ], // non-main dex — must be skipped entirely
            ],
        },
    };
    exchange.handleMessage (client, happyFrame);
    const btc = exchange.tickers['BTC/USDC:USDC'];
    const eth = exchange.tickers['ETH/USDC:USDC'];
    check ('allDexsAssetCtxs happy: BTC quoteVolume merged', (btc !== undefined) && (btc['quoteVolume'] === 9450588.2273));
    check ('allDexsAssetCtxs happy: BTC baseVolume merged', (btc !== undefined) && (btc['baseVolume'] === 120.5));
    check ('allDexsAssetCtxs happy: ETH quoteVolume merged', (eth !== undefined) && (eth['quoteVolume'] === 500000));
    check ('allDexsAssetCtxs happy: ETH baseVolume merged', (eth !== undefined) && (eth['baseVolume'] === 80.2));
    check ('allDexsAssetCtxs happy: client.resolve called with the "tickers" hash', client.resolveCalls.some ((c) => c.hash === 'tickers'));

    // length mismatch: perpUniverse has 2 entries (BTC, ETH) but this frame's
    // main-dex ctxs array has only 1 — perpUniverse has drifted from the live
    // universe; the frame must be dropped wholesale (never mis-assigned by
    // position), and a self-heal markets reload kicked off exactly once
    const beforeBtc = exchange.tickers['BTC/USDC:USDC'];
    const beforeEth = exchange.tickers['ETH/USDC:USDC'];
    const resolveCallsBefore = client.resolveCalls.length;
    const reloadState = { 'called': false };
    exchange.loadMarkets = async (reload?: boolean) => {
        reloadState['called'] = true;
        return exchange.markets;
    };
    const mismatchFrame = {
        'channel': 'allDexsAssetCtxs',
        'data': {
            'ctxs': [
                [ '', [ { 'dayNtlVlm': '1.0', 'dayBaseVlm': '1.0', 'midPx': '1.0', 'prevDayPx': '1.0' } ] ], // only 1 ctx, universe has 2
            ],
        },
    };
    exchange.handleMessage (client, mismatchFrame);
    // drain the fire-and-forget this.spawn (this.loadMarkets, true) scheduled via setTimeout (0, ...)
    await new Promise ((resolve) => { setTimeout (resolve, 20); });
    check ('allDexsAssetCtxs mismatch: BTC ticker UNCHANGED (frame dropped)', exchange.tickers['BTC/USDC:USDC'] === beforeBtc);
    check ('allDexsAssetCtxs mismatch: ETH ticker UNCHANGED (frame dropped)', exchange.tickers['ETH/USDC:USDC'] === beforeEth);
    check ('allDexsAssetCtxs mismatch: client.resolve NOT called again (frame dropped before the resolve call)', client.resolveCalls.length === resolveCallsBefore);
    check ('allDexsAssetCtxs mismatch: self-heal reload guard flipped on', exchange.options['perpUniverseReloading'] === true);
    check ('allDexsAssetCtxs mismatch: self-heal markets reload triggered', reloadState['called'] === true);
}

// ---------------------------------------------------------------------------
// Group 4: error frame — scoped reject + subscription cleanup, incl. method-aware
// (subscribe vs unsubscribe) hash attribution; unattributable frame rejects nothing
// ---------------------------------------------------------------------------
function testErrorFrame () {
    const exchange = buildExchange ();

    // subscribe-side bbo error: only the shared 'bidsasks' future is rejected, and
    // only the FAILED coin's own 'bbo:<coin>' subscription entry is removed — a
    // different, still-good subscription must survive untouched
    {
        const client = new MockClient ();
        client.subscriptions['bbo:BTC'] = true;
        client.subscriptions['bbo:ETH'] = true;
        //
        //     {
        //         "channel": "error",
        //         "data": "Error parsing JSON into valid websocket request: {\"method\":\"subscribe\",\"subscription\":{\"type\":\"bbo\",\"coin\":\"BTC\"}}"
        //     }
        //
        const frame = {
            'channel': 'error',
            'data': 'Error parsing JSON into valid websocket request: {"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}',
        };
        exchange.handleMessage (client, frame);
        check ('error/bbo subscribe: client.reject called on the shared "bidsasks" hash', client.rejectCalls.some ((c) => c.hash === 'bidsasks'));
        check ('error/bbo subscribe: rejected with an Error instance (ExchangeError), never a raw string', client.rejectCalls.length > 0 && client.rejectCalls.every ((c) => c.error instanceof Error) && client.rejectCalls.some ((c) => c.error instanceof ExchangeError));
        check ('error/bbo subscribe: subscriptions["bbo:BTC"] deleted', !('bbo:BTC' in client.subscriptions));
        check ('error/bbo subscribe: subscriptions["bbo:ETH"] left untouched (different coin)', client.subscriptions['bbo:ETH'] === true);
    }

    // method-aware attribution: an UNSUBSCRIBE-side bbo error must reject the
    // 'unsubscribe:bbo:<coin>' hash, never the shared subscribe-side 'bidsasks' one
    {
        const client = new MockClient ();
        client.subscriptions['unsubscribe:bbo:ETH'] = true;
        const frame = {
            'channel': 'error',
            'data': 'Error parsing JSON into valid websocket request: {"method":"unsubscribe","subscription":{"type":"bbo","coin":"ETH"}}',
        };
        exchange.handleMessage (client, frame);
        check ('error/bbo unsubscribe: rejects "unsubscribe:bbo:ETH"', client.rejectCalls.some ((c) => c.hash === 'unsubscribe:bbo:ETH'));
        check ('error/bbo unsubscribe: does NOT reject the shared "bidsasks" hash', !client.rejectCalls.some ((c) => c.hash === 'bidsasks'));
        check ('error/bbo unsubscribe: subscriptions["unsubscribe:bbo:ETH"] deleted', !('unsubscribe:bbo:ETH' in client.subscriptions));
    }

    // unattributable error frame (no embedded JSON request) — must log, not
    // blanket-reject every in-flight future
    {
        const client = new MockClient ();
        const frame = { 'channel': 'error', 'data': 'some opaque hyperliquid error text with no embedded request' };
        exchange.handleMessage (client, frame);
        check ('error/unattributable: nothing rejected', client.rejectCalls.length === 0);
    }
}

async function main () {
    testBidAsk ();
    testAllMids ();
    await testAllDexsAssetCtxs ();
    testErrorFrame ();
    if (failures > 0) {
        console.error (failures.toString () + ' check(s) failed');
        process.exit (1);
    }
    console.log ('All hyperliquid ws handler fixture tests passed (CORE-647 Task 4, spec §6.1)');
}

main ();
