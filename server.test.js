'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { once } = require('node:events');
const WebSocket = require('ws');
const {
    aggregatePrice,
    candleBucket,
    createServer,
    flushPriceUpdates,
    getCachedHistory,
    getOandaStatus,
    isOandaStreamStale,
    isAllowedOrigin,
    mapGranularity,
    mapSymbol,
    nextCandleBoundary,
    nextReconnectDelay,
    publishPrice,
    searchSymbols,
    toChartCandle,
    toFcsCandle
} = require('./server');

test('maps chart symbols and the requested OANDA timeframes', () => {
    assert.equal(mapSymbol('FX:XAUUSD'), 'XAU_USD');
    assert.equal(mapSymbol('XAU_USD'), 'XAU_USD');
    assert.equal(mapSymbol('FX:EURUSD'), 'EUR_USD');
    assert.equal(mapSymbol('COMMODITY:BCOUSD'), 'BCO_USD');
    assert.equal(mapSymbol('INDICES:SPX500USD'), 'SPX500_USD');
    assert.equal(mapGranularity('1m'), 'M1');
    assert.equal(mapGranularity('5'), 'M5');
    assert.equal(mapGranularity('60'), 'H1');
    assert.equal(mapGranularity('1D'), 'D');
    assert.equal(mapGranularity('1M'), 'M');
});

test('searches listed OANDA instruments by category, ticker, and name', () => {
    const xauByTicker = searchSymbols('xau', 'global', 1, 20);
    const xauByName = searchSymbols('gold', 'commodity', 1, 20);
    assert.equal(xauByTicker[0].ticker, 'FX:XAUUSD');
    assert.equal(xauByTicker[0].profile.symbol, 'XAUUSD');
    assert.equal(xauByTicker[0].profile.exchange, 'FX');
    assert.equal(xauByName[0].profile.name, 'Gold / US Dollar');
    assert.equal(searchSymbols('btc,eth,eur,gbp,sol,jpy,spy,vug', 'global', 1, 20).length, 14);
    assert.equal(searchSymbols('eur,gbp,jpy,aud,cad,chf', 'forex', 1, 20).length, 7);
    assert.equal(searchSymbols('', 'indices', 1, 20).length, 4);
    assert.equal(searchSymbols('', 'forex', 1, 20).length, 7);
    assert.equal(searchSymbols('xau', 'global', 1, 20, null)[0].ticker, 'FX:XAUUSD');
    assert.equal(searchSymbols('', 'fav', 1, 20).length, 0);
    assert.equal(searchSymbols('', 'fav', 1, 20, 'FX:XAUUSD')[0].ticker, 'FX:XAUUSD');
    assert.equal(searchSymbols('', 'fav', 1, 20, 'FX:UNKNOWN').length, 0);
});

test('converts only complete OANDA midpoint candles', () => {
    const candle = {
        complete: true,
        time: '2026-10-07T12:34:00.000000000Z',
        mid: { o: '3900.1', h: '3901.2', l: '3899.8', c: '3900.7' },
        volume: 42
    };
    assert.deepEqual(toFcsCandle(candle), {
        timestamp: Date.parse(candle.time) / 1000,
        tm: Date.parse(candle.time) / 1000,
        t: Date.parse(candle.time) / 1000,
        open: 3900.1,
        high: 3901.2,
        low: 3899.8,
        close: 3900.7,
        volume: 42,
        o: 3900.1,
        h: 3901.2,
        l: 3899.8,
        c: 3900.7,
        v: 42
    });
    assert.equal(toFcsCandle({ ...candle, complete: false }), null);
    const incomplete = toFcsCandle({ ...candle, complete: false }, true);
    assert.equal(incomplete.timestamp, Date.parse(candle.time) / 1000);
    assert.equal(incomplete.open, 3900.1);
});

test('formats candles as chart-compatible positional rows', () => {
    const candle = {
        timestamp: 1791376440,
        open: 3900.1,
        high: 3901.2,
        low: 3899.8,
        close: 3900.7,
        volume: 42
    };
    assert.deepEqual(toChartCandle(candle), [1791376440, 3900.1, 3901.2, 3899.8, 3900.7, 42]);
});

test('aggregates streamed prices into timeframe candles', () => {
    const first = aggregatePrice(null, 120, 10, 'M1');
    const second = aggregatePrice(first, 150, 12, 'M1');
    const next = aggregatePrice(second, 180, 11, 'M1');
    assert.deepEqual(second, { t: 120, o: 10, h: 12, l: 10, c: 12, v: 0 });
    assert.deepEqual(next, { t: 180, o: 11, h: 11, l: 11, c: 11, v: 0 });
});

test('updates a seeded open candle without changing its OANDA open timestamp', () => {
    const openTime = Date.parse('2026-10-08T06:00:00Z') / 1000;
    const open = { t: openTime, o: 4000, h: 4010, l: 3990, c: 4000, v: 12 };
    const raised = aggregatePrice(open, Date.parse('2026-10-08T06:32:10Z') / 1000, 4020, 'H1');
    const lowered = aggregatePrice(raised, Date.parse('2026-10-08T06:45:10Z') / 1000, 3980, 'H1');
    const next = aggregatePrice(lowered, Date.parse('2026-10-08T07:00:00Z') / 1000, 3995, 'H1');
    assert.deepEqual(raised, { t: openTime, o: 4000, h: 4020, l: 3990, c: 4020, v: 12 });
    assert.deepEqual(lowered, { t: openTime, o: 4000, h: 4020, l: 3980, c: 3980, v: 12 });
    assert.deepEqual(next, { t: openTime + 3600, o: 3995, h: 3995, l: 3995, c: 3995, v: 0 });
});

test('preserves seeded candle ranges and starts new candles for every timeframe', () => {
    const granularitySeconds = {
        M1: 60,
        M5: 300,
        M15: 900,
        M30: 1800,
        H1: 3600,
        H4: 14400,
        D: 86400,
        W: 604800
    };
    const start = Date.parse('2026-10-08T06:32:10Z') / 1000;

    for (const granularity of [...Object.keys(granularitySeconds), 'M']) {
        const bucket = candleBucket(start, granularity);
        const seed = { t: bucket, o: 100, h: 120, l: 80, c: 105, v: 7 };
        const samePeriodTime = bucket + 10;
        const raised = aggregatePrice(seed, samePeriodTime, 130, granularity);
        const lowered = aggregatePrice(raised, samePeriodTime + 1, 75, granularity);
        assert.deepEqual(lowered, { ...seed, h: 130, l: 75, c: 75 }, granularity);

        const nextTime = nextCandleBoundary(bucket, granularity);
        const next = aggregatePrice(lowered, nextTime, 90, granularity);
        assert.deepEqual(next, { t: candleBucket(nextTime, granularity), o: 90, h: 90, l: 90, c: 90, v: 0 }, granularity);
    }
});

test('batches live prices into one latest update per flush interval', () => {
    const frames = [];
    const socket = {
        readyState: 1,
        send(frame) {
            frames.push(JSON.parse(frame));
        }
    };
    const subscription = {
        client: null,
        symbol: 'FX:XAUUSD',
        instrument: 'XAU_USD',
        timeframe: '1',
        granularity: 'M1',
        candle: {
            t: Date.parse('2026-10-07T12:34:00Z') / 1000,
            o: 3900,
            h: 3905,
            l: 3890,
            c: 3900,
            v: 42
        },
        dirty: false,
        seeded: true
    };
    const client = { socket, subscriptions: new Map([['FX:XAUUSD|1', subscription]]) };
    const hub = { clients: new Set([client]), lastPriceTimes: new Map() };
    subscription.client = client;

    for (const [time, price] of [
        ['2026-10-07T12:34:01Z', 4000],
        ['2026-10-07T12:34:02Z', 4003],
        ['2026-10-07T12:34:03Z', 3999]
    ]) {
        publishPrice(hub, {
            type: 'PRICE',
            instrument: 'XAU_USD',
            time,
            bids: [{ price: String(price - 1) }],
            asks: [{ price: String(price + 1) }]
        });
    }

    assert.equal(frames.length, 0);
    flushPriceUpdates(hub);
    assert.equal(frames.length, 1);
    assert.deepEqual(frames[0].prices, {
        mode: 'initial',
        t: Date.parse('2026-10-07T12:34:00Z') / 1000,
        o: 3900,
        h: 4003,
        l: 3890,
        c: 3999,
        v: 42,
        timestamp: Date.parse('2026-10-07T12:34:00Z') / 1000,
        tm: Date.parse('2026-10-07T12:34:00Z') / 1000,
        open: 3900,
        high: 4003,
        low: 3890,
        close: 3999,
        volume: 42
    });
    flushPriceUpdates(hub);
    assert.equal(frames.length, 1);
});

test('coalesces cached history loads and removes expired or failed entries', async () => {
    const cache = new Map();
    let resolveLoad;
    let calls = 0;
    const load = () => {
        calls += 1;
        return new Promise((resolve) => {
            resolveLoad = resolve;
        });
    };
    const first = getCachedHistory(cache, 'XAU_USD|M1', load);
    const second = getCachedHistory(cache, 'XAU_USD|M1', load);
    assert.equal(first, second);
    assert.equal(calls, 0);
    await Promise.resolve();
    assert.equal(calls, 1);
    resolveLoad(['candle']);
    assert.deepEqual(await first, ['candle']);
    cache.get('XAU_USD|M1').expiresAt = Date.now() - 1;
    assert.deepEqual(await getCachedHistory(cache, 'XAU_USD|M1', async () => ['new candle']), ['new candle']);
    await assert.rejects(getCachedHistory(cache, 'failed', async () => {
        throw new Error('upstream failure');
    }), /upstream failure/);
    assert.equal(cache.has('failed'), false);
});

test('returns and caches complete history without changing the response contract', async () => {
    const originalFetch = global.fetch;
    const previousToken = process.env.OANDA_ACCESS_TOKEN;
    const previousAccountId = process.env.OANDA_ACCOUNT_ID;
    let candleRequests = 0;
    process.env.OANDA_ACCESS_TOKEN = 'test-token';
    process.env.OANDA_ACCOUNT_ID = 'test-account';
    global.fetch = async (url, options) => {
        if (String(url).startsWith('http://127.0.0.1:')) return originalFetch(url, options);
        candleRequests += 1;
        return new Response(JSON.stringify({ candles: [
            {
                complete: true,
                time: '2026-10-08T06:00:00.000000000Z',
                mid: { o: '4000', h: '4010', l: '3990', c: '4005' },
                volume: 12
            },
            {
                complete: false,
                time: '2026-10-08T07:00:00.000000000Z',
                mid: { o: '4005', h: '4020', l: '4000', c: '4015' },
                volume: 3
            }
        ] }), { status: 200 });
    };

    const server = createServer();
    try {
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        for (let request = 0; request < 2; request += 1) {
            const response = await originalFetch(
                `http://127.0.0.1:${address.port}/api/candles?symbol=FX%3AXAUUSD&period=1h&length=10`
            );
            const body = await response.json();
            assert.equal(Object.hasOwn(body, 'requestId'), false);
            assert.deepEqual(body.response, [[Date.parse('2026-10-08T06:00:00Z') / 1000, 4000, 4010, 3990, 4005, 12]]);
        }
        assert.equal(candleRequests, 1);
    } finally {
        await new Promise((resolve) => server.close(resolve));
        global.fetch = originalFetch;
        if (previousToken === undefined) delete process.env.OANDA_ACCESS_TOKEN;
        else process.env.OANDA_ACCESS_TOKEN = previousToken;
        if (previousAccountId === undefined) delete process.env.OANDA_ACCOUNT_ID;
        else process.env.OANDA_ACCOUNT_ID = previousAccountId;
    }
});

test('detects OANDA streams that have gone quiet for 15 seconds', () => {
    assert.equal(isOandaStreamStale(1000, 15999), false);
    assert.equal(isOandaStreamStale(1000, 16000), true);
    assert.equal(getOandaStatus(1000, true, 7999), 'green');
    assert.equal(getOandaStatus(1000, true, 8000), 'yellow');
    assert.equal(getOandaStatus(1000, true, 16000), 'red');
    assert.equal(getOandaStatus(1000, false, 2000), 'yellow');
    assert.equal(getOandaStatus(null, false, 2000, 1000), 'yellow');
    assert.equal(getOandaStatus(null, false, 16000, 1000), 'red');
});

test('backs off OANDA stream reconnects at 1s, 2s, 4s, then 10s', () => {
    assert.deepEqual([1000, 2000, 4000, 10000, 10000].map(nextReconnectDelay), [
        2000, 4000, 10000, 10000, 10000
    ]);
});

test('restarts an OANDA stream after its heartbeat watchdog expires', async () => {
    const originalFetch = global.fetch;
    const originalNow = Date.now;
    const previousToken = process.env.OANDA_ACCESS_TOKEN;
    const previousAccountId = process.env.OANDA_ACCOUNT_ID;
    const streamRequests = [];
    const seededFrames = [];
    let seedRequestCount = 0;
    const nowStarted = performance.now();
    const nowBase = originalNow();
    process.env.OANDA_ACCESS_TOKEN = 'test-token';
    process.env.OANDA_ACCOUNT_ID = 'test-account';
    Date.now = () => nowBase + (performance.now() - nowStarted) * 20;
    global.fetch = async (url, options) => {
        if (!String(url).includes('/pricing/stream')) {
            seedRequestCount += 1;
            return new Response(JSON.stringify({ candles: [
                {
                    complete: false,
                    time: '2026-10-08T06:00:00.000000000Z',
                    mid: {
                        o: '4000',
                        h: String(4010 + seedRequestCount),
                        l: String(3990 - seedRequestCount),
                        c: '4005'
                    },
                    volume: 12
                }
            ] }), { status: 200 });
        }
        streamRequests.push(new URL(url));
        const body = new ReadableStream({
            start(controller) {
                controller.enqueue(new TextEncoder().encode('{"type":"HEARTBEAT"}\n'));
                options.signal.addEventListener('abort', () => {
                    try {
                        controller.close();
                    } catch {
                        return;
                    }
                }, { once: true });
            }
        });
        return new Response(body, { status: 200 });
    };

    const server = createServer();
    let socket;
    try {
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
        await once(socket, 'open');
        socket.on('message', (frame) => {
            const message = JSON.parse(frame.toString());
            if (message.type === 'price') seededFrames.push(message.prices);
        });
        const errorReceived = new Promise((resolve) => {
            socket.on('message', (frame) => {
                const message = JSON.parse(frame.toString());
                if (message.type === 'error') resolve(message.message);
            });
        });
        socket.send(JSON.stringify({ type: 'join_symbol', symbol: 'FX:XAUUSD', timeframe: '1h' }));
        assert.match(await errorReceived, /no heartbeat or price data for 15 seconds/);
        await new Promise((resolve) => setTimeout(resolve, 1100));
        assert.ok(streamRequests.length >= 2);
        assert.ok(seedRequestCount >= 2);
        assert.deepEqual(seededFrames.slice(0, 2).map(({ o, h, l, c }) => [o, h, l, c]), [
            [4000, 4011, 3989, 4005],
            [4000, 4012, 3988, 4005]
        ]);
    } finally {
        Date.now = originalNow;
        if (socket?.readyState === WebSocket.OPEN) {
            const closed = once(socket, 'close');
            socket.close();
            await closed;
        }
        await new Promise((resolve) => server.close(resolve));
        global.fetch = originalFetch;
        if (previousToken === undefined) delete process.env.OANDA_ACCESS_TOKEN;
        else process.env.OANDA_ACCESS_TOKEN = previousToken;
        if (previousAccountId === undefined) delete process.env.OANDA_ACCOUNT_ID;
        else process.env.OANDA_ACCOUNT_ID = previousAccountId;
    }
});

test('timeframe switches replace the old subscription and seed the new one without request IDs', async () => {
    const originalFetch = global.fetch;
    const previousToken = process.env.OANDA_ACCESS_TOKEN;
    const previousAccountId = process.env.OANDA_ACCOUNT_ID;
    const candleRequests = [];
    const messages = [];
    let pricingStreamController;
    process.env.OANDA_ACCESS_TOKEN = 'test-token';
    process.env.OANDA_ACCOUNT_ID = 'test-account';
    global.fetch = async (url, options) => {
        const parsedUrl = new URL(url);
        if (parsedUrl.pathname.includes('/pricing/stream')) {
            return new Response(new ReadableStream({
                start(controller) {
                    pricingStreamController = controller;
                    controller.enqueue(new TextEncoder().encode('{"type":"HEARTBEAT"}\n'));
                    options.signal.addEventListener('abort', () => {
                        try {
                            controller.close();
                        } catch {
                            return;
                        }
                    }, { once: true });
                }
            }), { status: 200 });
        }

        const granularity = parsedUrl.searchParams.get('granularity');
        candleRequests.push(granularity);
        const bucket = candleBucket(Math.floor(Date.now() / 1000), granularity);
        return new Response(JSON.stringify({ candles: [{
            complete: false,
            time: new Date(bucket * 1000).toISOString(),
            mid: { o: '4000', h: '4010', l: '3990', c: '4005' },
            volume: 9
        }] }), { status: 200 });
    };

    const server = createServer();
    let socket;
    try {
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
        socket.on('message', (frame) => messages.push(JSON.parse(frame.toString())));
        await once(socket, 'open');

        const waitForPrice = async (timeframe, close = 4005) => {
            for (let attempt = 0; attempt < 100 && !messages.some((message) =>
                message.type === 'price'
                && message.timeframe === timeframe
                && message.prices?.c === close); attempt += 1) {
                await new Promise((resolve) => setTimeout(resolve, 10));
            }
        };
        socket.send(JSON.stringify({ type: 'join_symbol', symbol: 'FX:XAUUSD', timeframe: '1' }));
        await waitForPrice('1');
        assert.ok(pricingStreamController);

        socket.send(JSON.stringify({ type: 'join_symbol', symbol: 'FX:XAUUSD', timeframe: '60' }));
        socket.send(JSON.stringify({ type: 'leave_symbol', symbol: 'FX:XAUUSD', timeframe: '1' }));
        await waitForPrice('60');
        assert.ok(candleRequests.includes('M1'));
        assert.ok(candleRequests.includes('H1'));

        const tickTime = new Date().toISOString();
        pricingStreamController.enqueue(new TextEncoder().encode(`${JSON.stringify({
            type: 'PRICE',
            instrument: 'XAU_USD',
            time: tickTime,
            bids: [{ price: '4024' }],
            asks: [{ price: '4026' }]
        })}\n`));
        await waitForPrice('60', 4025);
        const latestFrames = messages.filter((message) => message.type === 'price').slice(-2);
        assert.deepEqual(latestFrames.map((message) => message.timeframe), ['60', '60']);
        assert.ok(latestFrames.every((message) => !Object.hasOwn(message, 'requestId')));
        assert.deepEqual(
            [latestFrames.at(-1).prices.o, latestFrames.at(-1).prices.h, latestFrames.at(-1).prices.l, latestFrames.at(-1).prices.c],
            [4000, 4025, 3990, 4025]
        );
    } finally {
        if (socket?.readyState === WebSocket.OPEN) {
            const closed = once(socket, 'close');
            socket.close();
            await closed;
        }
        await new Promise((resolve) => server.close(resolve));
        global.fetch = originalFetch;
        if (previousToken === undefined) delete process.env.OANDA_ACCESS_TOKEN;
        else process.env.OANDA_ACCESS_TOKEN = previousToken;
        if (previousAccountId === undefined) delete process.env.OANDA_ACCOUNT_ID;
        else process.env.OANDA_ACCOUNT_ID = previousAccountId;
    }
});

test('shares one OANDA pricing stream across clients with the same instrument', async () => {
    const originalFetch = global.fetch;
    const previousToken = process.env.OANDA_ACCESS_TOKEN;
    const previousAccountId = process.env.OANDA_ACCOUNT_ID;
    const streamRequests = [];
    const candleRequests = [];
    const seededFrames = [[], []];
    process.env.OANDA_ACCESS_TOKEN = 'test-token';
    process.env.OANDA_ACCOUNT_ID = 'test-account';
    global.fetch = async (url, options) => {
        if (!String(url).includes('/pricing/stream')) {
            const granularity = new URL(url).searchParams.get('granularity');
            candleRequests.push(granularity);
            return new Response(JSON.stringify({ candles: [
                {
                    complete: false,
                    time: '2026-10-08T06:00:00.000000000Z',
                    mid: { o: '4000', h: '4020', l: '3980', c: '4010' },
                    volume: 5
                }
            ] }), { status: 200 });
        }
        streamRequests.push(new URL(url));
        const body = new ReadableStream({
            start(controller) {
                controller.enqueue(new TextEncoder().encode('{"type":"HEARTBEAT"}\n'));
                options.signal.addEventListener('abort', () => controller.close(), { once: true });
            }
        });
        return new Response(body, { status: 200 });
    };

    const server = createServer();
    const sockets = [];
    try {
        await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
        const address = server.address();
        for (let index = 0; index < 2; index += 1) {
            const socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
            sockets.push(socket);
            socket.on('message', (frame) => {
                const message = JSON.parse(frame.toString());
                if (message.type === 'price') seededFrames[index].push(message);
            });
            await once(socket, 'open');
            socket.send(JSON.stringify({
                type: 'join_symbol',
                symbol: 'FX:XAUUSD',
                timeframe: index === 0 ? '1m' : '15m'
            }));
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
        assert.equal(streamRequests.length, 1);
        assert.equal(streamRequests[0].searchParams.get('instruments'), 'XAU_USD');
        assert.ok(candleRequests.includes('M1'));
        assert.ok(candleRequests.includes('M15'));
        for (const [index, timeframe] of ['1m', '15m'].entries()) {
            const seeded = seededFrames[index].find((message) => message.timeframe === timeframe)?.prices;
            assert.deepEqual(seeded && [seeded.o, seeded.h, seeded.l, seeded.c], [4000, 4020, 3980, 4010]);
        }
        const ping = new Promise((resolve) => {
            sockets[0].on('message', (frame) => {
                const message = JSON.parse(frame.toString());
                if (message.type === 'ping') resolve(message);
            });
        });
        assert.equal((await ping).oandaStatus, 'green');
    } finally {
        for (const socket of sockets) {
            if (socket.readyState === WebSocket.OPEN) socket.close();
        }
        await Promise.all(sockets.map((socket) => once(socket, 'close')));
        await new Promise((resolve) => server.close(resolve));
        global.fetch = originalFetch;
        if (previousToken === undefined) delete process.env.OANDA_ACCESS_TOKEN;
        else process.env.OANDA_ACCESS_TOKEN = previousToken;
        if (previousAccountId === undefined) delete process.env.OANDA_ACCOUNT_ID;
        else process.env.OANDA_ACCOUNT_ID = previousAccountId;
    }
});

test('aligns OANDA daily, weekly, and monthly candles to New York session boundaries', () => {
    const utc = (value) => Date.parse(value) / 1000;
    assert.equal(candleBucket(utc('2026-10-07T21:45:00Z'), 'D'), utc('2026-10-07T21:00:00Z'));
    assert.equal(nextCandleBoundary(utc('2026-10-31T21:00:00Z'), 'D'), utc('2026-11-01T22:00:00Z'));
    assert.equal(candleBucket(utc('2026-10-08T20:00:00Z'), 'W'), utc('2026-10-02T21:00:00Z'));
    assert.equal(nextCandleBoundary(utc('2026-10-30T21:00:00Z'), 'W'), utc('2026-11-06T22:00:00Z'));
    assert.equal(candleBucket(utc('2026-10-01T20:00:00Z'), 'M'), utc('2026-09-01T21:00:00Z'));
    assert.equal(nextCandleBoundary(utc('2026-10-01T21:00:00Z'), 'M'), utc('2026-11-01T22:00:00Z'));
});

test('rolls live candles over once at OANDA open times for M1, M5, H1, and D', async () => {
    const originalFetch = global.fetch;
    const previousToken = process.env.OANDA_ACCESS_TOKEN;
    const previousAccountId = process.env.OANDA_ACCOUNT_ID;
    const cases = [
        { granularity: 'M1', timeframe: '1m', open: '2026-10-08T06:00:00Z' },
        { granularity: 'M5', timeframe: '5m', open: '2026-10-08T06:00:00Z' },
        { granularity: 'H1', timeframe: '1h', open: '2026-10-08T06:00:00Z' },
        { granularity: 'D', timeframe: '1D', open: '2026-10-08T21:00:00Z' }
    ];
    process.env.OANDA_ACCESS_TOKEN = 'test-token';
    process.env.OANDA_ACCOUNT_ID = 'test-account';

    try {
        for (const { granularity, timeframe, open } of cases) {
            const seedTime = Date.parse(open) / 1000;
            const boundary = nextCandleBoundary(seedTime, granularity);
            let streamController;
            let candleRequestCount = 0;
            const messages = [];
            global.fetch = async (url, options) => {
                const parsedUrl = new URL(url);
                if (parsedUrl.pathname.includes('/pricing/stream')) {
                    return new Response(new ReadableStream({
                        start(controller) {
                            streamController = controller;
                            controller.enqueue(new TextEncoder().encode('{"type":"HEARTBEAT"}\n'));
                            options.signal.addEventListener('abort', () => {
                                try {
                                    controller.close();
                                } catch {
                                    return;
                                }
                            }, { once: true });
                        }
                    }), { status: 200 });
                }

                assert.equal(parsedUrl.searchParams.get('granularity'), granularity);
                assert.equal(parsedUrl.searchParams.get('dailyAlignment'), '17');
                assert.equal(parsedUrl.searchParams.get('alignmentTimezone'), 'America/New_York');
                assert.equal(parsedUrl.searchParams.get('weeklyAlignment'), 'Friday');
                candleRequestCount += 1;
                return new Response(JSON.stringify({ candles: [{
                    complete: false,
                    time: new Date(seedTime * 1000).toISOString(),
                    mid: { o: '100', h: '110', l: '90', c: '105' },
                    volume: 7
                }] }), { status: 200 });
            };

            const server = createServer();
            let socket;
            try {
                await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
                const address = server.address();
                socket = new WebSocket(`ws://127.0.0.1:${address.port}/ws`);
                socket.on('message', (frame) => messages.push(JSON.parse(frame.toString())));
                await once(socket, 'open');
                socket.send(JSON.stringify({
                    type: 'join_symbol',
                    symbol: 'FX:XAUUSD',
                    timeframe
                }));
                for (let attempt = 0; attempt < 100 && !messages.some((message) =>
                    message.type === 'price' && message.prices?.t === seedTime); attempt += 1) {
                    await new Promise((resolve) => setTimeout(resolve, 10));
                }
                assert.ok(streamController, `${granularity} stream did not start`);
                assert.ok(messages.some((message) =>
                    message.type === 'price' && message.prices?.t === seedTime), `${granularity} seed missing`);

                const sendTick = (timestamp, price) => streamController.enqueue(new TextEncoder().encode(
                    `${JSON.stringify({
                        type: 'PRICE',
                        instrument: 'XAU_USD',
                        time: new Date(timestamp * 1000).toISOString(),
                        bids: [{ price: String(price - 0.5) }],
                        asks: [{ price: String(price + 0.5) }]
                    })}\n`
                ));
                sendTick(boundary - 2, 120);
                sendTick(boundary - 1, 80);
                for (let attempt = 0; attempt < 100 && !messages.some((message) =>
                    message.type === 'price'
                    && message.prices?.t === seedTime
                    && message.prices?.h === 120
                    && message.prices?.c === 80); attempt += 1) {
                    await new Promise((resolve) => setTimeout(resolve, 10));
                }
                sendTick(boundary + 1, 105);
                for (let attempt = 0; attempt < 100 && !messages.some((message) =>
                    message.type === 'price'
                    && message.prices?.t === boundary
                    && message.prices?.c === 105); attempt += 1) {
                    await new Promise((resolve) => setTimeout(resolve, 10));
                }
                sendTick(boundary + 2, 111);
                for (let attempt = 0; attempt < 100 && !messages.some((message) =>
                    message.type === 'price'
                    && message.prices?.t === boundary
                    && message.prices?.c === 111); attempt += 1) {
                    await new Promise((resolve) => setTimeout(resolve, 10));
                }
                const oldFrames = messages.filter((message) =>
                    message.type === 'price' && message.prices?.t === seedTime);
                const newFrames = messages.filter((message) =>
                    message.type === 'price' && message.prices?.t === boundary);
                assert.deepEqual(
                    [oldFrames.at(-1).prices.o, oldFrames.at(-1).prices.h, oldFrames.at(-1).prices.l, oldFrames.at(-1).prices.c],
                    [100, 120, 80, 80],
                    `${granularity} closed candle OHLC`
                );
                assert.deepEqual(
                    [newFrames[0].prices.o, newFrames[0].prices.h, newFrames[0].prices.l, newFrames[0].prices.c],
                    [105, 105, 105, 105],
                    `${granularity} first tick creates the new bar`
                );
                assert.deepEqual(
                    [newFrames.at(-1).prices.o, newFrames.at(-1).prices.h, newFrames.at(-1).prices.l, newFrames.at(-1).prices.c],
                    [105, 111, 105, 111],
                    `${granularity} subsequent tick updates the new bar`
                );
                assert.equal(new Set(newFrames.map((message) => message.prices.t)).size, 1);
                assert.equal(candleRequestCount, 1, `${granularity} unexpectedly refetched during rollover`);
                assert.ok(newFrames.every((message) => !Object.hasOwn(message, 'requestId')));
            } finally {
                if (socket?.readyState === WebSocket.OPEN) {
                    const closed = once(socket, 'close');
                    socket.close();
                    await closed;
                }
                await new Promise((resolve) => server.close(resolve));
            }
        }
    } finally {
        global.fetch = originalFetch;
        if (previousToken === undefined) delete process.env.OANDA_ACCESS_TOKEN;
        else process.env.OANDA_ACCESS_TOKEN = previousToken;
        if (previousAccountId === undefined) delete process.env.OANDA_ACCOUNT_ID;
        else process.env.OANDA_ACCOUNT_ID = previousAccountId;
    }
});

test('allows only localhost or same-origin Codespaces WebSocket upgrades', () => {
    assert.equal(isAllowedOrigin('http://127.0.0.1:3000', '127.0.0.1:3000'), true);
    assert.equal(isAllowedOrigin('https://workspace-3000.app.github.dev', 'workspace-3000.app.github.dev'), true);
    assert.equal(isAllowedOrigin('https://attacker.example', '127.0.0.1:3000'), false);
});