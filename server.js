'use strict';

const fs = require('node:fs/promises');
const http = require('node:http');
const path = require('node:path');
const { URL } = require('node:url');
const { WebSocketServer, WebSocket } = require('ws');

const OANDA_API = 'https://api-fxpractice.oanda.com/v3';
const OANDA_STREAM = 'https://stream-fxpractice.oanda.com/v3';
const HOST = '127.0.0.1';
const PORT = Number(process.env.PORT || 3000);
const MAX_HISTORY = 10000;
const PAGE_SIZE = 5000;
const HISTORY_CACHE_TTL_MS = 10000;
const PRICE_BATCH_INTERVAL_MS = 250;
const SERVER_PING_INTERVAL_MS = 1000;
const OANDA_ALIVE_INTERVAL_MS = 7000;
const STREAM_HEARTBEAT_TIMEOUT_MS = 15000;
const SYMBOLS = [
    { instrument: 'XAU_USD', ticker: 'FX:XAUUSD', name: 'Gold / US Dollar', type: 'commodity', subType: 'Metals', aliases: ['gold'] },
    { instrument: 'XAG_USD', ticker: 'FX:XAGUSD', name: 'Silver / US Dollar', type: 'commodity', subType: 'Metals', aliases: ['silver'] },
    { instrument: 'EUR_USD', ticker: 'FX:EURUSD', name: 'Euro / US Dollar', type: 'forex', subType: 'Currency' },
    { instrument: 'GBP_USD', ticker: 'FX:GBPUSD', name: 'British Pound / US Dollar', type: 'forex', subType: 'Currency' },
    { instrument: 'USD_JPY', ticker: 'FX:USDJPY', name: 'US Dollar / Japanese Yen', type: 'forex', subType: 'Currency' },
    { instrument: 'AUD_USD', ticker: 'FX:AUDUSD', name: 'Australian Dollar / US Dollar', type: 'forex', subType: 'Currency' },
    { instrument: 'USD_CAD', ticker: 'FX:USDCAD', name: 'US Dollar / Canadian Dollar', type: 'forex', subType: 'Currency' },
    { instrument: 'USD_CHF', ticker: 'FX:USDCHF', name: 'US Dollar / Swiss Franc', type: 'forex', subType: 'Currency' },
    { instrument: 'NZD_USD', ticker: 'FX:NZDUSD', name: 'New Zealand Dollar / US Dollar', type: 'forex', subType: 'Currency' },
    { instrument: 'BCO_USD', ticker: 'COMMODITY:BCOUSD', name: 'Brent Crude Oil / US Dollar', type: 'commodity', subType: 'Energy' },
    { instrument: 'SPX500_USD', ticker: 'INDICES:SPX500USD', name: 'US SPX 500', type: 'indices', subType: 'Index' },
    { instrument: 'NAS100_USD', ticker: 'INDICES:NAS100USD', name: 'US Nas 100', type: 'indices', subType: 'Index' },
    { instrument: 'US30_USD', ticker: 'INDICES:US30USD', name: 'US Wall St 30', type: 'indices', subType: 'Index' },
    { instrument: 'DE30_EUR', ticker: 'INDICES:DE30EUR', name: 'Germany 30 / Euro', type: 'indices', subType: 'Index' }
];
const SYMBOLS_BY_INSTRUMENT = new Map(SYMBOLS.map((symbol) => [symbol.instrument, symbol]));
const SYMBOLS_BY_TICKER = new Map(SYMBOLS.map((symbol) => [symbol.ticker, symbol]));

const GRANULARITIES = new Map([
    ['1', 'M1'], ['1MIN', 'M1'], ['M1', 'M1'],
    ['5', 'M5'], ['5M', 'M5'], ['5MIN', 'M5'], ['M5', 'M5'],
    ['15', 'M15'], ['15M', 'M15'], ['15MIN', 'M15'], ['M15', 'M15'],
    ['30', 'M30'], ['30M', 'M30'], ['30MIN', 'M30'], ['M30', 'M30'],
    ['60', 'H1'], ['1H', 'H1'], ['H1', 'H1'],
    ['240', 'H4'], ['4H', 'H4'], ['H4', 'H4'],
    ['D', 'D'], ['1D', 'D'], ['D1', 'D'],
    ['W', 'W'], ['1W', 'W'], ['W1', 'W'],
    ['1M', 'M'], ['1MO', 'M'], ['1MONTH', 'M'], ['MO', 'M'], ['MONTH', 'M'], ['M', 'M']
]);

const SECONDS_PER_GRANULARITY = {
    M1: 60,
    M5: 5 * 60,
    M15: 15 * 60,
    M30: 30 * 60,
    H1: 60 * 60,
    H4: 4 * 60 * 60,
    D: 24 * 60 * 60,
    W: 7 * 24 * 60 * 60,
    M: 30 * 24 * 60 * 60
};

function mapSymbol(value) {
    const normalized = String(value || '').trim().toUpperCase().replaceAll('/', '_');
    const listedSymbol = SYMBOLS_BY_TICKER.get(normalized);
    if (listedSymbol) return listedSymbol.instrument;
    const instrument = normalized.replace(/^[A-Z]+:/, '').replaceAll('_', '');
    const match = SYMBOLS.find((symbol) => symbol.instrument.replaceAll('_', '') === instrument);
    if (match) return match.instrument;
    throw new Error(`Unsupported OANDA instrument: ${value}`);
}

function searchSymbols(search, type, page, perPage, symbols = '') {
    const rawQuery = String(search || '').trim().toLowerCase();
    const query = rawQuery.includes(',') ? '' : rawQuery;
    const category = String(type || '').toLowerCase();
    const selectedSymbols = new Set(String(symbols || '').split(',').map((symbol) => symbol.trim().toUpperCase()).filter(Boolean));
    if (category === 'fav' && selectedSymbols.size === 0) return [];
    const filtered = SYMBOLS.filter((symbol) => {
        if (category !== 'global' && category !== 'all' && category !== 'fav' && symbol.type !== category) return false;
        if (selectedSymbols.size > 0
            && !selectedSymbols.has(symbol.ticker)
            && !selectedSymbols.has(symbol.instrument)) return false;
        if (!query) return true;
        return [symbol.instrument, symbol.ticker, symbol.name, ...(symbol.aliases || [])]
            .some((value) => value.toLowerCase().includes(query));
    });
    return filtered.slice((page - 1) * perPage, page * perPage).map((symbol) => ({
        ticker: symbol.ticker,
        profile: {
            symbol: symbol.ticker.split(':')[1],
            name: symbol.name,
            exchange: symbol.ticker.split(':')[0],
            type: symbol.type,
            sub_type: symbol.subType,
            base_logo: '',
            curr_logo: '',
            exc_logo: ''
        },
        meta: { is_primary: 1 }
    }));
}

function mapGranularity(value) {
    const raw = String(value || '').trim();
    if (raw === '1m') return 'M1';
    const normalized = raw.toUpperCase();
    const granularity = GRANULARITIES.get(normalized);
    if (!granularity) throw new Error(`Unsupported chart timeframe: ${value}`);
    return granularity;
}

function toOandaTime(value) {
    const numeric = Number(value);
    const milliseconds = Number.isFinite(numeric) ? (numeric > 1e12 ? numeric : numeric * 1000) : Date.parse(value);
    if (!Number.isFinite(milliseconds)) throw new Error('Invalid candle time range.');
    return new Date(milliseconds).toISOString();
}

function toFcsCandle(candle, includeIncomplete = false) {
    if ((!candle.complete && !includeIncomplete) || !candle.mid) return null;
    const timestamp = Math.floor(Date.parse(candle.time) / 1000);
    const open = Number(candle.mid.o);
    const high = Number(candle.mid.h);
    const low = Number(candle.mid.l);
    const close = Number(candle.mid.c);
    if (![timestamp, open, high, low, close].every(Number.isFinite)) return null;
    const volume = Number(candle.volume) || 0;
    return {
        timestamp,
        tm: timestamp,
        t: timestamp,
        open,
        high,
        low,
        close,
        volume,
        o: open,
        h: high,
        l: low,
        c: close,
        v: volume
    };
}

function toChartCandle(candle) {
    return [candle.timestamp, candle.open, candle.high, candle.low, candle.close, candle.volume];
}

function candleBucket(timestamp, granularity) {
    if (granularity === 'D' || granularity === 'W' || granularity === 'M') {
        return easternSessionStart(timestamp, granularity);
    }
    const interval = SECONDS_PER_GRANULARITY[granularity];
    return Math.floor(timestamp / interval) * interval;
}

function easternParts(timestamp) {
    const parts = new Intl.DateTimeFormat('en-US', {
        timeZone: 'America/New_York',
        year: 'numeric',
        month: 'numeric',
        day: 'numeric',
        hour: 'numeric',
        hourCycle: 'h23'
    }).formatToParts(new Date(timestamp * 1000));
    return Object.fromEntries(parts.map(({ type, value }) => [type, Number(value)]));
}

function easternTimestamp(year, month, day, hour = 17) {
    const target = Date.UTC(year, month - 1, day, hour);
    const parts = easternParts(target / 1000);
    const localAsUtc = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour);
    return Math.floor((target - (localAsUtc - target)) / 1000);
}

function easternSessionStart(timestamp, granularity) {
    const { year, month, day, hour } = easternParts(timestamp);
    const date = new Date(Date.UTC(year, month - 1, day));
    if (granularity === 'D') {
        if (hour < 17) date.setUTCDate(date.getUTCDate() - 1);
    } else if (granularity === 'W') {
        const daysSinceFriday = (date.getUTCDay() - 5 + 7) % 7;
        date.setUTCDate(date.getUTCDate() - daysSinceFriday - (daysSinceFriday === 0 && hour < 17 ? 7 : 0));
    } else if (granularity === 'M') {
        if (day === 1 && hour < 17) date.setUTCMonth(date.getUTCMonth() - 1);
        date.setUTCDate(1);
    }
    return easternTimestamp(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
}

function nextCandleBoundary(openTime, granularity) {
    if (granularity !== 'D' && granularity !== 'W' && granularity !== 'M') {
        return openTime + SECONDS_PER_GRANULARITY[granularity];
    }
    const { year, month, day } = easternParts(openTime);
    const date = new Date(Date.UTC(year, month - 1, day));
    if (granularity === 'D') date.setUTCDate(date.getUTCDate() + 1);
    if (granularity === 'W') date.setUTCDate(date.getUTCDate() + 7);
    if (granularity === 'M') {
        date.setUTCMonth(date.getUTCMonth() + 1, 1);
    }
    return easternTimestamp(date.getUTCFullYear(), date.getUTCMonth() + 1, date.getUTCDate());
}

function aggregatePrice(previous, timestamp, price, granularity) {
    const bucket = candleBucket(timestamp, granularity);
    if (!previous || previous.t !== bucket) {
        return { t: bucket, o: price, h: price, l: price, c: price, v: 0 };
    }
    return {
        ...previous,
        h: Math.max(previous.h, price),
        l: Math.min(previous.l, price),
        c: price
    };
}

function getCredentials() {
    const token = process.env.OANDA_ACCESS_TOKEN || process.env.OANDA_API_KEY;
    const accountId = process.env.OANDA_ACCOUNT_ID;
    if (!token || !accountId) {
        throw new Error('Set OANDA_API_KEY and OANDA_ACCOUNT_ID in .env before starting the server.');
    }
    return { token, accountId };
}

async function requestOandaCandles(instrument, granularity, query, token) {
    const url = new URL(`${OANDA_API}/instruments/${encodeURIComponent(instrument)}/candles`);
    url.searchParams.set('granularity', granularity);
    url.searchParams.set('price', 'M');
    url.searchParams.set('dailyAlignment', '17');
    url.searchParams.set('alignmentTimezone', 'America/New_York');
    url.searchParams.set('weeklyAlignment', 'Friday');
    for (const [key, value] of Object.entries(query)) {
        if (value !== undefined && value !== null) url.searchParams.set(key, String(value));
    }

    const response = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` }
    });
    if (!response.ok) throw new Error(`OANDA candles request failed with HTTP ${response.status}.`);
    const body = await response.json();
    if (!Array.isArray(body.candles)) throw new Error('OANDA returned an invalid candles response.');
    return body.candles;
}

function estimateStart(granularity, count) {
    const lookbackSeconds = (SECONDS_PER_GRANULARITY[granularity] || 30 * 86400) * count * 2;
    return new Date(Date.now() - lookbackSeconds * 1000).toISOString();
}

async function fetchCompleteCandles(instrument, granularity, count, token, range = {}) {
    const { from, to } = range;
    if (!from && !to && count < PAGE_SIZE) {
        const page = await requestOandaCandles(instrument, granularity, { count: count + 1 }, token);
        return page.map((candle) => toFcsCandle(candle)).filter(Boolean).slice(-count);
    }

    let cursor = from ? toOandaTime(from) : estimateStart(granularity, count);
    const endTime = to ? Date.parse(toOandaTime(to)) : Date.now();
    const candles = [];

    for (let pageNumber = 0; pageNumber < 20; pageNumber += 1) {
        const raw = await requestOandaCandles(instrument, granularity, {
            count: PAGE_SIZE,
            from: cursor,
            to: to ? toOandaTime(to) : undefined,
            includeFirst: pageNumber === 0 ? undefined : false
        }, token);
        candles.push(...raw.map(toFcsCandle).filter(Boolean));
        if (!raw.length || raw.length < PAGE_SIZE) break;

        const lastTime = raw[raw.length - 1].time;
        if (Date.parse(lastTime) >= endTime) break;
        cursor = lastTime;
    }

    if (from) {
        const startSeconds = Math.floor(Date.parse(toOandaTime(from)) / 1000);
        const endSeconds = to ? Math.floor(Date.parse(toOandaTime(to)) / 1000) : Infinity;
        return candles.filter((candle) => candle.t >= startSeconds && candle.t <= endSeconds);
    }
    return candles.slice(-count);
}

function parsePositiveInteger(value, fallback, maximum) {
    const parsed = Number.parseInt(value, 10);
    if (!Number.isFinite(parsed) || parsed < 1) return fallback;
    return Math.min(parsed, maximum);
}

function getCachedHistory(cache, key, load) {
    const now = Date.now();
    for (const [cachedKey, entry] of cache) {
        if (entry.expiresAt <= now) cache.delete(cachedKey);
    }
    const cached = cache.get(key);
    if (cached && cached.expiresAt > now) return cached.promise;
    const promise = Promise.resolve().then(load).catch((error) => {
        if (cache.get(key)?.promise === promise) cache.delete(key);
        throw error;
    });
    cache.set(key, { expiresAt: now + HISTORY_CACHE_TTL_MS, promise });
    return promise;
}

async function handleCandles(requestUrl, response, historyCache) {
    try {
        const instrument = mapSymbol(requestUrl.searchParams.get('symbol'));
        const granularity = mapGranularity(requestUrl.searchParams.get('period'));
        const length = parsePositiveInteger(requestUrl.searchParams.get('length'), 600, MAX_HISTORY);
        const page = parsePositiveInteger(requestUrl.searchParams.get('page'), 1, MAX_HISTORY);
        const from = requestUrl.searchParams.get('from');
        const to = requestUrl.searchParams.get('to');
        const { token } = getCredentials();
        const requested = Math.min(length * page, MAX_HISTORY);
        const cacheKey = JSON.stringify([instrument, granularity, requested, from, to]);
        const candles = await getCachedHistory(historyCache, cacheKey, () =>
            fetchCompleteCandles(instrument, granularity, requested, token, { from, to }));
        const end = Math.max(0, candles.length - (page - 1) * length);
        const start = Math.max(0, end - length);
        const pageCandles = candles.slice(start, end);
        response.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
        response.end(JSON.stringify({
            status: true,
            response: pageCandles.map(toChartCandle),
            info: { ticker: instrument, period: granularity }
        }));
    } catch (error) {
        const status = error.message.startsWith('OANDA candles request failed') ? 502 : 400;
        response.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
        response.end(JSON.stringify({ status: false, msg: error.message }));
    }
}

function handleSymbolSearch(request, response) {
    const chunks = [];
    request.on('data', (chunk) => chunks.push(chunk));
    request.on('end', () => {
        try {
            const params = new URLSearchParams(Buffer.concat(chunks).toString());
            const page = parsePositiveInteger(params.get('page'), 1, 100);
            const perPage = parsePositiveInteger(params.get('per_page'), 20, 100);
            const rows = searchSymbols(params.get('search'), params.get('type'), page, perPage, params.get('symbol'));
            response.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
            response.end(JSON.stringify({ success: true, response: rows }));
        } catch (error) {
            response.writeHead(400, { 'Content-Type': 'application/json; charset=utf-8', 'Cache-Control': 'no-store' });
            response.end(JSON.stringify({ success: false, message: error.message }));
        }
    });
    request.on('error', (error) => {
        if (!response.headersSent) response.writeHead(400);
        response.end(JSON.stringify({ success: false, message: error.message }));
    });
}

function sendJson(socket, value) {
    if (socket.readyState === WebSocket.OPEN) socket.send(JSON.stringify(value));
}

function priceMidpoint(message) {
    const bid = Number(message.bids?.[0]?.price);
    const ask = Number(message.asks?.[0]?.price);
    if (Number.isFinite(bid) && Number.isFinite(ask)) return (bid + ask) / 2;
    const closeoutBid = Number(message.closeoutBid);
    const closeoutAsk = Number(message.closeoutAsk);
    if (Number.isFinite(closeoutBid) && Number.isFinite(closeoutAsk)) return (closeoutBid + closeoutAsk) / 2;
    return null;
}

function createPriceEnvelope(symbol, timeframe, candle) {
    return {
        type: 'price',
        symbol,
        timeframe,
        prices: {
            mode: 'initial',
            ...candle,
            timestamp: candle.t,
            tm: candle.t,
            open: candle.o,
            high: candle.h,
            low: candle.l,
            close: candle.c,
            volume: candle.v
        }
    };
}

function getActiveInstruments(hub) {
    return [...new Set([...hub.clients].flatMap((client) =>
        [...client.subscriptions.values()].map((subscription) => subscription.instrument)))].sort();
}

function flushPriceUpdates(hub) {
    const now = performance.now();
    if (now - hub.lastFlushAt < PRICE_BATCH_INTERVAL_MS) return;
    hub.lastFlushAt = now;
    for (const client of hub.clients) {
        for (const subscription of client.subscriptions.values()) {
            if (!subscription.dirty || !subscription.candle) continue;
            if (subscription.key && client.subscriptions.get(subscription.key) !== subscription) continue;
            subscription.dirty = false;
            sendJson(client.socket, createPriceEnvelope(
                subscription.symbol, subscription.timeframe, subscription.candle
            ));
        }
    }
}

function schedulePriceFlush(hub) {
    const delay = Math.max(0, PRICE_BATCH_INTERVAL_MS - (performance.now() - hub.lastFlushAt));
    hub.flushTimer = setTimeout(() => {
        flushPriceUpdates(hub);
        schedulePriceFlush(hub);
    }, delay);
}

function publishPrice(hub, priceMessage) {
    if (priceMessage.type !== 'PRICE' || !SYMBOLS_BY_INSTRUMENT.has(priceMessage.instrument)) return;
    const timestamp = Math.floor(Date.parse(priceMessage.time) / 1000);
    const price = priceMidpoint(priceMessage);
    if (!Number.isFinite(timestamp) || !Number.isFinite(price)) return;

    hub.lastPriceTimes.set(priceMessage.instrument, timestamp);
    for (const client of hub.clients) {
        for (const subscription of client.subscriptions.values()) {
            if (subscription.instrument !== priceMessage.instrument) continue;
            if (!subscription.seeded) continue;
            subscription.candle = aggregatePrice(
                subscription.candle, timestamp, price, subscription.granularity
            );
            subscription.dirty = true;
        }
    }
}

function isOandaStreamStale(lastDataAt, now = Date.now()) {
    return now - lastDataAt >= STREAM_HEARTBEAT_TIMEOUT_MS;
}

function getOandaStatus(lastDataAt, connected, now = Date.now(), startedAt = null) {
    const activityAt = lastDataAt ?? startedAt;
    if (activityAt === null) return connected ? 'yellow' : 'red';
    const age = now - activityAt;
    if (age >= STREAM_HEARTBEAT_TIMEOUT_MS) return 'red';
    if (age >= OANDA_ALIVE_INTERVAL_MS || !connected) return 'yellow';
    return 'green';
}

function sendServerPing(hub, client, now = Date.now()) {
    const status = getOandaStatus(
        hub.oandaLastDataAt, hub.oandaStreamConnected, now, hub.oandaStreamStartedAt
    );
    sendJson(client.socket, {
        type: 'ping',
        timestamp: now,
        oandaStatus: status
    });
}

async function refillMissedCandles(hub, instruments, token) {
    const recoveries = new Map();
    for (const instrument of instruments) {
        const lastTimestamp = hub.lastPriceTimes.get(instrument);
        if (!lastTimestamp) continue;
        const subscriptions = [...hub.clients].flatMap((client) =>
            [...client.subscriptions.values()].filter((subscription) => subscription.instrument === instrument));
        for (const subscription of subscriptions) {
            const key = `${instrument}|${subscription.granularity}`;
            if (!recoveries.has(key)) {
                const lastBucket = candleBucket(lastTimestamp, subscription.granularity);
                const from = new Date(Math.max(lastBucket, Math.floor(Date.now() / 1000) - 7 * 86400) * 1000).toISOString();
                recoveries.set(key, requestOandaCandles(instrument, subscription.granularity, { from, count: PAGE_SIZE }, token)
                    .then((candles) => candles.map((candle) => toFcsCandle(candle))
                        .filter((candle) => candle && candle.timestamp >= lastBucket)));
            }
            const candles = await recoveries.get(key);
            for (const candle of candles) {
                if (subscription.client.subscriptions.get(subscription.key) !== subscription) break;
                subscription.candle = {
                    t: candle.timestamp,
                    o: candle.open,
                    h: candle.high,
                    l: candle.low,
                    c: candle.close,
                    v: candle.volume
                };
                subscription.dirty = false;
                sendJson(subscription.client.socket, createPriceEnvelope(
                    subscription.symbol, subscription.timeframe, subscription.candle
                ));
            }
        }
    }
}

async function seedOpenCandles(hub, instruments, token, force = false) {
    const subscriptions = [...hub.clients].flatMap((client) => [...client.subscriptions.values()])
        .filter((subscription) => instruments.includes(subscription.instrument));
    const seeds = new Map();
    const generations = new Map();
    for (const subscription of subscriptions) {
        if (force) {
            subscription.seedGeneration += 1;
            subscription.seeded = false;
            subscription.seeding = false;
        }
        if (subscription.seeded || subscription.seeding) continue;
        subscription.seeding = true;
        generations.set(subscription, subscription.seedGeneration);
        const key = `${subscription.instrument}|${subscription.granularity}`;
        if (!seeds.has(key)) {
            seeds.set(key, requestOandaCandles(
                subscription.instrument, subscription.granularity, { count: 2 }, token
            ).then((candles) => {
                const current = [...candles].reverse().find((candle) => !candle.complete);
                return current ? toFcsCandle(current, true) : null;
            }));
        }
    }

    try {
        for (const subscription of subscriptions) {
            if (!subscription.seeding) continue;
            const generation = generations.get(subscription);
            if (generation !== subscription.seedGeneration) continue;
            if (subscription.client.subscriptions.get(subscription.key) !== subscription) continue;
            const key = `${subscription.instrument}|${subscription.granularity}`;
            let candle;
            try {
                candle = await seeds.get(key);
            } catch (error) {
                if (generation !== subscription.seedGeneration
                    || subscription.client.subscriptions.get(subscription.key) !== subscription) continue;
                throw error;
            }
            if (generation !== subscription.seedGeneration
                || subscription.client.subscriptions.get(subscription.key) !== subscription) continue;
            const seed = candle && {
                t: candle.timestamp,
                o: candle.open,
                h: candle.high,
                l: candle.low,
                c: candle.close,
                v: candle.volume
            };
            subscription.candle = seed;
            subscription.seeding = false;
            subscription.seeded = true;
            subscription.dirty = false;
            if (subscription.candle) {
                sendJson(subscription.client.socket, createPriceEnvelope(
                    subscription.symbol, subscription.timeframe, subscription.candle
                ));
            }
        }
    } catch (error) {
        for (const subscription of subscriptions) {
            if (subscription.seeding
                && generations.get(subscription) === subscription.seedGeneration) {
                subscription.seeding = false;
            }
        }
        throw error;
    }
}

async function readSharedOandaStream(hub, controller, instruments) {
    const { token, accountId } = getCredentials();
    const url = new URL(`${OANDA_STREAM}/accounts/${encodeURIComponent(accountId)}/pricing/stream`);
    url.searchParams.set('instruments', instruments.join(','));
    await refillMissedCandles(hub, instruments, token);
    await seedOpenCandles(hub, instruments, token, true);

    const streamController = new AbortController();
    const abortStream = () => streamController.abort();
    controller.signal.addEventListener('abort', abortStream, { once: true });
    let timedOut = false;
    let lastDataAt = Date.now();
    const watchdog = setInterval(() => {
        if (isOandaStreamStale(lastDataAt)) {
            timedOut = true;
            streamController.abort();
        }
    }, 1000);

    try {
        const response = await fetch(url, {
            headers: { Authorization: `Bearer ${token}` },
            signal: streamController.signal
        });
        if (!response.ok) throw new Error(`OANDA pricing stream failed with HTTP ${response.status}.`);
        hub.oandaStreamConnected = true;

        const decoder = new TextDecoder();
        let buffer = '';
        for await (const chunk of response.body) {
            if (controller.signal.aborted) break;
            buffer += decoder.decode(chunk, { stream: true });
            let newline;
            while ((newline = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, newline).trim();
                buffer = buffer.slice(newline + 1);
                if (!line) continue;
                lastDataAt = Date.now();
                hub.oandaLastDataAt = lastDataAt;
                try {
                    publishPrice(hub, JSON.parse(line));
                } catch (error) {
                    for (const client of hub.clients) {
                        if (client.subscriptions.size > 0) {
                            sendJson(client.socket, { type: 'error', message: `Invalid OANDA pricing frame: ${error.message}` });
                        }
                    }
                }
            }
        }
        if (!controller.signal.aborted) throw new Error('OANDA pricing stream ended unexpectedly.');
    } catch (error) {
        if (timedOut) throw new Error('OANDA pricing stream received no heartbeat or price data for 15 seconds.');
        throw error;
    } finally {
        hub.oandaStreamConnected = false;
        clearInterval(watchdog);
        controller.signal.removeEventListener('abort', abortStream);
    }
}

function waitForReconnectDelay(signal, delay) {
    return new Promise((resolve) => {
        if (signal.aborted) {
            resolve();
            return;
        }
        const finish = () => {
            clearTimeout(timer);
            signal.removeEventListener('abort', finish);
            resolve();
        };
        const timer = setTimeout(finish, delay);
        signal.addEventListener('abort', finish, { once: true });
    });
}

function nextReconnectDelay(delay) {
    return delay < 4000 ? delay * 2 : 10000;
}

async function runSharedOandaStream(hub, controller, instruments) {
    let reconnectDelay = 1000;
    while (!controller.signal.aborted && hub.desiredInstruments === instruments.join(',')) {
        try {
            await readSharedOandaStream(hub, controller, instruments);
            reconnectDelay = 1000;
        } catch (error) {
            if (controller.signal.aborted) break;
            for (const client of hub.clients) {
                if (client.subscriptions.size > 0) sendJson(client.socket, { type: 'error', message: error.message });
            }
            await waitForReconnectDelay(controller.signal, reconnectDelay);
            reconnectDelay = nextReconnectDelay(reconnectDelay);
        }
    }
}

function reconcileOandaStream(hub) {
    const desiredInstruments = getActiveInstruments(hub).join(',');
    const canSeedOnCurrentStream = hub.streamTask && hub.streamInstruments === desiredInstruments;
    hub.desiredInstruments = desiredInstruments;
    if (hub.streamController && hub.streamInstruments !== desiredInstruments) {
        hub.streamController.abort();
        return;
    }
    if (hub.streamTask || !desiredInstruments) {
        if (canSeedOnCurrentStream) {
            try {
                const { token } = getCredentials();
                seedOpenCandles(hub, desiredInstruments.split(','), token).catch((error) => {
                    for (const client of hub.clients) {
                        if ([...client.subscriptions.values()].some((subscription) =>
                            desiredInstruments.split(',').includes(subscription.instrument) && !subscription.seeded)) {
                            sendJson(client.socket, { type: 'error', message: error.message });
                        }
                    }
                    hub.streamController?.abort();
                });
            } catch (error) {
                for (const client of hub.clients) {
                    if (client.subscriptions.size > 0) {
                        sendJson(client.socket, { type: 'error', message: error.message });
                    }
                }
            }
        }
        return;
    }

    const controller = new AbortController();
    const instruments = desiredInstruments.split(',');
    hub.streamController = controller;
    hub.streamInstruments = desiredInstruments;
    hub.oandaStreamConnected = false;
    hub.oandaStreamStartedAt ??= Date.now();
    hub.streamTask = runSharedOandaStream(hub, controller, instruments).finally(() => {
        if (hub.streamController === controller) hub.streamController = null;
        hub.streamTask = null;
        hub.streamInstruments = '';
        if (hub.desiredInstruments) reconcileOandaStream(hub);
    });
}

function handleSharedSocketMessage(hub, client, rawMessage) {
    let message;
    try {
        message = JSON.parse(rawMessage.toString());
    } catch {
        return;
    }

    if (message.type === 'ping') {
        sendJson(client.socket, { type: 'pong', timestamp: message.timestamp });
        return;
    }
    if (message.type === 'remove_all') {
        client.subscriptions.clear();
        reconcileOandaStream(hub);
        return;
    }
    if (message.type === 'leave_symbol') {
        const key = `${message.symbol}|${message.timeframe}`;
        client.subscriptions.delete(key);
        reconcileOandaStream(hub);
        return;
    }
    if (message.type !== 'join_symbol') return;

    try {
        const instrument = mapSymbol(message.symbol);
        const granularity = mapGranularity(message.timeframe);
        const key = `${message.symbol}|${message.timeframe}`;
        client.subscriptions.set(key, {
            key,
            client,
            symbol: message.symbol,
            instrument,
            timeframe: message.timeframe,
            granularity,
            candle: null,
            dirty: false,
            seeded: false,
            seeding: false,
            seedGeneration: 0
        });
        sendJson(client.socket, {
            type: 'message',
            short: 'joined_room',
            symbol: message.symbol,
            timeframe: message.timeframe
        });
        reconcileOandaStream(hub);
    } catch (error) {
        sendJson(client.socket, { type: 'error', message: error.message });
    }
}

function isAllowedOrigin(origin, requestHost) {
    if (!origin) return true;
    try {
        const parsed = new URL(origin);
        const localOrigin = (parsed.hostname === 'localhost' || parsed.hostname === '127.0.0.1')
            && parsed.port === String(PORT);
        const codespaceOrigin = parsed.protocol === 'https:' && parsed.hostname.endsWith('.app.github.dev');
        return parsed.host === requestHost && (localOrigin || codespaceOrigin);
    } catch {
        return false;
    }
}

const STATIC_FILES = new Map([
    ['/examples/simple.html', ['examples/simple.html', 'text/html; charset=utf-8']],
    ['/src/fcsapi-chart.css', ['src/fcsapi-chart.css', 'text/css; charset=utf-8']],
    ['/src/fcsapi-chart.js', ['src/fcsapi-chart.js', 'text/javascript; charset=utf-8']]
]);

function createServer() {
    const hub = {
        clients: new Set(),
        desiredInstruments: '',
        streamInstruments: '',
        streamController: null,
        streamTask: null,
        lastPriceTimes: new Map(),
        lastFlushAt: 0,
        oandaLastDataAt: null,
        oandaStreamStartedAt: null,
        oandaStreamConnected: false
    };
    const historyCache = new Map();
    schedulePriceFlush(hub);
    const server = http.createServer(async (request, response) => {
        const requestUrl = new URL(request.url, `http://${HOST}:${PORT}`);
        if (request.method === 'GET' && requestUrl.pathname === '/') {
            response.writeHead(302, { Location: '/examples/simple.html' });
            response.end();
            return;
        }
        if (request.method === 'GET' && requestUrl.pathname === '/api/candles') {
            await handleCandles(requestUrl, response, historyCache);
            return;
        }
        if (request.method === 'POST' && requestUrl.pathname === '/api/symbols') {
            handleSymbolSearch(request, response);
            return;
        }
        const asset = STATIC_FILES.get(requestUrl.pathname);
        const chunkName = requestUrl.pathname.startsWith('/src/chunks/')
            ? requestUrl.pathname.slice('/src/chunks/'.length)
            : '';
        const chunkPath = /^[A-Za-z0-9_.-]+\.js$/.test(chunkName)
            ? path.join(__dirname, 'src', 'chunks', chunkName)
            : null;
        if (request.method === 'GET' && (asset || chunkPath)) {
            try {
                const filePath = asset ? path.join(__dirname, asset[0]) : chunkPath;
                const contentType = asset ? asset[1] : 'text/javascript; charset=utf-8';
                const contents = await fs.readFile(filePath);
                response.writeHead(200, { 'Content-Type': contentType, 'Cache-Control': 'no-store' });
                response.end(contents);
            } catch {
                response.writeHead(404);
                response.end('Not found');
            }
            return;
        }
        response.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
        response.end('Not found');
    });

    const webSocketServer = new WebSocketServer({ noServer: true });
    server.on('upgrade', (request, socket, head) => {
        const requestUrl = new URL(request.url, `http://${HOST}:${PORT}`);
        if (requestUrl.pathname !== '/ws' || !isAllowedOrigin(request.headers.origin, request.headers.host || '')) {
            socket.destroy();
            return;
        }
        webSocketServer.handleUpgrade(request, socket, head, (webSocket) => {
            webSocketServer.emit('connection', webSocket);
        });
    });

    webSocketServer.on('connection', (socket) => {
        const client = { socket, subscriptions: new Map() };
        hub.clients.add(client);
        const pingTimer = setInterval(() => sendServerPing(hub, client), SERVER_PING_INTERVAL_MS);
        socket.on('message', (message) => handleSharedSocketMessage(hub, client, message));
        socket.on('close', () => {
            clearInterval(pingTimer);
            client.subscriptions.clear();
            hub.clients.delete(client);
            reconcileOandaStream(hub);
        });
        sendJson(socket, { type: 'welcome' });
    });

    server.on('close', () => {
        clearTimeout(hub.flushTimer);
        hub.streamController?.abort();
        historyCache.clear();
    });

    return server;
}

function start() {
    getCredentials();
    const server = createServer();
    server.listen(PORT, HOST, () => {
        console.log(`FCS-compatible OANDA practice proxy listening at http://${HOST}:${PORT}`);
    });
}

if (require.main === module) {
    try {
        start();
    } catch (error) {
        console.error(error.message);
        process.exitCode = 1;
    }
}

module.exports = {
    aggregatePrice,
    candleBucket,
    createServer,
    flushPriceUpdates,
    getCachedHistory,
    isAllowedOrigin,
    mapGranularity,
    mapSymbol,
    nextCandleBoundary,
    nextReconnectDelay,
    publishPrice,
    isOandaStreamStale,
    getOandaStatus,
    searchSymbols,
    toChartCandle,
    toFcsCandle
};