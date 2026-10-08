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
    const normalized = String(value || '').trim().toUpperCase().replace(/^FX:/, '').replaceAll('/', '_');
    if (normalized === 'XAUUSD' || normalized === 'XAU_USD') return 'XAU_USD';
    throw new Error('Only XAU_USD market data is enabled.');
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

function toFcsCandle(candle) {
    if (!candle.complete || !candle.mid) return null;
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
    const date = new Date(timestamp * 1000);
    if (granularity === 'D') {
        date.setUTCHours(0, 0, 0, 0);
        return Math.floor(date.getTime() / 1000);
    }
    if (granularity === 'W') {
        date.setUTCHours(0, 0, 0, 0);
        date.setUTCDate(date.getUTCDate() - ((date.getUTCDay() + 6) % 7));
        return Math.floor(date.getTime() / 1000);
    }
    if (granularity === 'M') {
        date.setUTCDate(1);
        date.setUTCHours(0, 0, 0, 0);
        return Math.floor(date.getTime() / 1000);
    }
    const interval = SECONDS_PER_GRANULARITY[granularity];
    return Math.floor(timestamp / interval) * interval;
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

async function requestOandaCandles(granularity, query, token) {
    const url = new URL(`${OANDA_API}/instruments/XAU_USD/candles`);
    url.searchParams.set('granularity', granularity);
    url.searchParams.set('price', 'M');
    url.searchParams.set('dailyAlignment', '0');
    url.searchParams.set('alignmentTimezone', 'UTC');
    url.searchParams.set('weeklyAlignment', 'Monday');
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

async function fetchCompleteCandles(granularity, count, token, range = {}) {
    const { from, to } = range;
    if (!from && !to && count < PAGE_SIZE) {
        const page = await requestOandaCandles(granularity, { count: count + 1 }, token);
        return page.map(toFcsCandle).filter(Boolean).slice(-count);
    }

    let cursor = from ? toOandaTime(from) : estimateStart(granularity, count);
    const endTime = to ? Date.parse(toOandaTime(to)) : Date.now();
    const candles = [];

    for (let pageNumber = 0; pageNumber < 20; pageNumber += 1) {
        const raw = await requestOandaCandles(granularity, {
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

async function handleCandles(requestUrl, response) {
    try {
        const instrument = mapSymbol(requestUrl.searchParams.get('symbol'));
        const granularity = mapGranularity(requestUrl.searchParams.get('period'));
        const length = parsePositiveInteger(requestUrl.searchParams.get('length'), 600, MAX_HISTORY);
        const page = parsePositiveInteger(requestUrl.searchParams.get('page'), 1, MAX_HISTORY);
        const from = requestUrl.searchParams.get('from');
        const to = requestUrl.searchParams.get('to');
        const { token } = getCredentials();
        const requested = Math.min(length * page, MAX_HISTORY);
        const candles = await fetchCompleteCandles(granularity, requested, token, { from, to });
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

function publishPrice(state, priceMessage) {
    if (priceMessage.type !== 'PRICE' || priceMessage.instrument !== 'XAU_USD') return;
    const timestamp = Math.floor(Date.parse(priceMessage.time) / 1000);
    const price = priceMidpoint(priceMessage);
    if (!Number.isFinite(timestamp) || !Number.isFinite(price)) return;

    for (const subscription of state.subscriptions.values()) {
        subscription.candle = aggregatePrice(subscription.candle, timestamp, price, subscription.granularity);
        const candle = subscription.candle;
        sendJson(state.socket, {
            type: 'price',
            symbol: subscription.symbol,
            timeframe: subscription.timeframe,
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
        });
    }
}

async function readOandaStream(state, controller) {
    const { token, accountId } = getCredentials();
    const url = new URL(`${OANDA_STREAM}/accounts/${encodeURIComponent(accountId)}/pricing/stream`);
    url.searchParams.set('instruments', 'XAU_USD');
    const response = await fetch(url, {
        headers: { Authorization: `Bearer ${token}` },
        signal: controller.signal
    });
    if (!response.ok) throw new Error(`OANDA pricing stream failed with HTTP ${response.status}.`);

    const decoder = new TextDecoder();
    let buffer = '';
    for await (const chunk of response.body) {
        buffer += decoder.decode(chunk, { stream: true });
        let newline;
        while ((newline = buffer.indexOf('\n')) !== -1) {
            const line = buffer.slice(0, newline).trim();
            buffer = buffer.slice(newline + 1);
            if (!line) continue;
            try {
                publishPrice(state, JSON.parse(line));
            } catch {
                // Ignore malformed upstream stream lines and keep the connection alive.
            }
        }
    }
}

async function runOandaStream(state, controller) {
    while (!controller.signal.aborted && state.subscriptions.size > 0) {
        try {
            await readOandaStream(state, controller);
        } catch (error) {
            if (controller.signal.aborted) break;
            sendJson(state.socket, { type: 'error', message: error.message });
            await new Promise((resolve) => setTimeout(resolve, 1000));
        }
    }
}

function ensureOandaStream(state) {
    if (state.controller) return;
    const controller = new AbortController();
    state.controller = controller;
    runOandaStream(state, controller).finally(() => {
        if (state.controller === controller) state.controller = null;
        if (!state.controller && state.subscriptions.size > 0 && state.socket.readyState === WebSocket.OPEN) ensureOandaStream(state);
    });
}

function stopOandaStream(state) {
    state.controller?.abort();
    state.controller = null;
}

function handleSocketMessage(state, rawMessage) {
    let message;
    try {
        message = JSON.parse(rawMessage.toString());
    } catch {
        return;
    }

    if (message.type === 'ping') {
        sendJson(state.socket, { type: 'pong', timestamp: message.timestamp });
        return;
    }
    if (message.type === 'remove_all') {
        state.subscriptions.clear();
        stopOandaStream(state);
        return;
    }
    if (message.type === 'leave_symbol') {
        state.subscriptions.delete(`${message.symbol}|${message.timeframe}`);
        if (state.subscriptions.size === 0) stopOandaStream(state);
        return;
    }
    if (message.type !== 'join_symbol') return;

    try {
        mapSymbol(message.symbol);
        const granularity = mapGranularity(message.timeframe);
        const key = `${message.symbol}|${message.timeframe}`;
        state.subscriptions.set(key, {
            symbol: message.symbol,
            timeframe: message.timeframe,
            granularity,
            candle: null
        });
        sendJson(state.socket, {
            type: 'message',
            short: 'joined_room',
            symbol: message.symbol,
            timeframe: message.timeframe
        });
        ensureOandaStream(state);
    } catch (error) {
        sendJson(state.socket, { type: 'error', message: error.message });
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
    const server = http.createServer(async (request, response) => {
        const requestUrl = new URL(request.url, `http://${HOST}:${PORT}`);
        if (request.method === 'GET' && requestUrl.pathname === '/') {
            response.writeHead(302, { Location: '/examples/simple.html' });
            response.end();
            return;
        }
        if (request.method === 'GET' && requestUrl.pathname === '/api/candles') {
            await handleCandles(requestUrl, response);
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
        const state = { socket, subscriptions: new Map(), controller: null };
        socket.on('message', (message) => handleSocketMessage(state, message));
        socket.on('close', () => {
            state.subscriptions.clear();
            stopOandaStream(state);
        });
        sendJson(socket, { type: 'welcome' });
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

module.exports = { aggregatePrice, candleBucket, isAllowedOrigin, mapGranularity, mapSymbol, toChartCandle, toFcsCandle };