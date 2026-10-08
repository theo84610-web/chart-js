'use strict';

const assert = require('node:assert/strict');
const test = require('node:test');
const { aggregatePrice, candleBucket, isAllowedOrigin, mapGranularity, mapSymbol, toChartCandle, toFcsCandle } = require('./server');

test('maps chart symbols and the requested OANDA timeframes', () => {
    assert.equal(mapSymbol('FX:XAUUSD'), 'XAU_USD');
    assert.equal(mapSymbol('XAU_USD'), 'XAU_USD');
    assert.equal(mapGranularity('1m'), 'M1');
    assert.equal(mapGranularity('5'), 'M5');
    assert.equal(mapGranularity('60'), 'H1');
    assert.equal(mapGranularity('1D'), 'D');
    assert.equal(mapGranularity('1M'), 'M');
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

test('aligns daily candles at UTC midnight', () => {
    const timestamp = Date.parse('2026-10-07T17:45:00Z') / 1000;
    assert.equal(candleBucket(timestamp, 'D'), Date.parse('2026-10-07T00:00:00Z') / 1000);
});

test('allows only localhost or same-origin Codespaces WebSocket upgrades', () => {
    assert.equal(isAllowedOrigin('http://127.0.0.1:3000', '127.0.0.1:3000'), true);
    assert.equal(isAllowedOrigin('https://workspace-3000.app.github.dev', 'workspace-3000.app.github.dev'), true);
    assert.equal(isAllowedOrigin('https://attacker.example', '127.0.0.1:3000'), false);
});