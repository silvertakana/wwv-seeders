// src/index.ts
import { setLiveSnapshot } from "@worldwideview/seeder-sdk";
import * as Sentry from "@sentry/node";
import { fetch } from "undici";
var POLLING_INTERVAL_MS = 6e4;
var YAHOO_DELAY_MS = 250;
var DEFAULT_SYMBOLS = [
  { symbol: "^DJI", name: "Dow Jones" },
  { symbol: "^GSPC", name: "S&P 500" },
  { symbol: "^NDX", name: "Nasdaq 100" },
  { symbol: "AAPL", name: "Apple" },
  { symbol: "MSFT", name: "Microsoft" },
  { symbol: "BTC-USD", name: "Bitcoin" }
];
function getSymbolsToTrack() {
  const envSymbols = process.env.MARKET_TRACKER_SYMBOLS;
  if (envSymbols) {
    return envSymbols.split(",").map((s) => {
      const sym = s.trim();
      return { symbol: sym, name: sym };
    });
  }
  return DEFAULT_SYMBOLS;
}
var sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
var RateLimitError = class extends Error {
  constructor() {
    super("Rate limited by Yahoo Finance");
    this.name = "RateLimitError";
  }
};
async function fetchYahooQuote(symbol) {
  var _a, _b;
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1d&range=1d`;
  const res = await fetch(url, {
    headers: {
      "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }
  });
  if (res.status === 429) {
    throw new RateLimitError();
  }
  if (!res.ok) throw new Error(`Yahoo status ${res.status}`);
  const data = await res.json();
  const result = (_b = (_a = data.chart) == null ? void 0 : _a.result) == null ? void 0 : _b[0];
  if (!result || !result.meta) throw new Error("No data or meta returned");
  const meta = result.meta;
  const price = meta.regularMarketPrice;
  const previousClose = meta.chartPreviousClose;
  if (typeof price !== "number" || typeof previousClose !== "number") {
    throw new Error("Missing or invalid pricing fields");
  }
  const change = price - previousClose;
  const changePercent = previousClose !== 0 ? change / previousClose * 100 : 0;
  return { price, change, changePercent };
}
async function pollMarketData() {
  console.log(`[MarketTracker] Poll starting...`);
  const marketObj = /* @__PURE__ */ Object.create(null);
  const fetchedAt = Math.floor(Date.now() / 1e3);
  const symbols = getSymbolsToTrack();
  for (const item of symbols) {
    try {
      const data = await fetchYahooQuote(item.symbol);
      marketObj[item.symbol] = {
        symbol: item.symbol,
        name: item.name,
        price: data.price,
        change: data.change,
        changePercent: data.changePercent,
        last_updated: fetchedAt
      };
    } catch (err) {
      if (err instanceof RateLimitError) {
        console.warn(`[MarketTracker] Rate Limit Hit. Aborting current poll cycle.`);
        throw err;
      }
      console.error(`[MarketTracker] Failed to fetch ${item.symbol}: ${err.message}`);
    }
    await sleep(YAHOO_DELAY_MS);
  }
  try {
    if (Object.keys(marketObj).length > 0) {
      await setLiveSnapshot("market-tracker", marketObj, 5 * 60);
      console.log(`[MarketTracker] Poll OK: Flushed ${Object.keys(marketObj).length} symbols`);
    }
  } catch (err) {
    console.error("[MarketTracker] Flush failed:", err);
    Sentry.captureException(err, { extra: { context: "flushMarketData" } });
  }
}
var nextPollTimeout = null;
var currentPollDelay = POLLING_INTERVAL_MS;
async function pollLoop() {
  try {
    await pollMarketData();
    currentPollDelay = POLLING_INTERVAL_MS;
  } catch (e) {
    if (e instanceof RateLimitError) {
      currentPollDelay = Math.min(currentPollDelay * 2, 10 * 60 * 1e3);
      console.warn(`[MarketTracker] Backing off. Next poll in ${currentPollDelay / 1e3}s`);
    } else {
      currentPollDelay = Math.min(currentPollDelay * 1.5, 5 * 60 * 1e3);
    }
  } finally {
    nextPollTimeout = setTimeout(pollLoop, currentPollDelay);
  }
}
function startMarketTrackerPoller() {
  console.log("[MarketTracker] Starting background polling...");
  pollLoop();
}
var index_default = {
  name: "market-tracker",
  init: startMarketTrackerPoller
};
export {
  index_default as default,
  startMarketTrackerPoller
};
