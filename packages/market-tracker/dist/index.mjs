// src/index.ts
import yahooFinance from "yahoo-finance2";
import { withRetry } from "@worldwideview/seeder-sdk";

// src/isMarketOpen.ts
import { toZonedTime } from "date-fns-tz";
import { getDay, getHours, getMinutes } from "date-fns";
var TZ = "America/New_York";
function isMarketOpen(now) {
  const date = now ?? /* @__PURE__ */ new Date();
  const zonedDate = toZonedTime(date, TZ);
  const day = getDay(zonedDate);
  if (day === 0 || day === 6) {
    return false;
  }
  const minuteOfDay = getHours(zonedDate) * 60 + getMinutes(zonedDate);
  return minuteOfDay >= 570 && minuteOfDay < 960;
}

// src/index.ts
var TICKERS = ["AAPL", "MSFT", "NVDA", "SPY", "QQQ"];
async function fetchQuotes() {
  if (!isMarketOpen()) {
    return null;
  }
  const quotes = await withRetry(() => yahooFinance.quote(TICKERS));
  const results = [];
  for (const q of quotes) {
    if (q.regularMarketPrice == null) continue;
    results.push({
      id: q.symbol,
      price: q.regularMarketPrice,
      changePercent: q.regularMarketChangePercent ?? 0,
      timestamp: Date.now()
    });
  }
  return results;
}
var index_default = {
  name: "market-tracker",
  interval: 3e4,
  fetch: fetchQuotes
};
export {
  index_default as default
};
