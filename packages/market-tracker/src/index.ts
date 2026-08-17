import yahooFinance from 'yahoo-finance2';
import { withRetry } from '@worldwideview/seeder-sdk';
import { isMarketOpen } from './isMarketOpen';

export interface StockTick {
  id: string;
  price: number;
  changePercent: number;
  timestamp: number;
}

// The default export's static `quote()` is typed as `(...args: unknown[]): never`
// (deprecated static API), so `await` yields `unknown`. Type the result shape we
// actually consume and narrow it with a guard instead of relying on that return type.
interface MarketQuote {
  symbol: string;
  regularMarketPrice?: number;
  regularMarketChangePercent?: number;
}

function isMarketQuoteArray(value: unknown): value is MarketQuote[] {
  return Array.isArray(value);
}

const TICKERS = ['AAPL', 'MSFT', 'NVDA', 'SPY', 'QQQ'];

async function fetchQuotes(): Promise<StockTick[] | null> {
  if (!isMarketOpen()) {
    return null;
  }

  const quotes = await withRetry(() => yahooFinance.quote(TICKERS));

  if (!isMarketQuoteArray(quotes)) {
    return null;
  }

  const results: StockTick[] = [];
  for (const q of quotes) {
    if (q.regularMarketPrice == null) continue;
    results.push({
      id: q.symbol,
      price: q.regularMarketPrice,
      changePercent: q.regularMarketChangePercent ?? 0,
      timestamp: Date.now(),
    });
  }

  return results;
}

export default {
  name: 'market-tracker',
  interval: 30_000,
  fetch: fetchQuotes,
};
