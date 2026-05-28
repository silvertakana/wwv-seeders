import yahooFinance from 'yahoo-finance2';
import { withRetry } from '@worldwideview/seeder-sdk';
import { isMarketOpen } from './isMarketOpen';

export interface StockTick {
  id: string;
  price: number;
  changePercent: number;
  timestamp: number;
}

const TICKERS = ['AAPL', 'MSFT', 'NVDA', 'SPY', 'QQQ'];

async function fetchQuotes(): Promise<StockTick[] | null> {
  if (!isMarketOpen()) {
    return null;
  }

  const quotes = await withRetry(() => yahooFinance.quote(TICKERS));

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
