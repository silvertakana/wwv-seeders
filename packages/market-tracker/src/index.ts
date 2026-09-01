import YahooFinance from 'yahoo-finance2';
import { withRetry, getLiveSnapshot } from '@worldwideview/seeder-sdk';
import { isMarketOpen } from './isMarketOpen';

// yahoo-finance2 v4 removed the static API: instantiate the client first.
const yahooFinance = new YahooFinance({ suppressNotices: ['yahooSurvey'] });

export interface StockTick {
  id: string;
  price: number;
  changePercent: number;
  timestamp: number;
}

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
    // Market closed: serve the last published snapshot so the scheduler does not
    // count a nightly/weekend idle poll as a failure.
    const prev = await getLiveSnapshot('market-tracker');
    return prev && Array.isArray((prev as any).items) ? (prev as any).items : [];
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
