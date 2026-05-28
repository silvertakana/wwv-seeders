import { toZonedTime } from 'date-fns-tz';
import { getDay, getHours, getMinutes } from 'date-fns';

const TZ = 'America/New_York';

/**
 * Returns true if the US equities market is currently open.
 * Market hours: weekdays 09:30-16:00 ET (Eastern Time, honors DST automatically).
 *
 * @param now - optional Date to check; defaults to new Date() (current wall-clock time)
 */
export function isMarketOpen(now?: Date): boolean {
  const date = now ?? new Date();
  const zonedDate = toZonedTime(date, TZ);

  const day = getDay(zonedDate); // 0=Sunday, 1=Monday, ..., 6=Saturday
  if (day === 0 || day === 6) {
    return false;
  }

  const minuteOfDay = getHours(zonedDate) * 60 + getMinutes(zonedDate);
  // Market open window: [09:30, 16:00) = [570, 960) minutes from midnight
  return minuteOfDay >= 570 && minuteOfDay < 960;
}
