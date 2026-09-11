export interface GeoLocation {
    lat: number;
    lon: number;
    country: string;
    city: string;
}
/**
 * Geolocate an IPv4 address using the local geoip-lite database.
 * Returns null for private/unresolvable IPs.
 */
export declare function geolocateIp(ip: string): GeoLocation | null;
