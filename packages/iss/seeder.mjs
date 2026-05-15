const WTIA_URL = 'https://api.wheretheiss.at/v1/satellites/25544';

export default {
  id: "iss",
  name: "ISS Tracker",
  interval: 5000,

  async fetch(_ctx) {
    try {
      const res = await fetch(WTIA_URL);
      if (!res.ok) return [];
      const data = await res.json();

      console.log(`[ISS] Poll OK: ${data.latitude}, ${data.longitude}`);
      return [{
        id: "25544",
        name: "International Space Station",
        latitude: data.latitude,
        longitude: data.longitude,
        altitude: data.altitude * 1000,
        velocity: data.velocity,
        visibility: data.visibility,
        footprint: data.footprint
      }];
    } catch (error) {
      console.error(`[ISS] Polling error: ${error.message}`);
      return [];
    }
  }
};
