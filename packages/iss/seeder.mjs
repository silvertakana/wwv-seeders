const WTIA_URL = 'https://api.wheretheiss.at/v1/satellites/25544';

export default {
  // Required: the unique namespace used by your plugin
  id: "iss-tracker",
  
  // Define polling interval inside the seeder
  intervalMs: 5000, 
  
  // The Data Engine will call this function every intervalMs
  async fetch(ctx) {
    const { axios, logger } = ctx;
    
    try {
      // 1. Fetch data from the API
      const response = await axios.get(WTIA_URL);
      const data = response.data;
      
      // 2. Format the data into a GeoEntity array
      const entities = [{
        id: "25544",
        name: "International Space Station",
        latitude: data.latitude,
        longitude: data.longitude,
        altitude: data.altitude * 1000, // Convert kilometers to meters
        velocity: data.velocity,
        visibility: data.visibility,
        footprint: data.footprint
      }];
      
      logger.info(`[ISS] Poll OK: updated position to ${data.latitude}, ${data.longitude}`);
      
      // 3. Return the array of entities, the engine runner will broadcast it over WebSockets
      return entities;
      
    } catch (error) {
      logger.error(`[ISS] Polling error: ${error.message}`);
      return [];
    }
  }
};
