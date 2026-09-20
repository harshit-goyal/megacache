const { MegaCacheClient } = require("../src");
const { traceparentMiddleware, cachedDatabaseLoader } = require("../src/integrations");

const client = new MegaCacheClient();
const middleware = traceparentMiddleware(client); // app.use(middleware)
const load = cachedDatabaseLoader(client, { ttlSeconds: 30, staleSeconds: 120 });
// const user = await load("user:42", () => pool.query("select ..."));
module.exports = { client, middleware, load };
