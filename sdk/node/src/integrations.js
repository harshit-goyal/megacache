"use strict";

function traceparentMiddleware(client) {
  return async function megacacheTraceparent(request, response, next) {
    const value = request.headers?.traceparent;
    return client.withTraceparent(value, next);
  };
}

function cachedDatabaseLoader(client, policy) {
  return (key, query) => client.getOrLoad(key, async () => JSON.stringify(await query()), policy);
}

module.exports = { traceparentMiddleware, cachedDatabaseLoader };
