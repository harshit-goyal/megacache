package io.megacache;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

public final class Integrations {
    private Integrations() {}

    public static HttpHandler traceparent(MegaCacheClient client, HttpHandler next) {
        return exchange -> {
            String value = exchange.getRequestHeaders().getFirst("traceparent");
            try (MegaCacheClient.TraceparentScope ignored =
                     client.requestTraceparent(value)) {
                next.handle(exchange);
            }
        };
    }

    public static byte[] cachedQuery(
        MegaCacheClient client,
        Connection connection,
        String key,
        String sql,
        MegaCacheClient.CachePolicy policy,
        Function<ResultSet, byte[]> mapper
    ) throws IOException, SQLException {
        try {
            return client.getOrLoad(key, () -> {
                try (java.sql.Statement statement = connection.createStatement();
                     ResultSet rows = statement.executeQuery(sql)) {
                    return mapper.apply(rows);
                } catch (SQLException error) {
                    throw new QueryFailure(error);
                }
            }, policy).value;
        } catch (QueryFailure error) {
            throw (SQLException) error.getCause();
        }
    }

    private static final class QueryFailure extends RuntimeException {
        private static final long serialVersionUID = 1L;
        QueryFailure(SQLException cause) { super(cause); }
    }
}
