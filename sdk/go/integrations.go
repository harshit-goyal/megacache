package megacache

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
)

type traceparentContextKey struct{}

func WithTraceparent(ctx context.Context, value string) context.Context {
	return context.WithValue(ctx, traceparentContextKey{}, value)
}

func TraceparentFromContext(ctx context.Context) string {
	value, _ := ctx.Value(traceparentContextKey{}).(string)
	return value
}

func TraceparentMiddleware(client *Client, next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if value := request.Header.Get("traceparent"); value != "" {
			request = request.WithContext(WithTraceparent(request.Context(), value))
		}
		next.ServeHTTP(writer, request)
	})
}

func (c *Client) FetchContext(
	ctx context.Context,
	key, origin, path string,
	refresh bool,
) (FetchResult, error) {
	return c.Fetch(key, origin, path, refresh, TraceparentFromContext(ctx))
}

func CachedRow(
	ctx context.Context,
	client *Client,
	db *sql.DB,
	key, query string,
	policy CachePolicy,
	scan func(*sql.Row) (interface{}, error),
	args ...interface{},
) (CachedValue, error) {
	return client.GetOrLoad(key, func() ([]byte, error) {
		value, err := scan(db.QueryRowContext(ctx, query, args...))
		if err != nil { return nil, err }
		return json.Marshal(value)
	}, policy)
}
