import com.sun.net.httpserver.HttpHandler;
import io.megacache.Integrations;
import io.megacache.MegaCacheClient;

final class HttpDatabaseExample {
    static HttpHandler wrap(HttpHandler application) {
        return Integrations.traceparent(new MegaCacheClient(), application);
    }
}
