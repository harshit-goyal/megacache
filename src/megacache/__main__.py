"""MegaCache command-line entry point."""

import logging
import threading

from .config import Config
from .engine import CacheEngine
from .resp import MegaCacheRespServer
from .server import MegaCacheServer


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    config = Config.from_env()
    engine = CacheEngine(
        max_entries=config.max_entries,
        default_ttl_seconds=config.default_ttl_seconds,
        default_stale_seconds=config.default_stale_seconds,
        lease_seconds=config.lease_seconds,
    )
    http_server = MegaCacheServer((config.host, config.port), config, engine)
    resp_server = MegaCacheRespServer(
        (config.resp_host, config.resp_port), config, engine
    )
    resp_thread = threading.Thread(target=resp_server.serve_forever, daemon=True)
    resp_thread.start()
    logger = logging.getLogger("megacache")
    logger.info("HTTP API listening on %s:%s", config.host, config.port)
    logger.info("RESP2 API listening on %s:%s", config.resp_host, config.resp_port)
    try:
        http_server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        http_server.server_close()
        resp_server.shutdown()
        resp_server.server_close()
        resp_thread.join(timeout=5)


if __name__ == "__main__":
    main()
