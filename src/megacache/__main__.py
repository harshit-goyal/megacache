"""MegaCache command-line entry point."""

import logging

from .config import Config
from .engine import CacheEngine
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
    server = MegaCacheServer((config.host, config.port), config, engine)
    logging.getLogger("megacache").info(
        "MegaCache listening on http://%s:%s", config.host, config.port
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

