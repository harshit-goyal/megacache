"""TLS helpers shared by MegaCache protocol servers."""

import ssl
import threading
import time
from typing import Any

from .config import Config


def validate_tls_config(config: Config) -> None:
    if bool(config.tls_cert_file) != bool(config.tls_key_file):
        raise ValueError(
            "MEGACACHE_TLS_CERT_FILE and MEGACACHE_TLS_KEY_FILE "
            "must be configured together"
        )


def enable_server_tls(server: Any, config: Config) -> bool:
    validate_tls_config(config)
    if config.tls_cert_file is None:
        return False
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(config.tls_cert_file, config.tls_key_file)
    server.tls_context = context
    return True


class TLSRequestMixin:
    """Perform TLS negotiation inside a worker, never in the accept loop."""

    def initialize_transport(self) -> None:
        self.tls_context = None
        self._draining = threading.Event()
        self._connections = set()
        self._connections_lock = threading.Lock()

    @property
    def is_draining(self) -> bool:
        return self._draining.is_set()

    def start_draining(self) -> None:
        self._draining.set()

    def drain_connections(self, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            with self._connections_lock:
                if not self._connections:
                    return
            time.sleep(0.01)
        with self._connections_lock:
            connections = tuple(self._connections)
        for connection in connections:
            try:
                connection.shutdown(2)
            except OSError:
                pass
            connection.close()

    def process_request_thread(self, request: Any, client_address: tuple) -> None:
        secured = request
        context = getattr(self, "tls_context", None)
        try:
            if self.is_draining:
                self.shutdown_request(request)
                return
            if context is not None:
                request.settimeout(5)
                secured = context.wrap_socket(request, server_side=True)
            with self._connections_lock:
                self._connections.add(secured)
            self.finish_request(secured, client_address)
            self.shutdown_request(secured)
        except (OSError, ssl.SSLError):
            self.shutdown_request(secured)
        except Exception:
            self.handle_error(secured, client_address)
            self.shutdown_request(secured)
        finally:
            with self._connections_lock:
                self._connections.discard(secured)
