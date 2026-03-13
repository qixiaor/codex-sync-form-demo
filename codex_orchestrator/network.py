from __future__ import annotations

import os
import socket


DEFAULT_LOCAL_PROXY_URL = "http://127.0.0.1:7890"
DEFAULT_NO_PROXY = "127.0.0.1,localhost,::1"


def is_proxy_reachable(proxy_url: str) -> bool:
    if not proxy_url.startswith("http://"):
        return False
    host_port = proxy_url.removeprefix("http://").split("/", 1)[0]
    host, _, port_text = host_port.partition(":")
    if not host or not port_text:
        return False
    try:
        with socket.create_connection((host, int(port_text)), timeout=0.3):
            return True
    except OSError:
        return False


def resolve_proxy_url(proxy_url: str | None, auto_proxy: bool = True) -> str | None:
    if proxy_url:
        return proxy_url
    if auto_proxy and is_proxy_reachable(DEFAULT_LOCAL_PROXY_URL):
        return DEFAULT_LOCAL_PROXY_URL
    return None


def apply_proxy_to_env(
    env: dict[str, str],
    proxy_url: str | None,
    auto_proxy: bool = True,
) -> str | None:
    resolved_proxy_url = resolve_proxy_url(proxy_url, auto_proxy=auto_proxy)
    if not resolved_proxy_url:
        return None

    env["HTTP_PROXY"] = resolved_proxy_url
    env["HTTPS_PROXY"] = resolved_proxy_url
    env["ALL_PROXY"] = resolved_proxy_url
    env["http_proxy"] = resolved_proxy_url
    env["https_proxy"] = resolved_proxy_url
    env["all_proxy"] = resolved_proxy_url
    env["NO_PROXY"] = DEFAULT_NO_PROXY
    env["no_proxy"] = DEFAULT_NO_PROXY
    return resolved_proxy_url


def apply_process_proxy(proxy_url: str | None, auto_proxy: bool = True) -> str | None:
    return apply_proxy_to_env(os.environ, proxy_url=proxy_url, auto_proxy=auto_proxy)
