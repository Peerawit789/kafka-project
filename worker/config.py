import os
import yaml
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

CONFIG_PATH = os.path.join(os.path.dirname(__file__), "routers.yaml")


def load_routers_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    """
    Load routers.yaml and resolve environment variable references for credentials.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Router configuration file not found at {path}")

    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    routers = data.get("routers", {})
    resolved = {}

    for router_id, cfg in routers.items():
        user_env = cfg.get("username_env", "ROUTER_USER")
        pass_env = cfg.get("password_env", "ROUTER_PASS")
        enable_pass_env = cfg.get("enable_password_env", "ROUTER_ENABLE_PASS")

        username = os.getenv(user_env, "")
        password = os.getenv(pass_env, "")
        enable_password = os.getenv(enable_pass_env, password)

        resolved[router_id] = {
            "host": cfg.get("host"),
            "port": int(cfg.get("port", 22)),
            "username": username,
            "password": password,
            "enable_password": enable_password,
        }

    return resolved


def get_router_info(router_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve connection information for a specific router_id."""
    routers = load_routers_config()
    info = routers.get(router_id)
    if not info:
        logger.error(f"Router '{router_id}' not found in routers inventory.")
        return None
    return info
