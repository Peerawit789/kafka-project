import re
from typing import Dict, Any, Tuple

# Whitelist allowed routers
ALLOWED_ROUTERS = {"router1", "router2"}

# Interface name validation regex
INTERFACE_REGEX = re.compile(
    r"^(FastEthernet|GigabitEthernet|Ethernet|Serial)\d+(/\d+)*$",
    re.IGNORECASE
)

# Whitelist actions and their required / allowed parameters
ACTION_WHITELIST = {
    "show_interfaces": {
        "description": "Show brief IP interface status (show ip interface brief)",
        "required_params": [],
        "optional_params": []
    },
    "show_running_config": {
        "description": "Display active running configuration (show running-config)",
        "required_params": [],
        "optional_params": []
    },
    "show_routes": {
        "description": "Display routing table (show ip route)",
        "required_params": [],
        "optional_params": []
    },
    "interface_shutdown": {
        "description": "Shutdown specified network interface",
        "required_params": ["interface"],
        "optional_params": []
    },
    "interface_no_shutdown": {
        "description": "Bring up specified network interface (no shutdown)",
        "required_params": ["interface"],
        "optional_params": []
    }
}


def validate_action_request(router_id: str, action: str, params: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Validate that router_id, action, and params conform strictly to security whitelist.
    Returns (is_valid: bool, error_message: str).
    """
    if not router_id or router_id.strip() not in ALLOWED_ROUTERS:
        return False, f"Invalid router_id '{router_id}'. Allowed routers: {list(ALLOWED_ROUTERS)}"

    if action not in ACTION_WHITELIST:
        return False, f"Action '{action}' is not in the allowed action whitelist. Allowed: {list(ACTION_WHITELIST.keys())}"

    spec = ACTION_WHITELIST[action]
    required = spec["required_params"]

    for req in required:
        if req not in params or not str(params[req]).strip():
            return False, f"Missing required parameter '{req}' for action '{action}'"

    # Specific param validations
    if "interface" in params:
        if_name = str(params["interface"]).strip()
        if not INTERFACE_REGEX.match(if_name):
            return False, (
                f"Invalid interface name '{if_name}'. "
                "Must match format like FastEthernet0/0, GigabitEthernet0/1, Serial0/0/0"
            )

    # Disallow unexpected parameters
    allowed_params = set(required + spec["optional_params"])
    for p in params.keys():
        if p not in allowed_params:
            return False, f"Unexpected parameter '{p}' provided for action '{action}'"

    return True, ""
