import re
from typing import Dict, Any, List, Tuple

# Defense in depth: validate parameters against interface regex
INTERFACE_REGEX = re.compile(
    r"^(FastEthernet|GigabitEthernet|Ethernet|Serial)\d+(/\d+)*$",
    re.IGNORECASE
)

ACTION_WHITELIST = {
    "show_interfaces",
    "show_running_config",
    "show_routes",
    "interface_shutdown",
    "interface_no_shutdown"
}


def build_cisco_commands(action: str, params: Dict[str, Any]) -> Tuple[List[str], bool]:
    """
    Validate and translate an action and its parameters into an ordered sequence
    of Cisco IOS commands.
    Returns (commands: List[str], is_config_mode: bool).
    """
    if action not in ACTION_WHITELIST:
        raise ValueError(f"Action '{action}' is not permitted by whitelist.")

    commands = []
    is_config_mode = False

    if action == "show_interfaces":
        commands = [
            "terminal length 0",
            "show ip interface brief"
        ]

    elif action == "show_running_config":
        commands = [
            "terminal length 0",
            "show running-config"
        ]

    elif action == "show_routes":
        commands = [
            "terminal length 0",
            "show ip route"
        ]

    elif action == "interface_shutdown":
        if_name = params.get("interface", "").strip()
        if not if_name or not INTERFACE_REGEX.match(if_name):
            raise ValueError(f"Invalid interface name: '{if_name}'")
        is_config_mode = True
        commands = [
            "configure terminal",
            f"interface {if_name}",
            "shutdown",
            "end"
        ]

    elif action == "interface_no_shutdown":
        if_name = params.get("interface", "").strip()
        if not if_name or not INTERFACE_REGEX.match(if_name):
            raise ValueError(f"Invalid interface name: '{if_name}'")
        is_config_mode = True
        commands = [
            "configure terminal",
            f"interface {if_name}",
            "no shutdown",
            "end"
        ]

    return commands, is_config_mode
