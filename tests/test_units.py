import unittest
from backend.actions import validate_action_request as backend_validate
from worker.actions import build_cisco_commands


class TestRouterAutomation(unittest.TestCase):
    def test_backend_whitelist_success(self):
        # Valid actions
        valid, msg = backend_validate("router1", "show_interfaces", {})
        self.assertTrue(valid, msg)

        valid, msg = backend_validate("router2", "show_running_config", {})
        self.assertTrue(valid, msg)

        valid, msg = backend_validate("router1", "show_routes", {})
        self.assertTrue(valid, msg)

        valid, msg = backend_validate("router1", "interface_shutdown", {"interface": "FastEthernet0/0"})
        self.assertTrue(valid, msg)

        valid, msg = backend_validate("router2", "interface_no_shutdown", {"interface": "GigabitEthernet0/1/0"})
        self.assertTrue(valid, msg)

    def test_backend_whitelist_rejections(self):
        # Invalid router
        valid, msg = backend_validate("router99", "show_interfaces", {})
        self.assertFalse(valid)
        self.assertIn("Invalid router_id", msg)

        # Invalid action (CLI injection attempt)
        valid, msg = backend_validate("router1", "reload", {})
        self.assertFalse(valid)
        self.assertIn("not in the allowed action whitelist", msg)

        # Missing required interface param
        valid, msg = backend_validate("router1", "interface_shutdown", {})
        self.assertFalse(valid)
        self.assertIn("Missing required parameter", msg)

        # Invalid interface name (command injection attempt)
        valid, msg = backend_validate("router1", "interface_shutdown", {"interface": "eth0; rm -rf /"})
        self.assertFalse(valid)
        self.assertIn("Invalid interface name", msg)

    def test_worker_cisco_command_generation(self):
        cmds, is_cfg = build_cisco_commands("show_interfaces", {})
        self.assertFalse(is_cfg)
        self.assertIn("show ip interface brief", cmds)

        cmds, is_cfg = build_cisco_commands("interface_shutdown", {"interface": "FastEthernet0/1"})
        self.assertTrue(is_cfg)
        self.assertEqual(cmds, [
            "configure terminal",
            "interface FastEthernet0/1",
            "shutdown",
            "end"
        ])

        cmds, is_cfg = build_cisco_commands("interface_no_shutdown", {"interface": "GigabitEthernet0/0"})
        self.assertTrue(is_cfg)
        self.assertEqual(cmds, [
            "configure terminal",
            "interface GigabitEthernet0/0",
            "no shutdown",
            "end"
        ])


if __name__ == "__main__":
    unittest.main()
