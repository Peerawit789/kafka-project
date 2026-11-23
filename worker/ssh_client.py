import time
import re
import socket
import logging
import paramiko
from typing import List, Tuple

logger = logging.getLogger(__name__)

# Strip ANSI escape sequences from terminal streams
ANSI_ESCAPE = re.compile(r'\x1B(?:[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])')


class CiscoSSHClient:
    def __init__(self, host: str, port: int = 22, username: str = "", password: str = "", enable_password: str = "",
                 connect_timeout: int = 8, read_timeout: int = 15):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.enable_password = enable_password or password
        self.connect_timeout = connect_timeout
        self.read_timeout = read_timeout
        self.client = None
        self.channel = None

    def connect(self):
        """Establish SSH connection and open interactive shell channel."""
        logger.info(f"Connecting to router {self.host}:{self.port} as '{self.username}'...")
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            self.client.connect(
                hostname=self.host,
                port=self.port,
                username=self.username,
                password=self.password,
                timeout=self.connect_timeout,
                banner_timeout=10,
                auth_timeout=10,
                look_for_keys=False,
                allow_agent=False
            )
            self.channel = self.client.invoke_shell(term='vt100', width=160, height=200)
            self.channel.settimeout(self.read_timeout)

            # Wait for initial prompt
            initial_output = self._wait_for_prompt()
            logger.debug(f"Connected to {self.host}. Initial prompt received.")

            # Check if we are in user exec mode (ends with >)
            clean_initial = ANSI_ESCAPE.sub('', initial_output).strip()
            if clean_initial.endswith(">"):
                logger.info(f"Entering privileged EXEC mode on {self.host}...")
                self._enter_enable_mode()

            # Ensure paging is disabled
            self._send_command_simple("terminal length 0")

        except Exception as e:
            self.close()
            raise ConnectionError(f"SSH connection failed to {self.host}:{self.port} - {str(e)}")

    def _wait_for_prompt(self, timeout: int = None, custom_prompts: List[str] = None) -> str:
        """Read channel buffer until standard Cisco prompt pattern is reached."""
        max_time = timeout or self.read_timeout
        start_time = time.time()
        output = ""

        # Default prompt matching: hostname>, hostname#, hostname(config)#, hostname(config-if)#, or Password:
        prompt_regex = re.compile(r'([A-Za-z0-9_-]+(\([A-Za-z0-9_.-]+\))?[>#]\s*)$|(Password:\s*)$')

        while (time.time() - start_time) < max_time:
            if self.channel.recv_ready():
                chunk = self.channel.recv(4096).decode('utf-8', errors='ignore')
                output += chunk
                clean_chunk = ANSI_ESCAPE.sub('', output)

                if custom_prompts:
                    if any(p in clean_chunk for p in custom_prompts):
                        return output
                elif prompt_regex.search(clean_chunk.strip()):
                    return output
            else:
                time.sleep(0.1)

        raise TimeoutError(f"Timeout ({max_time}s) waiting for router prompt on {self.host}. Output so far:\n{output}")

    def _enter_enable_mode(self):
        """Send enable command and handle password prompt if required."""
        self.channel.send("enable\n")
        output = ""
        start_time = time.time()

        while (time.time() - start_time) < 8:
            if self.channel.recv_ready():
                chunk = self.channel.recv(4096).decode('utf-8', errors='ignore')
                output += chunk
                clean = ANSI_ESCAPE.sub('', output).strip()
                if clean.endswith("Password:"):
                    self.channel.send(self.enable_password + "\n")
                elif clean.endswith("#"):
                    return
            time.sleep(0.1)

        clean = ANSI_ESCAPE.sub('', output).strip()
        if not clean.endswith("#"):
            raise PermissionError(f"Failed to enter privileged EXEC mode (#) on {self.host}")

    def _send_command_simple(self, cmd: str):
        """Execute helper command without recording main output."""
        self.channel.send(cmd + "\n")
        time.sleep(0.2)
        self._wait_for_prompt(timeout=5)

    def execute_commands(self, commands: List[str]) -> str:
        """
        Execute an ordered list of commands on the interactive shell and accumulate cleaned output.
        """
        if not self.channel or self.channel.closed:
            raise RuntimeError("SSH channel is not open. Call connect() first.")

        full_output = []

        for cmd in commands:
            logger.info(f"Executing command on {self.host}: {cmd}")
            self.channel.send(cmd + "\n")
            time.sleep(0.15)
            cmd_output = self._wait_for_prompt(timeout=self.read_timeout)
            cleaned = ANSI_ESCAPE.sub('', cmd_output)
            full_output.append(cleaned)

        raw_result = "\n".join(full_output)
        # Normalize newlines
        return raw_result.replace("\r\n", "\n").strip()

    def close(self):
        """Gracefully close channel and client connection."""
        if self.channel:
            try:
                self.channel.close()
            except Exception:
                pass
            self.channel = None

        if self.client:
            try:
                self.client.close()
            except Exception:
                pass
            self.client = None
