#!/usr/bin/python
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
from shlex import quote as shlex_quote

import delegator
import paramiko


class SSHShellProcess:
    def __init__(self, return_code, stdout, stderr):
        self.return_code = return_code
        self.output = stdout
        self.err = stderr

    @property
    def ok(self):
        return self.return_code == 0

    def __repr__(self) -> str:
        return f'SSHShellProcess {{return code: {self.return_code}, output: {self.output}, err: {self.err}}}'


class SSHShell:
    def __init__(self, host, username=None, password=None):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy)
        self.client.connect(host, username=username, password=password)

    def command(self, script, timeout=120, get_pty=False):
        logging.debug(f'ssh exec: {script}')
        self.client.get_transport().set_keepalive(5)
        chan = self.client.get_transport().open_session()

        if get_pty:
            chan.get_pty()

        chan.settimeout(timeout)

        chan.exec_command(f'bash --login -c {shlex_quote(script)}')

        bufsize = 4096

        stdout = ''.join(chan.makefile('r', bufsize))
        stderr = ''.join(chan.makefile_stderr('r', bufsize))

        return SSHShellProcess(chan.recv_exit_status(), stdout, stderr)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):  # pylint: disable=redefined-builtin
        self.client.close()


class BashProcess:
    """bash process object"""

    def __init__(self, script, blocking: bool = True, timeout: int = 60) -> None:
        """constructor"""
        # Environ inherits from parent.

        # Remember passed-in arguments.
        self.script = script

        # Run the subprocess.
        self.sub = delegator.run(
            script, block=blocking, timeout=timeout
        )

    @property
    def output(self) -> str:
        """stdout of the running process"""
        return str(self.sub.out)

    @property
    def err(self) -> str:
        """stderr of the running process"""
        return str(self.sub.err)

    @property
    def ok(self) -> bool:
        """if the process exited with a 0 exit code"""
        return self.sub.ok

    @property
    def return_code(self) -> int:
        """the exit code of the process"""
        return self.sub.return_code

    def __repr__(self) -> str:
        return f'BashProcess {{return code: {self.return_code}, output: {self.output}, err: {self.err}}}'


class Bash:
    """an instance of bash"""

    def _exec(self, script, timeout=60) -> BashProcess:  # pylint: disable=unused-argument
        """execute the bash process as a child of this process"""
        return BashProcess(script, timeout=timeout)

    def command(self, script: str, timeout=60) -> BashProcess:
        """form up the command with shlex and execute"""
        logging.debug(f'bash exec: {script}')
        return self._exec(f"bash -c {shlex_quote(script)}", timeout=timeout)


def sshexec(script, host, username=None, password=None):
    with sshshell(host, username=username, password=password) as ssh:
        return ssh.command(script)


def sshshell(host, username=None, password=None):
    return SSHShell(host, username=username, password=password)


def exec(script):  # pylint: disable=redefined-builtin
    return Bash().command(script)


def shell():
    return Bash()
