#!/usr/bin/env python
from concurrent.futures import ThreadPoolExecutor
import fnmatch
import os
import re
import subprocess

import typer

LOG_LEVEL_MAP = {
    'ALWAYS': (typer.colors.BLACK, typer.colors.WHITE),
    'NOTICE': (None, typer.colors.MAGENTA),
    'INFO': (None, typer.colors.GREEN),
    'VERBOSE': (None, typer.colors.CYAN),
    'DEBUG': (None, typer.colors.BLUE),
    'WARN': (None, typer.colors.YELLOW),
    'ERROR': (None, typer.colors.RED),
    'FATAL': (typer.colors.RED, typer.colors.BLACK),
}
LOG_PATTERN = re.compile(r"^[\d\-]{10} [\d:]{8} UTC [^\s]+ ([A-Z]+):")

app = typer.Typer(
    help="""Run the DIRAC integration tests.

A local DIRAC setup can be created and tested by running:

\b
  ./integration_tests.py create
  ./integration_tests.py test-server
  ./integration_tests.py test-client

See below for additional subcommands which are useful during local development.

## Extensions

TODO

## Command completion

Command competion of typer based scripts can be enabled by running:

  typer --install-completion

After restarting your terminal you command completion is available using:

  typer ./integration_tests.py run ...
"""
)

@app.command()
def create(install_server: bool=True, install_client:bool=True):
    """Start a local instance of the integration tests"""
    typer.echo(f"Preparing environment")
    subprocess.run([], shell=True)
    if install_server:
        typer.echo(f"Installing server")
    if install_client:
        typer.echo(f"Installing client")


@app.command()
def destroy():
    """Destroy a local instance of the integration tests"""
    typer.echo(f"TODO")


@app.command()
def test_server():
    """Run the server integration tests."""
    typer.secho("Running server tests", err=True, fg=typer.colors.GREEN)
    base_cmd = _build_docker_cmd("server")
    ret = subprocess.run(base_cmd + ["bash", "TestCode/DIRAC/tests/CI/run_tests.sh"], check=False)
    color = typer.colors.GREEN if ret.returncode == 0 else typer.colors.RED
    typer.secho(f"Server tests finished with {ret.returncode}", err=True, fg=color)


@app.command()
def test_client():
    """Run the client integration tests."""
    typer.secho("Running client tests", err=True, fg=typer.colors.GREEN)
    base_cmd = _build_docker_cmd("client")
    ret = subprocess.run(base_cmd + ["bash", "TestCode/DIRAC/tests/CI/run_tests.sh"], check=False)
    color = typer.colors.GREEN if ret.returncode == 0 else typer.colors.RED
    typer.secho(f"Client tests finished with {ret.returncode}", err=True, fg=color)
    # docker cp client:/home/dirac/clientTestOutputs.txt "${BUILD_DIR}/log_client_tests.txt"


@app.command()
def exec_server():
    """Start an interactive session in the server container."""
    cmd = _build_docker_cmd("server")
    cmd += ["bash", "-c", ". $HOME/CONFIG && . $HOME/ServerInstallDIR/bashrc && exec bash"]
    typer.secho("Opening prompt inside server container", err=True, fg=typer.colors.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def exec_client():
    """Start an interactive session in the client container."""
    cmd = _build_docker_cmd("client")
    cmd += ["bash", "-c", ". $HOME/CONFIG && . $HOME/ClientInstallDIR/bashrc && exec bash"]
    typer.secho("Opening prompt inside client container", err=True, fg=typer.colors.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def list_services():
    """List the services which have been running.

    Only the services for which /log/current exists are shown.
    """
    typer.secho("Known services:", err=True)
    for service in _list_services():
        typer.secho(f"* {service}", err=True)


@app.command()
def runsvctrl(command: str, pattern: str):
    """Execute runsvctrl inside the server container."""
    cmd = _build_docker_cmd("server", cwd="/home/dirac/ServerInstallDIR/diracos/runit")
    services = fnmatch.filter(_list_services(), pattern)
    if not services:
        typer.secho(f"No services match {pattern!r}", color=typer.colors.RED)
        raise typer.Exit(code=1)
    cmd += ["runsvctrl", command] + services
    os.execvp(cmd[0], cmd)


@app.command()
def logs(pattern: str="*", lines: int=10, follow: bool=True):
    """Show DIRAC's logs from the service container.
    
    For services matching [--pattern] show the most recent [--lines] from the
    logs. If [--follow] is True, continiously stream the logs.
    """
    services = _list_services()
    base_cmd = _build_docker_cmd("server", tty=False) + ["tail"]
    base_cmd += [f"--lines={lines}"]
    if follow:
        base_cmd += ["-f"]
    with ThreadPoolExecutor(len(services)) as pool:
        for service in fnmatch.filter(services, pattern):
            cmd = base_cmd + [f"ServerInstallDIR/diracos/runit/{service}/log/current"]
            p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=None, text=True)
            pool.submit(_log_popen_stdout, p)


def _build_docker_cmd(container_name, *, cwd="/home/dirac", tty=True):
    cmd = ["docker", "exec"]
    if tty:
        cmd += ["-it"]
    cmd += [
        "-e=TERM=xterm-color",
        "-e=INSTALLROOT=/home/dirac",
        f"-e=INSTALLTYPE={container_name}",
        "-u=dirac",
        f"-w={cwd}",
        container_name,
    ]
    return cmd


def _list_services():
    cmd = _build_docker_cmd("server")
    cmd += ["bash", "-c", 'cd ServerInstallDIR/diracos/runit/; for fn in */*/log/current; do echo "$(dirname "$(dirname "$fn")")"; done']
    ret = subprocess.run(cmd, check=True, stdout=subprocess.PIPE, text=True)
    return ret.stdout.split()


def _log_popen_stdout(p):
    while p.poll() is None:
        line = p.stdout.readline().rstrip()
        if not line:
            continue
        bg, fg = None, None
        if match := LOG_PATTERN.match(line):
            bg, fg = LOG_LEVEL_MAP.get(match.groups()[0], (bg, fg))
        typer.secho(line, err=True, bg=bg, fg=fg)


if __name__ == "__main__":
    app()
