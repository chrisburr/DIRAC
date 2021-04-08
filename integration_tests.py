#!/usr/bin/env python
import fnmatch
import io
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from typing import Optional

import git
import typer
import yaml
from packaging.version import Version
from typer import colors as c

# Editable configuration
DEFAULT_HOST_OS = "cc7"
DEFAULT_MYSQL_VER = "8.0"
DEFAULT_ES_VER = "7.9.1"
FEATURE_VARIABLES = {
    "DIRACOSVER": "master",
    "DIRACOS_TARBALL_PATH": "",
    "TEST_HTTPS": "No",
    "DIRAC_USE_NEWTHREADPOOL": None,
    "USE_PYTHON3": None,
}
DEFAULT_MODULES = {
    "DIRAC": Path(__file__).parent.absolute(),
}

# Static configuration
DB_USER = "Dirac"
DB_PASSWORD = "Dirac"
DB_ROOTUSER = "root"
DB_ROOTPWD = "password"
DB_HOST = "mysql"
DB_PORT = "3306"

# Implementation details
LOG_LEVEL_MAP = {
    "ALWAYS": (c.BLACK, c.WHITE),
    "NOTICE": (None, c.MAGENTA),
    "INFO": (None, c.GREEN),
    "VERBOSE": (None, c.CYAN),
    "DEBUG": (None, c.BLUE),
    "WARN": (None, c.YELLOW),
    "ERROR": (None, c.RED),
    "FATAL": (c.RED, c.BLACK),
}
LOG_PATTERN = re.compile(r"^[\d\-]{10} [\d:]{8} UTC [^\s]+ ([A-Z]+):")

app = typer.Typer(
    help="""Run the DIRAC integration tests.

A local DIRAC setup can be created and tested by running:

\b
  ./integration_tests.py create

This is equivalent to running:

\b
  ./integration_tests.py prepare-environment
  ./integration_tests.py install-server
  ./integration_tests.py install-client
  ./integration_tests.py test-server
  ./integration_tests.py test-client

The test setup can be shutdown using:

\b
  ./integration_tests.py destroy

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
def create(
    flags: Optional[list[str]] = typer.Argument(None),
    editable: Optional[bool] = None,
    extra_modules: Optional[list[str]] = None,
    release_var: Optional[str] = None,
    run_server_tests: bool = True,
    run_client_tests: bool = True,
):
    """Start a local instance of the integration tests"""
    prepare_environment(flags, editable, extra_modules, release_var)
    install_server()
    install_client()
    if run_server_tests:
        test_server()
    if run_client_tests:
        test_client()


@app.command()
def destroy():
    """Destroy a local instance of the integration tests"""
    typer.secho("Shutting down and removing containers", err=True, fg=c.GREEN)
    with _gen_docker_compose(DEFAULT_MODULES) as docker_compose_fn:
        os.execvpe(
            "docker-compose",
            ["docker-compose", "-f", docker_compose_fn, "down", "-t", "0"],
            _make_env({}),
        )


@app.command()
def prepare_environment(
    flags: Optional[list[str]] = typer.Argument(None),
    editable: Optional[bool] = None,
    extra_modules: Optional[list[str]] = None,
    release_var: Optional[str] = None,
):
    _check_containers_running(is_up=False)
    if editable is None:
        editable = sys.stdout.isatty()
        typer.secho(
            f"No value passed for --[no-]editable, automatically detected: {editable}",
            fg=c.YELLOW,
        )
    typer.echo(f"Preparing environment")

    modules = DEFAULT_MODULES | dict(f.split("=", 1) for f in extra_modules)
    modules = {k: Path(v).absolute() for k, v in modules.items()}

    flags = dict(f.split("=", 1) for f in flags)
    docker_compose_env = _make_env(flags)
    server_flags = {}
    client_flags = {}
    for key, value in flags.items():
        if key.startswith("SERVER_"):
            server_flags[key[len("SERVER_") :]] = value
        elif key.startswith("CLIENT_"):
            client_flags[key[len("CLIENT_") :]] = value
        else:
            server_flags[key] = value
            client_flags[key] = value
    server_config = _make_config(modules, server_flags, release_var, editable)
    client_config = _make_config(modules, client_flags, release_var, editable)
    typer.secho("## Server config is:", fg=c.BRIGHT_WHITE, bg=c.BLACK)
    typer.secho(server_config)
    typer.secho("## Client config is:", fg=c.BRIGHT_WHITE, bg=c.BLACK)
    typer.secho(client_config)

    typer.secho("Running docker-compose to create contianers", fg=c.GREEN)
    with _gen_docker_compose(modules) as docker_compose_fn:
        subprocess.run(
            ["docker-compose", "-f", docker_compose_fn, "up", "-d"],
            check=True,
            env=docker_compose_env,
        )

    typer.secho("Creating users in server and client containers", fg=c.GREEN)
    for container_name in ["server", "client"]:
        cmd = _build_docker_cmd(container_name, user="root", cwd="/")
        gid = str(os.getgid())
        uid = str(os.getuid())
        subprocess.run(cmd + ["groupadd", "--gid", gid, "dirac"], check=True)
        subprocess.run(
            cmd
            + [
                "useradd",
                "--uid",
                uid,
                "--gid",
                gid,
                "-s",
                "/bin/bash",
                "-d",
                "/home/dirac",
                "dirac",
            ],
            check=True,
        )
        subprocess.run(cmd + ["chown", "dirac", "/home/dirac"], check=True)

    typer.secho("Creating MySQL user", fg=c.GREEN)
    cmd = ["docker", "exec", "mysql", "mysql", f"--password={DB_ROOTPWD}", "-e"]
    # It sometimes takes a while for MySQL to be ready so wait for a while if needed
    for _ in range(10):
        ret = subprocess.run(
            cmd + [f"CREATE USER '{DB_USER}'@'%' IDENTIFIED BY '{DB_PASSWORD}';"],
            check=False,
        )
        if ret.returncode != 0:
            typer.secho("Failed to connect to MySQL, will retry in 10 seconds", fg=c.YELLOW)
            time.sleep(10)
        break
    else:
        raise Exception(ret)
    subprocess.run(
        cmd + [f"CREATE USER '{DB_USER}'@'localhost' IDENTIFIED BY '{DB_PASSWORD}';"],
        check=True,
    )
    subprocess.run(
        cmd + [f"CREATE USER '{DB_USER}'@'mysql' IDENTIFIED BY '{DB_PASSWORD}';"],
        check=True,
    )

    typer.secho("Copying files to containers", fg=c.GREEN)
    for name, config in [("server", server_config), ("client", client_config)]:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "CONFIG"
            path.write_text(config)
            subprocess.run(
                ["docker", "cp", str(path), f"{name}:/home/dirac"],
                check=True,
            )

    # TODO: Copy DIRACOS_TARBALL_PATH if it is a local directory containing a DIRACOS tarball
    # if ls "${DIRACOS_TARBALL_PATH}"/diracos-*.tar.gz &> /dev/null; then
    #     docker cp "${DIRACOS_TARBALL_PATH}" server:"${DIRACOS_TARBALL_PATH}"
    #     docker cp "${DIRACOS_TARBALL_PATH}" client:"${DIRACOS_TARBALL_PATH}"
    # fi


@app.command()
def install_server():
    _check_containers_running()

    typer.secho("Running server installation", fg=c.GREEN)
    base_cmd = _build_docker_cmd("server", tty=False)
    subprocess.run(
        base_cmd
        + ["bash", "/home/dirac/LocalRepo/TestCode/DIRAC/tests/CI/install_server.sh"],
        check=True,
    )

    typer.secho("Copying credentials and certificates", fg=c.GREEN)
    base_cmd = _build_docker_cmd("client", tty=False)
    subprocess.run(
        base_cmd
        + [
            "mkdir",
            "-p",
            "/home/dirac/ServerInstallDIR/user",
            "/home/dirac/ClientInstallDIR/etc",
            "/home/dirac/.globus",
        ],
        check=True,
    )
    for path in [
        "etc/grid-security",
        "user/client.pem",
        "user/client.key",
        f"/tmp/x509up_u{os.getuid()}",
    ]:
        source = os.path.join("/home/dirac/ServerInstallDIR", path)
        ret = subprocess.run(
            ["docker", "cp", f"server:{source}", "-"],
            check=True,
            text=False,
            stdout=subprocess.PIPE,
        )
        if path.startswith("user/"):
            dest = f"client:/home/dirac/ServerInstallDIR/{os.path.dirname(path)}"
        elif path.startswith("/"):
            dest = f"client:{os.path.dirname(path)}"
        else:
            dest = f"client:/home/dirac/ClientInstallDIR/{os.path.dirname(path)}"
        subprocess.run(
            ["docker", "cp", "-", dest], check=True, text=False, input=ret.stdout
        )
    subprocess.run(
        base_cmd
        + [
            "bash",
            "-c",
            "cp /home/dirac/ServerInstallDIR/user/client.* /home/dirac/.globus/",
        ],
        check=True,
    )


@app.command()
def install_client():
    _check_containers_running()
    typer.secho("Running client installation", fg=c.GREEN)
    base_cmd = _build_docker_cmd("client")
    subprocess.run(
        base_cmd
        + ["bash", "/home/dirac/LocalRepo/TestCode/DIRAC/tests/CI/install_client.sh"],
        check=True,
    )


@app.command()
def test_server():
    """Run the server integration tests."""
    _check_containers_running()
    typer.secho("Running server tests", err=True, fg=c.GREEN)
    base_cmd = _build_docker_cmd("server")
    ret = subprocess.run(
        base_cmd + ["bash", "TestCode/DIRAC/tests/CI/run_tests.sh"], check=False
    )
    color = c.GREEN if ret.returncode == 0 else c.RED
    typer.secho(f"Server tests finished with {ret.returncode}", err=True, fg=color)


@app.command()
def test_client():
    """Run the client integration tests."""
    _check_containers_running()
    typer.secho("Running client tests", err=True, fg=c.GREEN)
    base_cmd = _build_docker_cmd("client")
    ret = subprocess.run(
        base_cmd + ["bash", "TestCode/DIRAC/tests/CI/run_tests.sh"], check=False
    )
    color = c.GREEN if ret.returncode == 0 else c.RED
    typer.secho(f"Client tests finished with {ret.returncode}", err=True, fg=color)
    # docker cp client:/home/dirac/clientTestOutputs.txt "${BUILD_DIR}/log_client_tests.txt"


@app.command()
def exec_server():
    """Start an interactive session in the server container."""
    _check_containers_running()
    cmd = _build_docker_cmd("server")
    cmd += [
        "bash",
        "-c",
        ". $HOME/CONFIG && . $HOME/ServerInstallDIR/bashrc && exec bash",
    ]
    typer.secho("Opening prompt inside server container", err=True, fg=c.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def exec_client():
    """Start an interactive session in the client container."""
    _check_containers_running()
    cmd = _build_docker_cmd("client")
    cmd += [
        "bash",
        "-c",
        ". $HOME/CONFIG && . $HOME/ClientInstallDIR/bashrc && exec bash",
    ]
    typer.secho("Opening prompt inside client container", err=True, fg=c.GREEN)
    os.execvp(cmd[0], cmd)


@app.command()
def list_services():
    """List the services which have been running.

    Only the services for which /log/current exists are shown.
    """
    _check_containers_running()
    typer.secho("Known services:", err=True)
    for service in _list_services():
        typer.secho(f"* {service}", err=True)


@app.command()
def runsvctrl(command: str, pattern: str):
    """Execute runsvctrl inside the server container."""
    _check_containers_running()
    cmd = _build_docker_cmd("server", cwd="/home/dirac/ServerInstallDIR/diracos/runit")
    services = fnmatch.filter(_list_services(), pattern)
    if not services:
        typer.secho(f"No services match {pattern!r}", fg=c.RED)
        raise typer.Exit(code=1)
    cmd += ["runsvctrl", command] + services
    os.execvp(cmd[0], cmd)


@app.command()
def logs(pattern: str = "*", lines: int = 10, follow: bool = True):
    """Show DIRAC's logs from the service container.

    For services matching [--pattern] show the most recent [--lines] from the
    logs. If [--follow] is True, continiously stream the logs.
    """
    _check_containers_running()
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


@contextmanager
def _gen_docker_compose(modules):
    input_fn = Path(__file__).parent / "tests/CI/docker-compose.yml"
    docker_compose = yaml.safe_load(input_fn.read_text())
    volumes = [
        f"{path}:/home/dirac/LocalRepo/ALTERNATIVE_MODULES/{name}"
        for name, path in modules.items()
    ]
    volumes += [
        f"{path}:/home/dirac/LocalRepo/TestCode/{name}"
        for name, path in modules.items()
    ]
    # Copies are needed
    docker_compose["services"]["dirac-server"]["volumes"] = volumes[:]
    docker_compose["services"]["dirac-client"]["volumes"] = volumes[:]
    with tempfile.TemporaryDirectory() as tmpdir:
        prefix = "ci"
        output_fn = Path(tmpdir) / prefix / "docker-compose.yml"
        output_fn.parent.mkdir()
        output_fn.write_text(yaml.safe_dump(docker_compose, sort_keys=False))
        yield output_fn


def _check_containers_running(*, is_up=True):
    with _gen_docker_compose(DEFAULT_MODULES) as docker_compose_fn:
        running_containers = subprocess.run(
            ["docker-compose", "-f", docker_compose_fn, "ps", "-q", "-a"],
            stdout=subprocess.PIPE,
            check=True,
            text=True,
        ).stdout.split("\n")
    if is_up:
        if not any(running_containers):
            typer.secho(
                f"No running containers found, environment must be prepared first!",
                err=True,
                fg=c.RED,
            )
            raise typer.Exit(code=1)
    else:
        if any(running_containers):
            typer.secho(
                f"Running instance already found, it must be destroyed first!",
                err=True,
                fg=c.RED,
            )
            raise typer.Exit(code=1)


def _find_dirac_release_and_branch():
    # Start by looking for the GitHub/GitLab environment variables
    ref = os.environ.get("CI_COMMIT_REF_NAME", os.environ.get("GITHUB_REF"))
    if ref == "refs/heads/integration":
        return "integration", ""
    ref = os.environ.get(
        "CI_MERGE_REQUEST_TARGET_BRANCH_NAME", os.environ.get("GITHUB_BASE_REF")
    )
    if ref == "integration":
        return "integration", ""

    repo = git.Repo(os.getcwd())
    # Try to make sure the upstream remote is up to date
    try:
        upstream = repo.remote("upstream")
    except ValueError:
        typer.secho("No upstream remote found, adding", err=True, fg=c.YELLOW )
        upstream = repo.create_remote(
            "upstream", "https://github.com/DIRACGrid/DIRAC.git"
        )
    try:
        upstream.fetch()
    except Exception:
        typer.secho("Failed to fetch from remote 'upstream'", err=True, fg=c.YELLOW)
    # Find the most recent tag on the current branch
    version = Version(
        repo.git.describe(
            dirty=True,
            tags=True,
            long=True,
            match="*[0-9]*",
            exclude=["v[0-9]r*", "v[0-9][0-9]r*"],
        ).split("-")[0]
    )
    # See if there is a remote branch named "rel-vXrY"
    version_branch = f"rel-v{version.major}r{version.minor}"
    try:
        upstream.refs[version_branch]
    except IndexError:
        typer.secho(
            f"Failed to find branch for {version_branch}, defaulting to integration",
            err=True,
            fg=c.YELLOW,
        )
        return "integration", ""
    else:
        return "", f"v{version.major}r{version.minor}"


def _make_env(flags):
    env = os.environ.copy()
    env["DIRAC_UID"] = str(os.getuid())
    env["DIRAC_GID"] = str(os.getgid())
    env["HOST_OS"] = flags.pop("HOST_OS", DEFAULT_HOST_OS)
    env["CI_REGISTRY_IMAGE"] = flags.pop("CI_REGISTRY_IMAGE", "diracgrid")
    env["MYSQL_VER"] = flags.pop("MYSQL_VER", DEFAULT_MYSQL_VER)
    env["ES_VER"] = flags.pop("ES_VER", DEFAULT_ES_VER)
    return env


def _dict_to_shell(variables):
    lines = []
    for name, value in variables.items():
        if value is None:
            continue
        elif isinstance(value, list):
            lines += [f"declare -a {name}"]
            lines += [f"{name}+=({shlex.quote(v)})" for v in value]
        elif isinstance(value, bool):
            lines += [f"export {name}={'Yes' if value else 'No'}"]
        elif isinstance(value, str):
            lines += [f"export {name}={shlex.quote(value)}"]
        else:
            raise NotImplementedError(name, value, type(value))
    return "\n".join(lines)


def _make_config(modules, flags, release_var, editable):
    config = {
        "DEBUG": "True",
        # MYSQL Settings
        "DB_USER": DB_USER,
        "DB_PASSWORD": DB_PASSWORD,
        "DB_ROOTUSER": DB_ROOTUSER,
        "DB_ROOTPWD": DB_ROOTPWD,
        "DB_HOST": DB_HOST,
        "DB_PORT": DB_PORT,
        # ElasticSearch settings
        "NoSQLDB_HOST": "elasticsearch",
        "NoSQLDB_PORT": "9200",
        # Hostnames
        "SERVER_HOST": "server",
        "CLIENT_HOST": "client",
        # Test specific variables
        "WORKSPACE": "/home/dirac",
    }

    if editable:
        config["PIP_INSTALL_EXTRA_ARGS"] = "-e"

    for module_name, module_path in modules.items():
        module_ci_config_path = module_path / "tests/.dirac-ci-config.yaml"
        if not module_ci_config_path.exists():
            continue
        module_ci_config = yaml.safe_load(module_ci_config_path.read_text())
        config |= module_ci_config["config"]
    config["DIRAC_CI_SETUP_SCRIPT"] = "/home/dirac/LocalRepo/TestCode/" + config["DIRAC_CI_SETUP_SCRIPT"]

    # This can likely be removed after the Python 3 migration
    if release_var:
        config |= dict([release_var.split("=", 1)])
    else:
        config["DIRAC_RELEASE"], config["DIRACBRANCH"] = _find_dirac_release_and_branch()

    for key, default_value in FEATURE_VARIABLES.items():
        config[key] = flags.pop(key, default_value)
    config["TESTREPO"] = [
        f"/home/dirac/LocalRepo/TestCode/{name}" for name in modules
    ]
    config["ALTERNATIVE_MODULES"] = [
        f"/home/dirac/LocalRepo/ALTERNATIVE_MODULES/{name}" for name in modules
    ]
    if not config["USE_PYTHON3"]:
        config["ALTERNATIVE_MODULES"] = [
            f"{x}/src/{Path(x).name}" for x in config["ALTERNATIVE_MODULES"]
        ]

    # Exit with an error if there are unused feature flags remaining
    if flags:
        typer.secho(f"Unrecognised feature flags {flags!r}", err=True, fg=c.RED)
        raise typer.Exit(code=1)

    return _dict_to_shell(config)


def _build_docker_cmd(container_name, *, user="dirac", cwd="/home/dirac", tty=True):
    cmd = ["docker", "exec"]
    if tty:
        if sys.stdout.isatty():
            cmd += ["-it"]
        else:
            typer.secho(
                'Not passing "-it" to docker as stdout is not a tty',
                err=True,
                fg=c.YELLOW,
            )
    cmd += [
        "-e=TERM=xterm-color",
        "-e=INSTALLROOT=/home/dirac",
        f"-e=INSTALLTYPE={container_name}",
        f"-u={user}",
        f"-w={cwd}",
        container_name,
    ]
    return cmd


def _list_services():
    cmd = _build_docker_cmd("server")
    cmd += [
        "bash",
        "-c",
        'cd ServerInstallDIR/diracos/runit/ && for fn in */*/log/current; do echo "$(dirname "$(dirname "$fn")")"; done',
    ]
    ret = subprocess.run(cmd, check=False, stdout=subprocess.PIPE, text=True)
    if ret.returncode:
        typer.secho("Failed to find list of available services", err=True, fg=c.RED)
        typer.secho(f"stdout was: {ret.stdout!r}", err=True)
        typer.secho(f"stderr was: {ret.stderr!r}", err=True)
        raise typer.Exit(1)
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
