#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""
Pure Python, zero dependencies automations for AWS Glue Project.

Automation script to run test in Glue Docker Container.

This script should ONLY depends on standard library. No third party library
include boto3 should be used.

**Dependencies**

- AWS CodeCommit Repository
- AWS CodeBuild Project
- AWS S3 Bucket to store source code and dependency artifacts
- AWS CodeArtifact to store Glue Python Library

You HAVE TO have AWS CLI installed in the runtime.

Ref:

- https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/
"""

# ------------------------------------------------------------------------------
# Import Python library
# ------------------------------------------------------------------------------
__ACT_01_IMPORT_LIBRARY = None

# Standard library
from typing import Tuple, List, Dict, Callable, Optional, Any

import os
import re
import sys
import json
import glob
import shutil
import logging
import subprocess
import dataclasses
from pathlib import Path
from datetime import datetime
from functools import wraps

# Optional Third Party
try:
    from boto_session_manager import BotoSesManager, AwsServiceEnum
    from s3pathlib import S3Path, context

    HAS_S3PATHLIB = True
except ImportError:
    HAS_S3PATHLIB = False

# ------------------------------------------------------------------------------
# Configure logger
# ------------------------------------------------------------------------------
__ACT_02_CONFIGURE_LOGGER = None

logger = logging.getLogger("bot")
logger.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(stream=sys.stdout)
stream_handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "[User] %(message)s",
    datefmt="%Y/%m/%d %H:%m:%S",
)
stream_handler.setFormatter(formatter)

logger.addHandler(stream_handler)


def info(msg: str):
    logger.info(f"| {msg}")


def error(msg: str):
    logger.error(f"| {msg}")


# visual printer
def print_header1(msg: str):
    msg = f" {msg} "
    bar = "=" * 10
    logger.info(f"#{bar}{msg:=<70}")


def print_header2(msg: str):
    msg = f" {msg} "
    bar = "-" * 10
    logger.info(f"#{bar}{msg:-<70}")


def print_header3(msg: str):
    msg = f" {msg} "
    bar = "~" * 10
    logger.info(f"#{bar}{msg:~<70}")


def decohints(decorator: Callable) -> Callable:
    return decorator


def print_header(
    start_msg: str = "Start",
    error_msg: str = "Error, elapsed = {elapsed} sec",
    end_msg: str = "End, elapsed = {elapsed} sec",
    printer: Callable = print_header1,
):
    """
    Pretty print visual bar when a function start, error, end.

    The func you want to decorate has to have an option arg:
    ``vp: Callable=None``
    """

    @decohints
    def deco(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if "vp" in kwargs:
                vp = kwargs.pop("vp")
            else:
                vp = printer
            st = datetime.utcnow()
            vp(start_msg.format(**kwargs))
            logger.info("|")
            try:
                result = func(*args, **kwargs)
            except Exception as e:
                et = datetime.utcnow()
                elapsed = int((et - st).total_seconds())
                logger.info("|")
                vp(error_msg.format(elapsed=elapsed))
                raise e
            et = datetime.utcnow()
            elapsed = int((et - st).total_seconds())
            logger.info("|")
            vp(end_msg.format(elapsed=elapsed))
            return result

        return wrapper

    return deco


# ------------------------------------------------------------------------------
# Resolve important path
# ------------------------------------------------------------------------------
__ACT_03_RESOLVE_IMPORTANT_PATH = None

dir_home = Path.home()
dir_project_root = Path(__file__).absolute().parent.parent

dir_venv = dir_project_root / ".venv"
dir_venv_bin = dir_venv / "bin"
path_python = dir_venv_bin / "python"
path_pip = dir_venv_bin / "pip"
path_twine = dir_venv_bin / "twine"

path_pygitrepo_config_json = dir_project_root / "glue-automation-config.json"
path_glue_etl_scripts_json = dir_project_root / "glue-etl-scripts.json"

path_setup_py = dir_project_root / "setup.py"
path_main_requirements = dir_project_root / "requirements.txt"
path_dev_requirements = dir_project_root / "requirements-dev.txt"
path_test_requirements = dir_project_root / "requirements-test.txt"
path_glue_requirements = dir_project_root / "requirements-glue.txt"

dir_dist = dir_project_root / "dist"
dir_build = dir_project_root / "build"
dir_build_site_packages = dir_build / "site-packages"
dir_tests = dir_project_root / "tests"
dir_htmlcov = dir_project_root / "htmlcov"
dir_pytest_cache = dir_project_root / ".pytest_cache"

dir_sphinx_doc = dir_project_root / "docs"
dir_sphinx_doc_source = dir_sphinx_doc / "source"
dir_sphinx_doc_source_conf_py = dir_sphinx_doc_source / "conf.py"
dir_sphinx_doc_build = dir_sphinx_doc / "build"
dir_sphinx_doc_build_html = dir_sphinx_doc_build / "html"
path_sphinx_doc_build_html_index = dir_sphinx_doc_build_html / "README.html"

# ------------------------------------------------------------------------------
# Parse pygitrepo-config.json
# ------------------------------------------------------------------------------
__ACT_04_PYGITREPO_CONFIG_JSON = None


def strip_comment_line_with_symbol(line, start):
    """
    Strip comments from line string.
    """
    parts = line.split(start)
    counts = [len(re.findall(r'(?:^|[^"\\]|(?:\\\\|\\")+)(")', part)) for part in parts]
    total = 0
    for nr, count in enumerate(counts):
        total += count
        if total % 2 == 0:
            return start.join(parts[: nr + 1]).rstrip()
    else:  # pragma: no cover
        return line.rstrip()


def strip_comments(string, comment_symbols=frozenset(("#", "//"))):
    """
    Strip comments from json string.
    :param string: A string containing json with comments started by comment_symbols.
    :param comment_symbols: Iterable of symbols that start a line comment (default # or //).
    :return: The string with the comments removed.
    """
    lines = string.splitlines()
    for k in range(len(lines)):
        for symbol in comment_symbols:
            lines[k] = strip_comment_line_with_symbol(lines[k], start=symbol)
    return "\n".join(lines)


def read_text(abspath, encoding="utf-8") -> str:
    """
    Read string from a file.
    :type abspath: str
    :type encoding: str
    :rtype: str
    """
    with open(abspath, "rb") as f:
        return f.read().decode(encoding)


def read_json(abspath: str) -> Any:
    return json.loads(strip_comments(read_text(abspath)))


def no_slash(s: str) -> str:
    """
    ensure s3 key ends WITHOUT "/"
    """
    return s[:-1] if s.endswith("/") else s


@dataclasses.dataclass
class PyGitRepoConfig:
    PACKAGE_NAME: str = ""
    PYTHON_VERSION: str = ""
    AWS_PROFILE: str = ""
    AWS_GLUE_ARTIFACT_S3_BUCKET: str = ""
    AWS_GLUE_ARTIFACT_S3_PREFIX: str = ""
    AWS_CODE_ARTIFACT_DOMAIN: str = ""
    AWS_CODE_ARTIFACT_REPO: str = ""

    @classmethod
    def from_json(cls, path: str) -> "PyGitRepoConfig":
        raw_config_data = read_json(path)
        config_data = {
            k: v for k, v in raw_config_data.items() if not k.startswith("_")
        }
        return cls(**config_data)

    def __hash__(self):
        return hash(1)

    @property
    def package_name_slug(self) -> str:
        return self.PACKAGE_NAME.replace("_", "-")

    @property
    def package_version(self) -> str:
        sys.path.append(os.path.join(dir_project_root, self.PACKAGE_NAME))
        from _version import __version__

        return __version__

    @property
    def py_ver_major(self) -> int:
        return int(self.PYTHON_VERSION.split(".")[0])

    @property
    def py_ver_minor(self) -> int:
        return int(self.PYTHON_VERSION.split(".")[1])

    @property
    def py_ver_micro(self) -> int:
        return int(self.PYTHON_VERSION.split(".")[2])

    @property
    def py_ver_major_minor(self) -> str:
        return f"{self.py_ver_major}.{self.py_ver_major}"

    @property
    def python_runtime(self) -> str:
        return f"python{self.py_ver_major}.{self.py_ver_minor}"

    @property
    def aws_glue_artifact_s3_uri(self) -> str:
        """
        It won't ends with "/"
        """
        prefix = no_slash(self.AWS_GLUE_ARTIFACT_S3_PREFIX)
        return f"s3://{self.AWS_GLUE_ARTIFACT_S3_BUCKET}/{prefix}/artifacts"

    @property
    def aws_glue_deployment_s3_uri(self) -> str:
        """
        It won't ends with "/"
        """
        prefix = no_slash(self.AWS_GLUE_ARTIFACT_S3_PREFIX)
        return f"s3://{self.AWS_GLUE_ARTIFACT_S3_BUCKET}/{prefix}/deployment"


pygitrepo_config = PyGitRepoConfig.from_json(str(path_pygitrepo_config_json))

dir_venv_site_packages = (
    dir_venv
    / "lib"
    / pygitrepo_config.python_runtime
    / "site-packages"
)

# ------------------------------------------------------------------------------
# Parse glue-etl-scripts.json
# ------------------------------------------------------------------------------
__ACT_04_GLUE_ETL_SCRIPTS_CONFIG_JSON = None


@dataclasses.dataclass
class GlueETLScript:
    """
    Glue ETL Script data container class.
    """
    name: str = ""
    version: str = ""

    @property
    def job_name(self) -> str:
        return self.name.rstrip(".py")


def _parse_glue_etl_scripts_json() -> Dict[str, GlueETLScript]:
    glue_etl_scripts = dict()
    if path_glue_etl_scripts_json.exists():
        for name, dct in read_json(f"{path_glue_etl_scripts_json}").items():
            glue_etl_script = GlueETLScript(
                name=name,
                version=dct["version"],
            )
            path_glue_etl_script = dir_project_root / glue_etl_script.name
            if path_glue_etl_script.exists():
                glue_etl_scripts[name] = glue_etl_script
            else:
                error(
                    f"glue ETL script {glue_etl_script.name!r} "
                    f"is defined but not exists!"
                )
    return glue_etl_scripts


# ------------------------------------------------------------------------------
# detect current runtime
# ------------------------------------------------------------------------------
_ACT_5_DETECT_CURRENT_RUNTIME = None


def _append_aws_profile_args(args: List[str]):
    """
    Append ``--profile ${AWS_PROFILE}`` to aws cli args if needed.
    """
    if pygitrepo_config.AWS_PROFILE:
        args.extend(["--profile", pygitrepo_config.AWS_PROFILE])


def get_aws_account_id_from_aws_cli() -> str:
    """
    This function works in:

    - local laptop
    - AWS Cloud9
    """
    if not (
        IS_LOCAL
        or IS_CLOUD9
    ):
        raise RuntimeError
    args = ["aws", "sts", "get-caller-identity"]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    output = subprocess.run(
        args,
        capture_output=True,
        check=True,
    ).stdout.decode("utf-8")
    aws_account_id = json.loads(output)["Account"]
    return aws_account_id


def get_aws_region_from_aws_cli() -> str:
    """
    This function works in:

    - local laptop
    - AWS Cloud9
    """
    if not (
        IS_LOCAL
        or IS_CLOUD9
    ):
        raise RuntimeError
    args = ["aws", "configure", "get", "region"]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    aws_region = (
        subprocess.run(
            args,
            capture_output=True,
            check=True,
        )
        .stdout.decode("utf-8")
        .strip()
    )
    return aws_region


def get_aws_key_pair() -> Tuple[str, str]:
    """
    This function works in:

    - local laptop
    """
    if not (
        IS_LOCAL
    ):
        raise RuntimeError
    args = ["aws", "configure", "get", "aws_access_key_id"]
    _append_aws_profile_args(args)
    aws_access_key_id = (
        subprocess.run(
            args,
            capture_output=True,
            check=True,
        )
        .stdout.decode("utf-8")
        .strip()
    )

    args = ["aws", "configure", "get", "aws_secret_access_key"]
    _append_aws_profile_args(args)
    aws_secret_access_key = (
        subprocess.run(
            args,
            capture_output=True,
            check=True,
        )
        .stdout.decode("utf-8")
        .strip()
    )
    return aws_access_key_id, aws_secret_access_key


def get_git_branch_from_git_cli() -> str:
    """
    Use ``git`` CLI to get the current git branch.
    """
    res = subprocess.run(
        ["git", "branch", "--show-current"], capture_output=True, check=True
    )
    return res.stdout.decode("utf-8").strip()


def get_git_commit_id_from_git_cli() -> str:
    """
    Use ``git`` CIL to get current git commit id.
    """
    res = subprocess.run(
        ["git", "rev-parse", "HEAD"], capture_output=True, check=True
    )
    return res.stdout.decode("utf-8").strip()


IS_LOCAL = False
IS_CLOUD9 = False
IS_CODEBUILD = False

CURRENT_RUNTIME = None

if "C9_PROJECT" in os.environ:
    IS_CLOUD9 = True
# identify if it is codebuild runtime
# ref: https://docs.aws.amazon.com/codebuild/latest/userguide/build-env-ref-env-vars.html
elif "CODEBUILD_BUILD_ID" in os.environ:
    IS_CODEBUILD = True
else:
    IS_LOCAL = True

if IS_CODEBUILD:
    _CODEBUILD_BUILD_ARN = os.environ["CODEBUILD_BUILD_ARN"]
    _parts = _CODEBUILD_BUILD_ARN.split(":")
    AWS_ACCOUNT_ID = _parts[4]
    AWS_REGION = _parts[3]
else:
    AWS_ACCOUNT_ID = get_aws_account_id_from_aws_cli()
    AWS_REGION = get_aws_region_from_aws_cli()

# ------------------------------------------------------------------------------
# Setup virtual environment
# ------------------------------------------------------------------------------
__ACT_05_SETUP_VIRTUALENV = None


def _set_pyenv_version(target_ver: str):
    """
    Set pyenv global python to desired version.

    :param target_ver: example, 3.6, 3.7, 3.8, ...
    """
    dir_pyenv_versions = dir_home / ".pyenv" / "versions"
    path_pyenv_current_version = dir_home / ".pyenv_current_version"

    python_full_version = None

    for p in dir_pyenv_versions.iterdir():
        if p.name.startswith(target_ver):
            python_full_version = p.name

    if python_full_version is None:
        print(f"Error: cannot find python{target_ver} interpreter in pyenv!")
        sys.exit(1)

    subprocess.call(["pyenv", "global", python_full_version])
    path_pyenv_current_version.write_text(python_full_version)

    subprocess.call(["pyenv", "rehash"])


# def set_pyenv_version():
#     visual_print_1("set pyenv version")
#     _set_pyenv_version(pygitrepo_config.py_ver_major_minor)


@print_header(start_msg="Create Python virtual environment")
def _venv_up():
    if not dir_venv.exists():
        info(f"run 'virtualenv -p {pygitrepo_config.python_runtime} {dir_venv}' command ...")
        subprocess.run(
            ["virtualenv", "-p", pygitrepo_config.python_runtime, f"{dir_venv}"],
            check=True
        )
    else:
        info(f"virtualenv {dir_venv!r} already exists! do nothing.")


@print_header(start_msg="Remove Python virtual environment")
def _venv_remove():
    if dir_venv.exists():
        info(f"run 'rm -r {dir_venv}' command ...")
        subprocess.run(
            ["rm", "-r", f"{dir_venv}"],
            check=True
        )
        info("done")
    else:
        info(f"virtualenv {dir_venv!r} NOT exists! do nothing.")


# ------------------------------------------------------------------------------
# Install dependencies
# ------------------------------------------------------------------------------
__ACT_06_INSTALL_DEPENDENCIES = None


@print_header(start_msg="Install Core Dependencies")
def _pip_install():
    # install core dependencies
    subprocess.run(
        [f"{path_pip}", "install", "-e", f"{dir_project_root}"],
        check=True
    )
    # install awsglue lib 3.0 from GitHub
    subprocess.run(
        [
            f"{path_pip}", "install",
            "git+https://github.com/awslabs/aws-glue-libs.git@master",
        ]
    )


@print_header(start_msg="Install Dev Dependencies")
def _pip_install_dev():
    # install dev dependencies
    subprocess.run(
        [f"{path_pip}", "install", "-r", f"{path_dev_requirements}"],
        check=True
    )


@print_header(start_msg="Install Test Dependencies")
def _pip_install_test():
    # install test dependencies
    subprocess.run(
        [f"{path_pip}", "install", "-r", f"{path_test_requirements}"],
        check=True
    )


@print_header(start_msg="Install Glue Dependencies")
def _pip_install_glue():
    # install glue dependencies
    subprocess.run(
        [f"{path_pip}", "install", "-r", f"{path_glue_requirements}"],
        check=True
    )


@print_header(start_msg="Install All Dependencies")
def _pip_install_all():
    # install core dependencies
    subprocess.run(
        [f"{path_pip}", "install", "-e", f"{dir_project_root}"],
        check=True
    )
    # install awsglue lib 3.0 from GitHub
    subprocess.run(
        [
            f"{path_pip}", "install",
            "git+https://github.com/awslabs/aws-glue-libs.git@master",
        ]
    )
    # install glue runtime built-in dependencies
    # it provides auto-complete, type-hint for local development
    subprocess.run(
        [
            f"{path_pip}", "install", "-r", f"{dir_project_root}/requirements-glue.txt",
        ],
        check=True,
    )
    subprocess.run(
        [
            f"{path_pip}", "install", "-r", f"{dir_project_root}/requirements-dev.txt",
        ],
        check=True,
    )
    subprocess.run(
        [
            f"{path_pip}", "install", "-r", f"{dir_project_root}/requirements-doc.txt",
        ],
        check=True,
    )
    subprocess.run(
        [
            f"{path_pip}", "install", "-r", f"{dir_project_root}/requirements-test.txt",
        ],
        check=True,
    )


# ------------------------------------------------------------------------------
# Run test in glue docker container
# ------------------------------------------------------------------------------
__ACT_07_RUN_TEST = None

IMAGE_REPO = "public.ecr.aws/glue/aws-glue-libs"
IMAGE_TAG = "glue_libs_3.0.0_image_01"


def _is_image_pulled(
    image_repo: str,
    image_tag: str,
) -> bool:
    res = subprocess.run(
        [
            "docker", "image", "ls"
        ],
        capture_output=True,
        check=True,
    )
    found = False
    for line in res.stdout.decode("utf-8").split("\n"):
        if line.startswith(image_repo) and image_tag in line:
            found = True
    return found


def _login_docker_to_aws_ecr_func(
    aws_account_id: str,
    aws_region: str,
):
    """
    Use docker client to login AWS ECR. Then you can pull / push image
    on behalf of your IAM Role.

    If you are using docker cli on AWS Cloud9, please follow this document
    https://docs.aws.amazon.com/AmazonECS/latest/developerguide/create-container-image.html
    and search the "Installing Docker on Amazon Linux 2" section.
    """
    info("run 'aws ecr get-login-password ...' command")
    args = ["aws", "ecr", "get-login-password", "--region", aws_region]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    pipe = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
    )

    info(
        f"run 'docker login --username AWS --password-stdin "
        f"{aws_account_id}.dkr.ecr.{aws_region}.amazonaws.com' command"
    )
    res = subprocess.run(
        [
            "docker",
            "login",
            "--username",
            "AWS",
            "--password-stdin",
            f"{aws_account_id}.dkr.ecr.{aws_region}.amazonaws.com",
        ],
        stdin=pipe.stdout,
        capture_output=True,
        check=True,
    )
    info(res.stdout.decode("utf-8").strip())


@print_header(start_msg="Login docker to AWS ECR")
def _login_docker_to_aws_ecr():
    _login_docker_to_aws_ecr_func(AWS_ACCOUNT_ID, AWS_REGION)


def _login_docker_to_aws_ecr_if_image_not_pulled():
    if _is_image_pulled(IMAGE_REPO, IMAGE_TAG) is False:
        _login_docker_to_aws_ecr()


def get_access_key_pair_from_env_var() -> Tuple[str, str]:
    """
    This function only works in AWS CodeBuild.
    We pass in AWS Credential explicitly into the AWS Glue container.
    """
    aws_access_key_id = os.environ["aws_access_key_id"]
    aws_secret_access_key = os.environ["aws_secret_access_key"]
    return aws_access_key_id, aws_secret_access_key


def _run_test_in_glue_docker_container(test_path: str):
    """
    Run pytest in glue docker container.

    Ref:

    - https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/

    :param test_path: the pytest test case location. can be a directory,
        a specific ``test_*.py`` file
    """
    args = [
        "docker",
        "run",
        "-v",
        f"{dir_home}/.aws:/home/glue_user/.aws",
        "-v",
        f"{dir_project_root}/:/home/glue_user/workspace/",
        "-v",
        f"{dir_venv_site_packages}/:/home/glue_user/workspace/extra_python_path/",
        "-e",
        "IS_CONTAINER=y",
        "-e",
        f"AWS_REGION={AWS_REGION}",
    ]

    # For EC2, and Local laptop, glue docker container can use the AWS CLI
    # configuration from the ~/.aws file
    # For CodeBuild runtime, there's no ~/.aws file, you have to explicitly
    # pass in the AWS Credential
    if IS_CODEBUILD:
        aws_access_key_id, aws_secret_access_key = get_access_key_pair_from_env_var()
        args.extend(
            [
                "-e",
                f"AWS_ACCESS_KEY_ID={aws_access_key_id}",
                "-e",
                f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}",
            ]
        )
    elif IS_CLOUD9:
        pass
    elif IS_LOCAL:
        aws_access_key_id, aws_secret_access_key = get_aws_key_pair()
        args.extend(
            [
                "-e",
                f"AWS_ACCESS_KEY_ID={aws_access_key_id}",
                "-e",
                f"AWS_SECRET_ACCESS_KEY={aws_secret_access_key}",
            ]
        )

    args.extend(
        [
            "-e",
            "PYTHONPATH=$PYTHONPATH:/home/glue_user/workspace/extra_python_path/",
            "-e",
            "DISABLE_SSL=true",
            "--rm",
            "-p",
            "4041:4040",  # avoid conflict to jupyter lab
            "-p",
            "18081:18080",  # avoid conflict to jupyter lab
            "--name",
            "glue_pytest",
            "public.ecr.aws/glue/aws-glue-libs:glue_libs_3.0.0_image_01",
            "-c",
            f"python3 -m pytest /home/glue_user/workspace/{test_path} -s -p no:cacheprovider --disable-warnings --cov={pygitrepo_config.PACKAGE_NAME}",
        ]
    )
    subprocess.run(args)


@print_header(start_msg="Run all test in glue docker container")
def _run_all_test_in_glue_docker_container():
    _run_test_in_glue_docker_container(test_path="tests")


@print_header(start_msg="Run Glue ETL Script in glue docker container")
def _run_etl_job_in_glue_docker_container(script_path: str):
    """
    Run GLUE ETL script in glue docker container.

    Ref:

    - https://aws.amazon.com/blogs/big-data/develop-and-test-aws-glue-version-3-0-jobs-locally-using-a-docker-container/

    :param script_path: a Glue ETL script ``main_*.py``
    """
    args = [
        "docker",
        "run",
        "-v",
        f"{dir_home}/.aws:/home/glue_user/.aws",
        "-v",
        f"{dir_project_root}/:/home/glue_user/workspace/",
        "-v",
        f"{dir_venv_site_packages}/:/home/glue_user/workspace/extra_python_path/",
        "-e",
        "IS_CONTAINER=y",
        "-e",
        f"AWS_REGION={AWS_REGION}",
    ]
    if IS_LOCAL:
        if pygitrepo_config.AWS_PROFILE:
            args.extend(["-e", f"AWS_PROFILE={pygitrepo_config.AWS_PROFILE}"])

    args.extend([
        "-e",
        "DISABLE_SSL=true",
        "--rm",
        "-p",
        "4041:4040",
        "-p",
        "18081:18080",
        "--name",
        "glue_spark_submit",
        "public.ecr.aws/glue/aws-glue-libs:glue_libs_3.0.0_image_01",
        "spark-submit",
        f"/home/glue_user/workspace/{script_path}",
    ])
    subprocess.run(args)


@print_header(start_msg="Start Jupyter Notebook")
def _start_jupyter_lab():
    """
    JUPYTER_WORKSPACE_LOCATION=/local_path_to_workspace/jupyter_workspace/
$ docker run -it -v ~/.aws:/home/glue_user/.aws -v $JUPYTER_WORKSPACE_LOCATION:/home/glue_user/workspace/jupyter_workspace/ -e AWS_PROFILE=$PROFILE_NAME -e DISABLE_SSL=true --rm -p 4040:4040 -p 18080:18080 -p 8998:8998 -p 8888:8888 --name glue_jupyter_lab amazon/aws-glue-libs:glue_libs_3.0.0_image_01 /home/glue_user/jupyter/jupyter_start.sh
    """
    if not (
        IS_LOCAL
    ):
        raise RuntimeError("You can only start jupyter notebook on local laptop!")
    args = [
        "docker",
        "run",
        "-it",
        "-e",
        "IS_CONTAINER=y",
        "-v",
        f"{dir_home}/.aws:/home/glue_user/.aws",
        "-v",
        f"{dir_project_root}:/home/glue_user/workspace/jupyter_workspace/",
        "-v",
        f"{dir_venv_site_packages}/:/home/glue_user/workspace/extra_python_path/",
    ]
    if pygitrepo_config.AWS_PROFILE:
        args.extend(["-e", f"AWS_PROFILE={pygitrepo_config.AWS_PROFILE}"])

    args.extend([
        "-e",
        "DISABLE_SSL=true",
        "--rm",
        "-p",
        "4040:4040",
        "-p",
        "18080:18080",
        "-p",
        # jupyter notebook need additional port  binding
        "8998:8998",
        "-p",
        "8888:8888",
        "--name",
        "glue_jupyter_lab",
        "public.ecr.aws/glue/aws-glue-libs:glue_libs_3.0.0_image_01",
        "/home/glue_user/jupyter/jupyter_start.sh",
    ])
    cmd = " ".join(args)
    info("Run the following command in your terminal to start a local Glue Jupyter Lab:")
    info(f"  {cmd}")


# ------------------------------------------------------------------------------
# Build Artifacts
# ------------------------------------------------------------------------------
__ACT_08_BUILD_ARTIFACTS = None


def _prepare_dist():
    # clean up existing "dist" dir, if exists
    if dir_dist.exists():
        shutil.rmtree(f"{dir_dist}")

    dir_dist.mkdir(parents=True, exist_ok=True)


def _build_python_library():
    """
    build Glue Python Library artifact. It is basically the source code
    in tar file and wheel file.
    """
    subprocess.run(
        [
            f"{path_python}",
            f"{path_setup_py}",
            "sdist",
            "bdist_wheel",
            "--universal",
            "--dist-dir",
            f"{dir_dist}",
        ],
        cwd=f"{dir_project_root}",
        check=True,
    )


def _prepare_site_packages():
    # clean up existing "build/site-packages" dir, if exists
    if dir_build_site_packages.exists():
        shutil.rmtree(f"{dir_build_site_packages}")

    dir_build_site_packages.mkdir(parents=True, exist_ok=True)


def _build_site_packages():
    """
    build additional third party library dependencies layer for your
    Glue Python Library from requirements.txt
    """
    subprocess.run(
        [
            f"{path_pip}", "install",
            "-r", f"{dir_project_root}/requirements.txt",
            "-t", f"{dir_build_site_packages}",
        ],
        check=True,
    )
    # don't build if it is a zero dependency project
    if len(list(dir_build_site_packages.iterdir())) == 0:
        return

    cwd = os.getcwd()
    os.chdir(f"{dir_build_site_packages}")

    subprocess.run(
        [
            "zip",
            f"{dir_dist}/site-packages-{pygitrepo_config.package_version}.zip",
            "-r",
            "-9",
            "-q",
        ]
        + glob.glob("*")
        # ignore built-in glue runtime python libraries
        + [
            "-x",
            "pip*",
            "setuptools*",
            "twine*",
            "blac*",
            "_pytest*",
            "pytest*",
            "boto3*",
            "botocore*",
            "certifi*",
            "chardet*",
            "cycler*",
            "Cython*",
            "docutils*",
            "enum34*",
            "fsspec*",
            "idna*",
            "jmespath*",
            "joblib*",
            "kiwisolver*",
            "matplotlib*",
            "mpmath*",
            "numpy*",
            "pandas*",
            "patsy*",
            "pmdarima*",
            "ptvsd*",
            "pyarrow*",
            "pydevd*",
            "pyhocon*",
            "PyMySQL*",
            "pyparsing*",
            "pytz*",
            "dateutil*",
            "requests*",
            "s3fs*",
            "s3transfer*",
            "scikit-learn*",
            "scipy*",
            "six*",
            "statsmodels*",
            "subprocess32*",
            "sympy*",
            "tbats*",
            "urllib3*",
        ],
        check=True,
    )
    os.chdir(cwd)


@print_header(start_msg="Build Artifacts")
def _build_artifacts():
    """
    Build Python Artifacts for Glue Job.

    If it is a Glue Python Library:

    1. Python library source code.
    2. Python library dependencies, except those Glue runtime built-in libraries.
        such as boto3, pandas, pyspark.
    """
    _prepare_dist()
    _build_python_library()
    _prepare_site_packages()
    _build_site_packages()


# ------------------------------------------------------------------------------
# Publish artifacts
# ------------------------------------------------------------------------------
__ACT_09_PUBLISH_ARTIFACTS = None


def get_bsm() -> 'BotoSesManager':
    """
    Get boto session manager.
    """
    if IS_LOCAL:
        if pygitrepo_config.AWS_PROFILE:
            return BotoSesManager(
                region_name=AWS_REGION,
                profile_name=pygitrepo_config.AWS_PROFILE,
            )
        else:
            return BotoSesManager(
                region_name=AWS_REGION,
            )
    elif IS_CLOUD9 or IS_CODEBUILD:
        return BotoSesManager(region_name=AWS_REGION)
    else:  # pragma: no cover
        raise NotImplementedError


bsm: 'BotoSesManager'
if HAS_S3PATHLIB:
    bsm = get_bsm()
    context.attach_boto_session(bsm.boto_ses)


# # login private PyPI in AWS CodeArtifacts
def _login_aws_code_artifact_func(
    aws_account_id: str,
    aws_code_artifact_domain: str,
    aws_code_artifact_repo: str
):
    """
    Configure ``pip`` and ``twine`` to use AWS Code Artifact.

    Ref: https://docs.aws.amazon.com/codeartifact/latest/ug/using-python.html
    """
    args = [
        "aws",
        "codeartifact",
        "login",
        "--tool",
        "pip",
        "--domain",
        aws_code_artifact_domain,
        "--domain-owner",
        aws_account_id,
        "--repository",
        aws_code_artifact_repo,
    ]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    subprocess.run(
        args,
        check=True,
    )

    args = [
        "aws",
        "codeartifact",
        "login",
        "--tool",
        "twine",
        "--domain",
        aws_code_artifact_domain,
        "--domain-owner",
        aws_account_id,
        "--repository",
        aws_code_artifact_repo,
    ]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    subprocess.run(
        args,
        check=True,
    )


@print_header(start_msg="Login to AWS CodeArtifact")
def _login_aws_code_artifact():
    _login_aws_code_artifact_func(
        AWS_ACCOUNT_ID,
        pygitrepo_config.AWS_CODE_ARTIFACT_DOMAIN,
        pygitrepo_config.AWS_CODE_ARTIFACT_REPO,
    )


def _get_latest_published_version_in_aws_code_artifact(
    aws_code_artifact_domain: str,
    aws_code_artifact_repo: str,
    package_name: str,
) -> Optional[str]:
    """
    Given a python package name, find the latest published version in
    AWS CodeArtifact.
    """
    package_name_slug = package_name.replace("_", "-")
    args = [
        "aws",
        "codeartifact",
        "list-package-versions",
        "--domain",
        aws_code_artifact_domain,
        "--repository",
        aws_code_artifact_repo,
        "--package",
        package_name_slug,
        "--format",
        "pypi",
        "--max-items",
        "1",
    ]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    res = subprocess.run(
        args,
        capture_output=True,
    )
    if res.returncode == 0:
        output = res.stdout.decode("utf-8")
        latest_version = json.loads(output)["defaultDisplayVersion"]
        return latest_version
    else:
        return None


def _get_latest_published_version() -> Optional[str]:
    return _get_latest_published_version_in_aws_code_artifact(
        pygitrepo_config.AWS_CODE_ARTIFACT_DOMAIN,
        pygitrepo_config.AWS_CODE_ARTIFACT_REPO,
        pygitrepo_config.PACKAGE_NAME,
    )


def _is_version_already_published_in_aws_code_artifact(
    aws_code_artifact_domain: str,
    aws_code_artifact_repo: str,
    package_name: str,
    package_version: str,
) -> bool:
    """
    Given a python package name and a version, check if it is already published
    in AWS CodeArtifact.
    """
    package_name_slug = package_name.replace("_", "-")
    args = [
        "aws",
        "codeartifact",
        "describe-package-version",
        "--domain",
        aws_code_artifact_domain,
        "--repository",
        aws_code_artifact_repo,
        "--package",
        package_name_slug,
        "--package-version",
        package_version,
        "--format",
        "pypi",
    ]
    if IS_LOCAL:
        _append_aws_profile_args(args)
    res = subprocess.run(
        args,
        capture_output=True,
    )
    if res.returncode == 0:
        return True
    else:
        return False


def _is_version_already_published() -> bool:
    return _is_version_already_published_in_aws_code_artifact(
        pygitrepo_config.AWS_CODE_ARTIFACT_DOMAIN,
        pygitrepo_config.AWS_CODE_ARTIFACT_REPO,
        pygitrepo_config.PACKAGE_NAME,
        pygitrepo_config.package_version,
    )


@print_header(start_msg="Publish Glue Python Library to AWS S3")
def _publish_glue_python_library_to_s3():
    """
    Copy the ``dist`` folder to S3.
    """
    if _is_version_already_published():
        info(
            f"{pygitrepo_config.PACKAGE_NAME}=={pygitrepo_config.package_version} "
            f"is already published! do nothing."
        )
    else:
        info(
            f"{pygitrepo_config.PACKAGE_NAME}=={pygitrepo_config.package_version} "
            "is not published yet, ready to publish."
        )
        valid_filename = [
            f"{pygitrepo_config.PACKAGE_NAME}-{pygitrepo_config.package_version}.tar.gz",
            f"{pygitrepo_config.PACKAGE_NAME}-{pygitrepo_config.package_version}-py2.py3-none-any.whl",
            f"site-packages-{pygitrepo_config.package_version}.zip",
        ]
        target_s3_uri_prefix = (
            f"{pygitrepo_config.aws_glue_artifact_s3_uri}/"
            f"{pygitrepo_config.PACKAGE_NAME}/"
            f"{pygitrepo_config.package_version}"
        )
        file_list: List[Path] = list()
        for path in dir_dist.iterdir():
            if path.name in valid_filename:
                file_list.append(path)

        if len(file_list) == 0:
            error(f"found no distribution files in {dir_dist!r}")
            error(f"maybe you need to run 'make bud' command first?")
            return

        info(f"run 'aws s3 cp ... ' command")
        for path in file_list:
            if path.name in valid_filename:
                args = [
                    "aws",
                    "s3",
                    "cp",
                    f"{path}",
                    f"{target_s3_uri_prefix}/{path.name}",
                ]
                if IS_LOCAL:
                    _append_aws_profile_args(args)
                subprocess.run(
                    args,
                    check=True,
                )


@print_header(start_msg="Publish Glue Python Library to AWS CodeArtifact")
def _publish_glue_python_library_to_code_artifact():
    if _is_version_already_published():
        info(
            f"{pygitrepo_config.PACKAGE_NAME}=={pygitrepo_config.package_version} "
            f"is already published! do nothing."
        )
    else:
        info(
            f"{pygitrepo_config.PACKAGE_NAME}=={pygitrepo_config.package_version} "
            "is not published yet, ready to publish."
        )
        valid_filename = [
            f"{pygitrepo_config.PACKAGE_NAME}-{pygitrepo_config.package_version}.tar.gz",
            f"{pygitrepo_config.PACKAGE_NAME}-{pygitrepo_config.package_version}-py2.py3-none-any.whl",
        ]
        file_list: List[Path] = list()
        for path in dir_dist.iterdir():
            if path.name in valid_filename:
                file_list.append(path)

        if len(file_list) == 0:
            error(f"found no distribution files in {dir_dist!r}")
            error(f"maybe you need to run 'make bud' command first?")
            return

        args = [
            "twine",
            "upload",
            "--repository",
            "codeartifact",
        ]
        args.extend([f"{path}" for path in file_list])
        cmd = " ".join(args)
        info(f"run '{cmd}'")
        subprocess.run(args, check=True)


@print_header(start_msg="Publish Glue ETL Scripts to AWS S3")
def _publish_glue_etl_scripts_to_s3():
    glue_etl_scripts = _parse_glue_etl_scripts_json()
    has_etl_script = False
    for glue_etl_script in glue_etl_scripts.values():
        has_etl_script = True
        path_glue_etl_script = dir_project_root / glue_etl_script.name
        s3_uri = (
            f"{pygitrepo_config.aws_glue_artifact_s3_uri}/"
            f"{glue_etl_script.job_name}/"
            f"{glue_etl_script.job_name}-{glue_etl_script.version}.py"
        )
        s3path = S3Path.from_s3_uri(s3_uri)
        if s3path.exists():
            info(f"{s3path.uri!r} is already published! do nothing.")
        else:
            info(f"upload {glue_etl_script.name!r} to {s3path.uri!r}")
            s3path.upload_file(f"{path_glue_etl_script}")
            info(f"  preview at {s3path.console_url}")
    if has_etl_script is False:
        info("we don't have Glue ETL Scripts in this project! do nothing.")


@print_header(start_msg="Print helpful project information")
def _project_info():
    s3_console_url = (
        f"https://{AWS_REGION}.console.aws.amazon.com/s3/buckets/"
        f"{pygitrepo_config.AWS_GLUE_ARTIFACT_S3_BUCKET}?"
        f"prefix={no_slash(pygitrepo_config.AWS_GLUE_ARTIFACT_S3_PREFIX)}/"
        f"{pygitrepo_config.PACKAGE_NAME}/&region={AWS_REGION}"
    )
    info(f"- Artifacts S3 Bucket Console: {s3_console_url}")
    code_artifacts_console_url = (
        f"https://{AWS_REGION}.console.aws.amazon.com/codesuite/codeartifact/d/"
        f"{AWS_ACCOUNT_ID}/{pygitrepo_config.AWS_CODE_ARTIFACT_DOMAIN}/r/"
        f"{pygitrepo_config.AWS_CODE_ARTIFACT_REPO}/p/pypi/"
        f"{pygitrepo_config.package_name_slug}/versions?region={AWS_REGION}"
    )
    info(f"- AWS CodeArtifact Console: {code_artifacts_console_url}")


@print_header(start_msg="Build Sphinx Document")
def _build_doc():
    """
    :type config: RepoConfig
    """
    os.environ["PATH"] = f"{dir_venv_bin}" + os.pathsep + os.environ.get("PATH", "")
    os.environ["VIRTUAL_ENV"] = f"{dir_venv}"

    # self.req_doc(config, _dry_run=_dry_run, **kwargs)
    # self.pip_dev_install(config, _dry_run=_dry_run, **kwargs)
    shutil.rmtree(f"{dir_sphinx_doc_build}", ignore_errors=True)
    shutil.rmtree(f"{dir_sphinx_doc_build / pygitrepo_config.PACKAGE_NAME}", ignore_errors=True)

    # call sphinx make html command
    subprocess.call([
        "make",
        "-C",
        f"{dir_sphinx_doc}",
        "html",
    ])


@print_header(start_msg="View Sphinx Document")
def _view_doc():
    subprocess.call(["open", path_sphinx_doc_build_html_index])


# ------------------------------------------------------------------------------
# Automations
# ------------------------------------------------------------------------------
__ACT_10_AUTOMATIONS = None


def venv_up():
    _venv_up()


def venv_remove():
    _venv_remove()


def pip_install():
    _pip_install()


def pip_install_test():
    _pip_install_test()


def pip_install_dev():
    _pip_install_dev()


def pip_install_glue():
    _pip_install_glue()


def pip_install_all():
    _pip_install_all()


def login_ecr():
    _login_docker_to_aws_ecr_if_image_not_pulled()


def run_unit_test():
    _login_docker_to_aws_ecr_if_image_not_pulled()
    _run_all_test_in_glue_docker_container()


def run_single_test(test_path: str):
    _login_docker_to_aws_ecr_if_image_not_pulled()

    @print_header(start_msg=f"Run single test in {test_path}")
    def run_test():
        _run_test_in_glue_docker_container(test_path)

    run_test()


def run_etl_job(script_path: str):
    _login_docker_to_aws_ecr_if_image_not_pulled()
    _run_etl_job_in_glue_docker_container(script_path)


def start_jupyter_notebook():
    _start_jupyter_lab()


def build_artifacts():
    _build_artifacts()


def publish_artifacts():
    _publish_glue_python_library_to_s3()
    _login_aws_code_artifact()
    _publish_glue_python_library_to_code_artifact()
    _publish_glue_etl_scripts_to_s3()


def login_aws_code_artifact():
    _login_aws_code_artifact()


def project_info():
    _project_info()
