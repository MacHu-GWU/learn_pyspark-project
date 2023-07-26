#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import subprocess
from pathlib import Path

dir_here = Path(__file__).absolute().parent
dir_project_root = dir_here.parent
dir_home = Path.home()

os.chdir(dir_project_root)

args = [
    "docker",
    "run",
    "-it",
    "--rm",
]
args.extend(["--name", "learn_pyspark"])
args.extend(["-v", f"{dir_project_root}:/home/jovyan"])
args.extend(["-v", f"{dir_home}/.aws:/home/jovyan/.aws"])
args.extend(["-p", "8888:8888"])
args.extend(["jupyter/pyspark-notebook:aarch64-spark-3.3.0"])
subprocess.run(args)
