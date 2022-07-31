#!/bin/bash

dir_project_root="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
dir_site_packages="${dir_project_root}/site-packages"

pip3.8 install -r requirements-notebook.txt -t "${dir_site_packages}"
