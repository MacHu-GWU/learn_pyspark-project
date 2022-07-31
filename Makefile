# -*- coding: utf-8 -*-

help: ## ** Show this help message
	@perl -nle'print $& if m{^[a-zA-Z_-]+:.*?## .*$$}' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-40s\033[0m %s\n", $$1, $$2}'


up: ## ** Create Virtual Environment
	python3 ./bin/s01_venv_up.py


remove: ## ** Remove Virtual Environment
	python3 ./bin/s02_venv_remove.py


install: ## ** Install main dependencies and Package itself
	python3 ./bin/s03_pip_install.py


install-test: ## Install All Dependencies
	python3 ./bin/s04_pip_install_test.py


install-all: ## Install All Dependencies
	python3 ./bin/s05_pip_install_all.py


test-only: ## Run test without installing test dependencies
	python3 ./bin/s07_run_unit_test.py


test: install-all test-only ## ** Run Code Coverage test


lab: ## ** Run Glue dev jupyter lab
	python3 ./bin/s09_start_jupyter_lab.py


login-ecr: ## ** Login AWS ECR
	python3 ./bin/s14_login_ecr.py


login-ca: ## ** Login AWS CodeArtifact
	python3 ./bin/s15_login_codeartifact.py


build-doc: ## ** Publish code artifacts
	python3 ./bin/s21_build_doc.py


view-doc: ## ** Publish code artifacts
	python3 ./bin/s22_view_doc.py


info: ## ** Print helpful project information
	python3 ./bin/s13_project_info.py
