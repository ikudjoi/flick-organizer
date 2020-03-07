#!/usr/bin/env bash

# set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
VENV_DIR="${SCRIPT_DIR}/venv_tests"

# Create venv if it does not exists
if [ ! -d "${VENV_DIR}" ]; then
  python3.7 -m venv ${VENV_DIR}
fi

source ${VENV_DIR}/bin/activate
pip install -r requirements.txt
export LC_ALL=en_US.UTF-8
