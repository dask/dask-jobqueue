#!/usr/bin/env bash

function jobqueue_before_install {
  true  # Pass
}

function jobqueue_install {
  which python
  pip install --no-deps -e .
}

function jobqueue_script {
  pytest --verbose
}

function jobqueue_after_script {
  echo "Done."
}
