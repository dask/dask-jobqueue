#!/usr/bin/env bash

set -x

function jobqueue_before_install {
  # Nothing to go
  echo "Before install, nothing to do"
}

function jobqueue_install {
  echo "Install, nothing to do"
}

function jobqueue_script {
  echo "script: Run tests without enf"
}

function jobqueue_after_success {
    echo "Hurrah"
}
