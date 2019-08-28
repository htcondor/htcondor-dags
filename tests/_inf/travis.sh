#!/usr/bin/env bash

set -e

pytest --cov

codecov -t 89120b28-41f0-4f87-bef9-400d269784b1
