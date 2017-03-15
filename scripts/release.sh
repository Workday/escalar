#!/usr/bin/env bash
bash <(curl -s https://codecov.io/bash)
sbt clean
sbt publishSigned
sbt sonatypeRelease