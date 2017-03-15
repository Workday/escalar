#!/usr/bin/env bash
sbt +clean
sbt +publishSigned
sbt +sonatypeRelease