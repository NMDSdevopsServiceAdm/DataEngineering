#!/bin/bash

if git diff --cached --name-only | grep -E '\.py$' | xargs grep -nE '\.shw\(' ; then
  echo "❌ ERROR: .show() detected in staged Python files."
  echo "Please remove or comment out Spark .show() calls before committing."
  exit 1
fi

exit 0
