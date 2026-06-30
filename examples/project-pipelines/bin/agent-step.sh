#!/bin/sh
printf 'project-pipelines-step-ready\n'
while IFS= read -r line; do
  printf 'project-pipelines-step:%s\n' "$line"
done
