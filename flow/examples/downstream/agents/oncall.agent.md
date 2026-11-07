---
apiVersion: saige/v1
name: oncall
version: 1.0.0
description: helps the on-call engineer
model:
  use: house/house-1
tools:
  registry:
    - lookup_runbook
limits:
  budget:
    max_cost: 1
    max_requests: 4
    max_tokens: 4096
---
Help the on-call engineer.
