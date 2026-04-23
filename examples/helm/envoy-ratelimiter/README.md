<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Envoy Rate Limiter Helm Chart

This Helm chart deploys the Envoy Rate Limiter service and a Redis instance on a Kubernetes cluster (designed for GKE).

## Overview

The Envoy Rate Limiter is a central service that coordinated rate limits across distributed clients. This is particularly useful for Apache Beam pipelines running on Dataflow to avoid overwhelming external APIs.

This chart installs:
-   Redis deployment and service.
-   Rate Limiter deployment and service (Internal Load Balancer).
-   Autoscaler (HPA) for the rate limiter.
-   PodMonitoring for Google Cloud Managed Service for Prometheus (optional).

## Prerequisites

-   A running Kubernetes cluster (e.g., GKE).
-   `kubectl` configured to connect to the cluster.
-   `helm` CLI installed.

## Installation

1.  **Clone the repository** (if you haven't already).
2.  **Navigate to the chart directory**:
    ```bash
    cd examples/helm/envoy-ratelimiter
    ```
3.  **Install the chart**:
    ```bash
    helm install envoy-ratelimiter . -n envoy-ratelimiter --create-namespace
    ```

## Configuration

You can override the default values by creating a custom `values.yaml` file or using `--set` flags.

Key parameters to consider:
-   `configYaml`: The actual rate limit rules. You **must** configure this for your specific use case.
-   `loadBalancerIP`: If you have a reserved static internal IP, provide it here.

See `values.yaml` for the full list of configurable parameters.

### Example custom values file (`custom-values.yaml`):

```yaml
configYaml: |
  domain: my_api
  descriptors:
    - key: client_id
      rate_limit:
        unit: minute
        requests_per_unit: 100
```

Apply it with:
```bash
helm install envoy-ratelimiter . -f custom-values.yaml -n envoy-ratelimiter
```
