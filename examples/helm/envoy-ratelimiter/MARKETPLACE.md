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

# Steps to Publish Envoy Rate Limiter on Google Cloud Marketplace

This guide outlines the steps required to take this Helm chart and publish it as a Public Kubernetes Application on the Google Cloud Marketplace.

## 1. Prerequisites (Partner Advantage)

To list solutions on the Google Cloud Marketplace, you must be a registered partner.
1.  **Join Partner Advantage**: Register at [Partner Advantage](https://partneradvantage.goog/).
2.  **Access Producer Portal**: Once registered, you will gain access to the Producer Portal, where you manage your listings.

## 2. Package the Application

GCP Marketplace requires the application to be delivered via a container registry and a package (Helm chart).

### Container Images
-   Ensure the `envoyproxy/ratelimit` and `redis` images you use are compliant with Marketplace requirements (e.g., no severe vulnerabilities).
-   You may need to push these images to a Google-managed registry (like Artifact Registry) during the listing process.

### Helm Chart
-   The files in this directory compose the Helm chart.
-   You will need to package this chart (usually as a `.tgz` file) or link your repository to the Producer Portal.

## 3. Create UI Schema (`app-schema.yaml`)

To enable "one-click" deploy with input configs, Marketplace uses a UI schema to generate the entry form.
You will need to create an `app-schema.yaml` file (usually in a folder specified by the integration guidelines, or pasted into the portal).

Here is an example for our Rate Limiter:

```yaml
properties:
  namespace:
    type: string
    title: Namespace
    description: Kubernetes namespace to deploy resources into.
    default: envoy-ratelimiter

  loadBalancerIP:
    type: string
    title: Static Internal IP (Optional)
    description: A pre-reserved static internal IP address for the load balancer. Leave empty for auto-allocation.

  configYaml:
    type: string
    title: Rate Limit Configuration
    description: The YAML configuration for your rate limits.
    display: textarea
    default: |
      domain: mongo_cps
      descriptors:
        - key: database
          value: users
          rate_limit:
            unit: second
            requests_per_unit: 500
```

## 4. Verification and Submission

1.  **Test Deployment**: The Producer Portal will provide instructions to test your deployment in a sandboxed environment to ensure the Helm chart installs correctly and the UI schema works.
2.  **Submit for Review**: Once tested, submit the listing for review. Google will audit it for security and functionality before it goes live.
