# Security Policy

## Supported versions

Until the first public release, security fixes are made on the default branch. After releases begin, this section must list the supported release lines.

## Reporting a vulnerability

Use GitHub Private Vulnerability Reporting for this repository. Do not disclose the issue in a public issue, discussion, pull request, or chat.

Include:

- the affected version or commit;
- a minimal reproduction;
- expected and observed impact;
- relevant configuration and deployment assumptions;
- any suggested mitigation.

Repository administrators must enable Private Vulnerability Reporting, secret scanning, and push protection before the repository is made public. If the private reporting feature is unavailable, contact the repository owner through a private channel and wait for acknowledgement before disclosure.

## Scope

High-value areas include authentication and authorization, secret handling, namespace adoption, Kubernetes RBAC, workflow fencing and leases, callback and outbound URL validation, and supply-chain integrity.

Never include production credentials or sensitive customer data in a report.
