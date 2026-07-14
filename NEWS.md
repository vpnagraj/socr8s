# Changelog

## v0.1.0 (2026-07-11)

Initial release!

socr8s provides a Kubernetes-native framework for reviewing containerized tools. The current implementation includes **build** and **pull** components, each of which processes jobs from a centralized queue backed by a database.

This release includes all Kubernetes manifests and documentation to deploy the stack, populate the job queue, and monitor build/pull success.