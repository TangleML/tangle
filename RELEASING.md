# **Release Process**

This document outlines the process and policies for creating a new release of the Tangle project.

This guide is primarily intended for project Maintainers and designated **Release Managers** (as defined in `GOVERNANCE.md`).

## **1\. Versioning**

Tangle follows **Semantic Versioning 2.0.0** (https://semver.org/\#semantic-versioning-200). All release tags must adhere to this specification.

Given a version number `MAJOR.MINOR.PATCH`, we increment:

* `MAJOR` version for incompatible, breaking API changes.
* `MINOR` version for new, backwards-compatible functionality.
* `PATCH` version for backwards-compatible bug fixes.

**Pre-1.0.0:** While the project is in its early stages (version `0.x.y`), breaking changes may be introduced in `MINOR` releases.

## **2\. Release Cadence**

Tangle does not have a fixed release schedule (e.g., "every 6 weeks").

* **Minor/Major** releases are cut when a sufficient number of features or changes have accumulated on the `main` branch.
* **Patch** releases are made on-demand to address critical bugs or security vulnerabilities.

## **3\. Release Process**

Releases will be available at https://github.com/TangleML/tangle/releases

Releases will be published to PyPI and/or Hugging Face (details TBD)

Update the website and documentation https://tangleml.com/

Any disputes or disagreements related to the release process will be handled in accordance with the decision making section of GOVERNANCE.md.

## **4\. Support & Backport Policy**

* **Supported Versions:** We provide active support for the **latest `MINOR` release** (e.g., `v0.2.x`).
* **Security Fixes:** Critical security vulnerabilities will be backported to the previous `MINOR` release, at the discretion of the Security Team.
* **Bug Fixes:** We do not generally backport non-critical bug fixes. We encourage all users to upgrade to the latest version.

## Appendix

Current list of Release Managers:

* @Ark-kun
* @camielvs

