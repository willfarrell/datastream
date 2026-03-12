# Security Policy

This document outlines security procedures and general policies for the datastream Open Source projects as found on https://github.com/willfarrell/datastream.

* [Security Goals](#security-goals)
* [Supported Versions](#supported-versions)
* [Reporting a Vulnerability](#reporting-a-vulnerability)
* [Disclosure Policy](#disclosure-policy)

## Security Goals
Our goal is to ensure OSS follows secure design principles and meets security best practices as outlined by the following [OWASP ASVS v5.0 Level 3](https://github.com/OWASP/ASVS/tree/master/5.0/en).

Standards are evaluated using automated scans (Linting, Unit tests, SAST, SCA, DAST, Perf) and manual self-audits. 3rd party audits are welcome.

## Secure design principles

- secure by default
- use white lists
- no backdoors
- follow least privilege
- keep it simple

## Supported Versions
Only the latest version is supported for security updates.

## Threat model

All options and configuration are assumed to be trusted as they are configured by the implementing developer.

## Trust Boundaries

datastream processes data through streams. The configuration of datastream is trusted. It's up to the implementing developer to apply input validation to ensure data is properly structured and safe to use. User inputs to all packages are fuzzed.

## Reporting a Vulnerability

The datastream OSS team and community take all security vulnerabilities
seriously. Thank you for improving the security of our open source
software. We appreciate your efforts and responsible disclosure and will
make every effort to acknowledge your contributions.

Report security vulnerabilities by emailing the lead maintainer at:
```
willfarrell@proton.me
```
The lead maintainer will acknowledge your email within 24 hours, and will
send a more detailed response within 48 hours indicating the next steps in
handling your report. After the initial reply to your report, the security
team will endeavour to keep you informed of the progress towards a fix and
full announcement, and may ask for additional information or guidance.

Report security vulnerabilities in third-party modules to the person or
team maintaining the module.

## Disclosure Policy

When the security team receives a security bug report, they will assign it
to a primary handler. This person will coordinate the fix and release
process, involving the following steps:

  * Confirm the problem and determine the affected versions.
  * Audit code to find any potential similar problems.
  * Prepare fixes for all releases still under maintenance. These fixes
    will be released as fast as possible to NPM.
