# Security policy

## Reporting

Please report vulnerabilities privately through GitHub's **Security** tab for
this repository. Do not include API keys, tracker credentials, private torrent
metadata, or database exports in a public issue.

## Operator secrets

CrownIndex reads TMDB and Jackett credentials from an untracked `.env` file.
The example configuration contains no live credentials. The public API,
Bitmagnet UI, Jackett UI, and FlareSolverr status bind to loopback by default;
review authentication and network policy before exposing any service.

TMDB credentials are provided only to CrownIndex. They are not forwarded to
Bitmagnet, where an API credential could otherwise be included in request URLs
and upstream error logs.

## Dependency audit exception

`Cargo.lock` contains `rsa 0.9.10` through SQLx's derive-macro dependency
metadata. It is not reachable in any target or feature combination used by
CrownIndex (`cargo tree --target all --invert rsa` is empty). CI verifies that
invariant before applying the `RUSTSEC-2023-0071` audit exception. If the crate
becomes reachable, CI fails and the exception must be removed or reassessed.
