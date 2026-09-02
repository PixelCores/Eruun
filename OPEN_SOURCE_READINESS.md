# Open-Source Readiness Gate

This repository's technical cleanup does not establish ownership or relicensing rights. Before public launch, an authorized rights holder must confirm that the original code, historical contributions, documentation, examples, and assets may be released under MIT. Mechanical renaming and adding `LICENSE` are not substitutes for that review.

## Required before public launch

- [ ] Rights holder approves relicensing and publication scope.
- [ ] Contribution history and third-party material are reviewed.
- [ ] Security findings and sensitive-content scans are resolved.
- [ ] `THIRD_PARTY_LICENSES.csv` is regenerated with the pinned tool and reviewed; unknown, restricted, and forbidden classifications are resolved.
- [ ] Default branch protection and required CI checks are enabled.
- [ ] GitHub dependency review is enabled for pull requests.
- [ ] Private Vulnerability Reporting is enabled.
- [ ] Secret scanning and push protection are enabled.
- [ ] The initial image, tag, Chart version, appVersion, and `EruunVersion` all agree.
- [ ] A clean-install deployment is validated in a disposable cluster.

The third-party inventory is technical due-diligence material and does not replace legal advice.
