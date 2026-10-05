# How to Contribute

We'd love to accept your patches and contributions to this project. There are
just a few small guidelines you need to follow.

## Contributor License Agreement

Contributions to this project must be accompanied by a Contributor License
Agreement. You (or your employer) retain the copyright to your contribution;
this simply gives us permission to use and redistribute your contributions as
part of the project. Head over to <https://cla.developers.google.com/> to see
your current agreements on file or to sign a new one.

You generally only need to submit a CLA once, so if you've already submitted one
(even if it was for a different project), you probably don't need to do it
again.

## Code reviews

All submissions, including submissions by project members, require review. We
use GitHub pull requests for this purpose. Consult
[GitHub Help](https://help.github.com/articles/about-pull-requests/) for more
information on using pull requests.


## Branching and Backporting Policy

To maintain consistency across supported releases and prevent missing fixes in newer releases, all contributors must follow this workflow:

1. **Primary PR on `master`:** Always open your initial Pull Request against the `master` branch, unless the issue or bug is only applicable to older release branches (e.g., if the code has been refactored or removed on `master`).
2. **Update Release notes in `Changes.md`:** Update the release notes in `Changes.md` to document your changes; please provide a concise, one-line summary describing the impact of your contribution.
3. **Determine Target Versions:** Identify the minimum version branch where your change or fix needs to be released.
4. **Backport to All Higher Active Branches:** Once the PR is approved/merged into `master`, backport the change to the minimum version branch **and all higher active version branches** (for example, if a fix targets `3.1`, open backport PRs for `3.1`, `4.0`, and any higher active branches).

> **Note:** Backporting to older release branches is optional. If you choose not to backport your change, it will naturally be included in the next release branch cut from `master`.
