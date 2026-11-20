# Engineering Git & Coding Workflow Standards

## 1. Purpose & Scope
This document defines the standard Git workflow, naming conventions, and code quality practices for all software engineers. Consistent conventions improve readability, streamline code reviews, and enable seamless automation across Jira and our CI/CD pipelines.

---

## 2. Branch Naming Convention
All feature, bugfix, and maintenance branches must follow this exact pattern:

```text
<type>/<JIRA-TICKET>-<kebab-case-description>
```

### Allowed Types

| Type | Purpose | Example |
| :--- | :--- | :--- |
| **`feat`** | New feature or enhancement | `feat/PROJ-1042-s3-multipart-upload` |
| **`fix`** | Bug fix | `fix/PROJ-889-handle-null-pointer` |
| **`hotfix`** | Urgent production fix | `hotfix/PROJ-901-auth-token-expiry` |
| **`chore`** | Build scripts, dependencies, tooling | `chore/PROJ-1102-upgrade-go-1.23` |
| **`refactor`** | Code restructuring without behavior change | `refactor/PROJ-754-simplify-retry-logic` |
| **`docs`** | Documentation updates only | `docs/PROJ-612-update-api-readme` |

### Formatting Rules
* **Use lowercase only** (except for the uppercase Jira project key) to prevent case-sensitivity issues across macOS and Linux environments.
* **Separate words with hyphens (`-`)**—never use spaces or underscores.
* **Keep descriptions concise** (2 to 5 words) so branch names remain readable in terminal prompts and CI dashboards.

---

## 3. Branch Lifecycle & Limits
To prevent repository clutter, merge conflicts, and excessive context switching, adhere to the following lifecycle rules:

* **WIP Limit:** Engineers should maintain **no more than 2–3 active branches** simultaneously (e.g., one primary feature branch, one branch awaiting code review, and an occasional urgent hotfix).
* **Short-Lived Branches:** Branches must be merged within **3–5 business days**. Break larger Jira tickets into smaller, incremental PRs/MRs—using feature flags to hide incomplete functionality if necessary.
* **Auto-Cleanup:** Source branches must be deleted immediately upon merge (enforced via repository settings). Any remote branch with **no activity for 30+ days** and no open PR/MR is considered stale and subject to automated or administrative cleanup.

---

## 4. Commit Message Standard
We follow the **Conventional Commits** specification to enable automated changelog generation and clear Git history.

```text
<type>(<JIRA-TICKET>): <imperative summary>

[optional body explaining "why", not "how"]
```

### Examples
* **Good:** `feat(PROJ-1042): add exponential backoff to storage client`
* **Bad:** `fixed stuff` or `PROJ-1042 updates`

---

## 5. Pull Request (PR) / Merge Request (MR) Standards
* **Title Format:** Must match the commit message format (`feat(PROJ-1042): add exponential backoff to storage client`). When using *Squash and Merge*, this title becomes the permanent commit message on `main`.
* **Size Limit:** Keep changes under **400 lines of modified code** (excluding auto-generated files, lockfiles, and test fixtures) to ensure thorough and timely peer reviews.
* **Self-Review:** Authors must review their own diff, resolve all automated linter warnings, and verify that all CI checks pass before requesting reviews.

---

## 6. Automation & Enforcement
To avoid manual policing during code reviews, these standards are enforced automatically:

### Jira Integration
Including the Jira issue key (`PROJ-123`) in both the branch name and commit message enables Jira to:
* Automatically link branches, commits, and PRs/MRs to the ticket's **Development** panel.
* Trigger automated workflow transitions (e.g., *PR Opened* $\rightarrow$ *In Review*, *PR Merged* $\rightarrow$ *Done*).

### Git Hooks & CI Checks
1. **Local Pre-Commit / Commit-Msg Hooks:** Configure tools such as `pre-commit` or `Husky` to validate commit message formatting locally before pushing.
2. **CI Branch Name Validation:** CI pipelines enforce branch naming using the following regular expression:
   ```regex
   ^(feat|fix|hotfix|chore|refactor|docs)\/[A-Z]+-[0-9]+-[a-z0-9-]+\$
   ```
3. **Branch Protection:** Direct pushes to `main` are disabled. All changes require an approved PR/MR and passing CI status checks.

---

## 7. General Code Quality Baseline
* **Automated Formatting & Linting:** Code style (indentation, spacing, bracket placement) must be handled exclusively by automated formatters and linters in CI—never debated in code review comments.
* **Test Coverage:** Every `feat` or `fix` PR/MR must include unit or integration tests verifying the new behavior or reproducing the resolved bug.
* **Secrets Management:** Never commit API keys, passwords, certificates, or `.env` files. Automated secret scanning runs on every push.
