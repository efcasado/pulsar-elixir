# Contributing to pulsar-elixir

Bug fixes, features, tests, and documentation improvements are welcome. Start with the
[README](README.md) for public API usage. Read the [architecture guide](docs/architecture.md)
before changing clients, consumers, producers, readers, brokers, topology, or supervision.

## Issues

Search existing issues before opening one and use a title that describes the problem or goal.
Link relevant documentation, upstream issues, or example repositories and briefly explain how
they relate. [GitHub Issues](https://github.com/efcasado/pulsar-elixir/issues)

- **Questions.** Explain what you're trying to do, show the relevant code or configuration, and
  describe what you've already tried and where you're stuck.
- **Bug reports.** Describe what you expected and what happened. Include the shortest steps to
  reproduce the problem and a minimal code example or reproduction repository when possible.
  List the `pulsar_elixir`, Elixir, Erlang/OTP, and Pulsar broker versions, plus the
  `off_broadway_pulsar` version if relevant. Include relevant client or broker logs and stack
  traces from the failing run when available. Use fenced code blocks and redact credentials and
  private data from anything you share.
- **Feature requests.** Describe the use case, the behavior you'd like, and how it would help.
  Include an example or an existing workaround if it clarifies the request.

## Pull requests

Keep each PR focused on one contribution that reviewers can evaluate on its own. Include the
tests, documentation, and supporting changes it needs. Open separate PRs for unrelated work.

The [PR template](.github/pull_request_template.md) starts with `### Description`. In a few
sentences, explain what prompted the change and what it does. Give reviewers enough context to
understand the change before opening the diff.

```markdown
### Description

An idle reader can stay open past its configured timeout while it processes flow-control
updates. This change makes the reader close when the timeout expires.
```

Add a related issue, reproduction steps, or upgrade instructions when they help explain the
change.

Make sure new or existing tests cover behavior changes. Review the documentation and examples,
and update them where needed.

## Conventional Commits

We use [Conventional Commits 1.0.0](https://www.conventionalcommits.org/en/v1.0.0/) for commit
messages and PR titles. We squash-merge each PR into a single commit on `main`, so write the PR
title as the subject for that commit. Release Please uses the commit history to determine version
bumps and generate the changelog.

```text
<type>[(<scope>)][!]: <short description>
```

Choose the type that best describes the change. The optional scope names the affected area,
such as `consumer`, `reader`, or `ci`. Write a short description that starts with a verb.

| Type | Use it for |
| --- | --- |
| `feat` | New functionality |
| `fix` | Bug fixes |
| `perf` | Performance improvements |
| `deps` | Dependency updates, including CI and Docker dependencies |
| `docs` | Documentation changes |
| `style` | Formatting and other code style changes |
| `refactor` | Code restructuring that preserves behavior |
| `test` | Changes to tests or test support |
| `build` | Build system or packaging changes |
| `ci` | CI workflow changes |
| `chore` | Other maintenance |
| `revert` | Reverting an earlier commit |

Examples include `fix(reader): enforce inactivity timeout`, `docs: explain manual flow control`,
and `deps(ci): bump mise-action`. The [release configuration](release-please-config.json) lists
the changelog sections.

For a breaking change, put `!` before the colon in the PR title and explain the impact and
migration steps in the PR description. In a commit message, mark the change with `!` in the
subject or a `BREAKING CHANGE:` footer after a blank line. You can use both.

```text
refactor(producer)!: remove the producer_count option

BREAKING CHANGE: Remove producer_count from producer configuration.
```

## Verification

Install the project toolchain from [`.mise.toml`](.mise.toml). These commands assume `mise` is
active in your shell.

```sh
mise install
mix deps.get
mix format --check-formatted
MIX_ENV=test mix compile --warnings-as-errors
MIX_ENV=test mix credo --strict
MIX_ENV=test mix dialyzer
mix test --exclude integration --warnings-as-errors
```

Run the integration suites relevant to your change. The harness uses Docker Compose to start
and stop Pulsar. To run all integration tests, use
`mix test --only integration --max-cases 2 --warnings-as-errors`.

For changes to published documentation, run `mix docs` and check the generated pages and links.

Under `### Verification`, briefly list the checks you ran and their results. Include the
`off_broadway_pulsar` revision and test result, and mention any checks still pending and why.

### Verify `off_broadway_pulsar` compatibility

Before merging any PR, run the [off_broadway_pulsar](https://github.com/efcasado/off_broadway_pulsar)
test suite against the `pulsar-elixir` checkout containing your changes. Use a separate checkout
of the companion project's `main` branch. In its `mix.exs`, temporarily replace the Hex
dependency on `:pulsar` with a local path dependency.

```elixir
{:pulsar, path: "../pulsar-elixir"},
```

Adjust the path to point to your PR checkout. With Docker running, run these commands from the
companion checkout.

```sh
mise install
mix deps.get
mix test --warnings-as-errors
```

Check that the companion's Hex dependency range accepts the intended Pulsar release. If the
change requires a companion update, test the two changes together and link the `off_broadway_pulsar`
PR under `### Verification`.

## AI-assisted contributions

We welcome and encourage AI-assisted contributions. Use AI to explore the code and draft changes
or tests. You are responsible for everything you submit, including generated code, tests, and
documentation.

Run a separate adversarial AI review before submitting AI-generated code. We encourage the same
review for other substantive changes. Ask the reviewer to look for regressions, edge cases,
and gaps in test coverage. Verify the findings yourself and address the issues you confirm.

Review the full diff yourself. Be able to explain how the change works, why it fits the project,
and what the tests demonstrate. Check generated suggestions against the project's APIs and
documented behavior, run the relevant checks yourself, and be prepared to answer review questions
and make revisions.

### Run a review from the CLI

Run the examples from the repository root with an authenticated CLI. Coding agents with shell
access can use the same commands to start a separate review session.

Codex has built-in commands to select a review target.

```sh
# Review staged, unstaged, and untracked changes before committing.
codex review --uncommitted

# Review the PR against its up-to-date target branch from a clean working tree.
codex review --base origin/main
```

For a focused review, set the prompt below and choose one of the CLI commands. Replace
`origin/main` with the up-to-date ref for your PR's target branch if it differs. To review local
changes to tracked files, use `git diff HEAD --` in place of the branch diff. Use
`git diff --cached --` to review staged changes only.

```sh
review_prompt='Challenge the proposed changes in the supplied diff. Look for
regressions, edge cases, and gaps in test coverage. For OTP changes, read
docs/architecture.md and check process ownership, restart behavior, and recovery.
Return actionable findings with file and line references.'
```

```sh
# Codex
review_diff=$(git diff origin/main...HEAD --) &&
  printf '%s\n' "$review_diff" |
  codex exec --sandbox read-only "$review_prompt"
```

```sh
# Claude Code
review_diff=$(git diff origin/main...HEAD --) &&
  printf '%s\n' "$review_diff" |
  claude --tools Read --disallowedTools 'mcp__*' --print "$review_prompt"
```

```sh
# Mistral Vibe
review_diff=$(git diff origin/main...HEAD --) &&
  printf '%s\n\n%s\n' "$review_prompt" "$review_diff" |
  vibe --enabled-tools read_file --prompt
```

The CLI references explain the available review options.
[Codex](https://learn.chatgpt.com/docs/developer-commands?surface=cli#codex-review) ·
[Claude Code](https://code.claude.com/docs/en/cli-reference) ·
[Mistral Vibe](https://docs.mistral.ai/vibe/code/cli/work-with-cli#programmatic-mode)
