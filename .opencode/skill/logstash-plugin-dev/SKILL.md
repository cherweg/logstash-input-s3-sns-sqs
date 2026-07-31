---
name: logstash-plugin-dev
description: Build, test and smoke-verify the logstash-input-s3-sns-sqs plugin inside JRuby / Logstash containers via podman. Use WHEN the user asks to "run the tests", "test the plugin", "test a PR", "build the gem", "install into logstash", "smoke test", "reproduce issue", or any local verification against JRuby 9.4+ / Logstash 8+. Do NOT use for release/publish steps — that is a maintainer action.
---

# Logstash Plugin Dev — logstash-input-s3-sns-sqs

Local build & test workflow for this repo. **Host Ruby is MRI 2.6 which cannot
load the plugin** — everything runs inside `docker.io/library/jruby:9.4`
via `podman`. `docker` is NOT installed on this machine.

Assume the working directory is the repo root
(`/Users/christian.herweg/github/logstash-input-s3-sns-sqs`) unless otherwise stated.

## 0. Environment sanity check

Before any container work:

```sh
podman machine list                   # must show a Running machine
podman info --format '{{.Host.Arch}}' # arm64 on this Mac
```

The `jruby:9.4` image is multi-arch (amd64 + arm64), so it works natively on
Apple Silicon. First pull is ~500 MB.

### 0.1 Corporate TLS interception (MITM proxy)

This developer's network re-signs TLS with an internal corporate CA. Symptom:
`certificate verify failed` when the container tries to reach
`artifacts.elastic.co` (logstash-plugin) or `rubygems.org`. The Elastic
Logstash image and `jruby:9.4` do NOT trust the corporate CA out of the box.

**Workaround**: export the Mac System keychain into a PEM bundle inside the
repo, then set `SSL_CERT_FILE` for the container.

```sh
# One-time: dump the full Mac system CA bundle (contains corporate CA)
security find-certificate -a -p /Library/Keychains/System.keychain > ./system-cas.pem
# (git-ignore this file — it's an environment artefact)

# Then in every podman run, add:
#   -v "$PWD":/plugin  -e SSL_CERT_FILE=/plugin/system-cas.pem
```

Every recipe below assumes this file exists. Remove `system-cas.pem` at the
end of a working session — do not commit it.

## 1. Testing a PR (the most common request)

Given a PR number `<N>`:

```sh
# 1) Fetch the PR into a local branch (never merges into master)
git fetch origin "pull/<N>/head:pr-<N>"
git checkout "pr-<N>"

# 2) Inspect the diff before running anything
gh pr view <N> --repo cherweg/logstash-input-s3-sns-sqs
gh pr diff <N> --repo cherweg/logstash-input-s3-sns-sqs
```

Then run the two verification layers described in §2 and §3.

Return to master with `git checkout master` when done. Do NOT push, merge or
delete the PR branch unless explicitly told to.

## 2. Layer 1 — `bundle install` + `rspec` in JRuby

Minimal test loop. Caches `vendor/bundle` between runs.

```sh
podman run --rm -it \
  -v "$PWD":/plugin -w /plugin \
  -e BUNDLE_PATH=/plugin/vendor/bundle \
  docker.io/library/jruby:9.4 \
  bash -lc "gem install bundler -v '~> 2.4' --no-doc \
         && bundle install \
         && bundle exec rspec --format documentation"
```

Expected outcome:
- `bundle install` resolves ~200 gems (5–15 min cold, seconds warm).
- `rspec` prints two examples, both green. The suite is intentionally thin
  (see CHANGELOG 2.1.2). **Green rspec ≠ full validation.**

If `bundle install` fails on a network-restricted machine, add
`--network=host` to the `podman run` line.

## 3. Layer 2 — smoke test inside real Logstash

This is the check that actually caught issue #94 (`File.exists?` NoMethodError).

```sh
# 3a. Build the gem inside JRuby (never on host MRI)
podman run --rm \
  -v "$PWD":/plugin -w /plugin \
  docker.io/library/jruby:9.4 \
  gem build logstash-input-s3-sns-sqs.gemspec
# -> produces logstash-input-s3-sns-sqs-<version>.gem in repo root

# 3b. Install into the exact Logstash image used in production and
#     validate the config parses (no AWS traffic).
#     NOTE: --entrypoint "" is required (image WORKDIR is /home, not /usr/share/logstash).
#           --local skips the artifacts.elastic.co pack check; SSL_CERT_FILE
#           makes the fallback network path work behind corporate MITM (§0.1).
podman run --rm \
  -v "$PWD":/plugin \
  -w /usr/share/logstash \
  --entrypoint "" \
  -e SSL_CERT_FILE=/plugin/system-cas.pem \
  docker.elastic.co/logstash/logstash:9.4.4 \
  bash -lc "bin/logstash-plugin install --local /plugin/logstash-input-s3-sns-sqs-*.gem \
         && bin/logstash \
              -e 'input { s3snssqs { queue => \"dummy\" region => \"eu-central-1\" } } \
                  output { stdout {} }' \
              --config.test_and_exit"
```

Expected outcome:
- `logstash-plugin install` prints `Installation successful`.
- `logstash --config.test_and_exit` prints
  `Config Validation Result: OK. Exiting Logstash` and exits 0.
- No `NoMethodError`, no stack trace on stderr.

Any Ruby-version-compat bug (`File.exists?`, `Fixnum`, keyword-arg regressions)
will crash here — earlier than in production.

Clean up the built gem when done: `rm logstash-input-s3-sns-sqs-*.gem`.

## 4. Iterating on code changes

Mount the repo and re-run §3 after every edit. There is no watch mode. Keep
the same `BUNDLE_PATH=/plugin/vendor/bundle` cache to avoid re-resolving gems.

For faster inner loop on Ruby-only changes (no gem rebuild), you can mount
the plugin into an already-installed Logstash and edit in place:

```sh
podman run --rm -it \
  -v "$PWD/lib":/usr/share/logstash/vendor/bundle/jruby/3.1.0/gems/logstash-input-s3-sns-sqs-<ver>/lib \
  docker.elastic.co/logstash/logstash:9.4.4 \
  bin/logstash --config.test_and_exit ...
```

Only useful when the gem is already installed inside the image; usually the
gem-install path (§3b) is simpler and less error-prone.

## 5. Interpreting results

| Symptom                                            | Likely cause                                            | Fix |
|----------------------------------------------------|---------------------------------------------------------|-----|
| `NoMethodError: File.exists?`                      | Ruby 3.2+ removed it — old code path                    | Replace with `File.exist?` (see PR #95, issue #94) |
| `LoadError: cannot load such file -- java`         | Ran under MRI Ruby, not JRuby                           | Re-run inside the container |
| `Bundler could not find compatible versions ...`   | Gemfile.lock captured from older bundler                | Delete `Gemfile.lock` and retry, or match bundler `~> 2.4` |
| `logstash-plugin install ... Bundler::VersionConflict` | Plugin depends on gem already vendored in Logstash    | Check `s.add_runtime_dependency` versions in gemspec |
| `AWS Core CA bundle not found`                     | `use_aws_bundled_ca => true` but SDK layout changed     | Look at `plugin_mixins/aws_config/v2.rb` |

## 6. Hard rules

- **Never** run `bundle exec rake publish_gem` — release only, maintainer only.
- **Never** commit the built `.gem` file, `Gemfile.lock` churn, or
  `vendor/bundle/`.
- **Never** modify `Gemfile.lock` manually.
- **Always** verify against `docker.elastic.co/logstash/logstash:9.4.4`
  (Logstash 9.4.x is the current test target, matches maintainer + PR #95
  reporter environment).
- Prefer `podman` over `docker` — `docker` is not installed.
