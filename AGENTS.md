# AGENTS.md — logstash-input-s3-sns-sqs

> Project-specific guidance for AI agents working in this repo.
> **Read this before making any code / tooling / release changes.**

---

## 1. What this project is

A **Logstash input plugin** written in Ruby (runs on JRuby, since Logstash uses JRuby).
It ingests S3 object-created events via SQS (optionally via SNS → SQS fan-out), downloads
the referenced S3 objects, and emits Logstash events from their contents.

- Gem name: `logstash-input-s3-sns-sqs`
- Current version: `2.2.2` (see `logstash-input-s3-sns-sqs.gemspec`)
- Author: Christian Herweg
- License: Apache-2.0
- Target: Logstash 8 / 9. The current Logstash 9.4.4 image ships
  **JRuby 10.0.6 running Ruby 3.4.5** (verified against
  `docker.elastic.co/logstash/logstash:9.4.4`), so any Ruby-3.2+ removal
  (`File.exists?`, `Fixnum`, `TRUE`/`FALSE`, hash-as-kwarg) will crash there.
  The gem itself still builds fine in `jruby:9.4` (Ruby 3.1) — see §3.1.

Key runtime dependencies (see gemspec):
`logstash-core-plugin-api ~> 2.0`, `aws-sdk-core ~> 3`, `aws-sdk-s3`, `aws-sdk-sqs`,
`aws-sdk-sns`, `concurrent-ruby`, `stud`, `logstash-codec-json`, `logstash-codec-plain`.

The plugin **includes an in-house `aws_config` mixin** under
`lib/logstash/plugin_mixins/aws_config/` — this was inlined to remove the
external `logstash-mixin-aws` dependency (see CHANGELOG 2.2.2: “Inhouse mixin components”).
Do not re-introduce that gem without discussing with maintainer.

---

## 2. Source layout

```
lib/logstash/
├── inputs/
│   ├── s3snssqs.rb                 # main input class LogStash::Inputs::S3SNSSQS
│   ├── codec_factory.rb            # picks codec by folder/content-type
│   ├── mime/magic_gzip_validator.rb # small magic-byte gzip detection (perf)
│   ├── s3/
│   │   ├── client_factory.rb       # per-bucket / per-region S3 client cache
│   │   └── downloader.rb           # S3 GET + local temp file handling
│   ├── s3snssqs/
│   │   └── log_processor.rb        # decompress + line-split + codec + emit event
│   └── sqs/
│       └── poller.rb               # SQS receive loop, visibility, delete
└── plugin_mixins/
    └── aws_config/
        ├── generic.rb              # config DSL used by v2.rb
        ├── v2.rb                   # AWS SDK v2/v3 wiring: aws_options_hash
        └── ...
spec/inputs/s3sqs_spec.rb           # rspec (currently minimal; mostly disabled)
fixtures/log-stream.real-formatted  # real sample used by the spec
docs/index.asciidoc                 # user-facing plugin documentation
```

### Threading model
- One SQS poller loop feeds a work queue.
- N `consumer_threads` decode S3 objects concurrently.
- A watcher thread restarts consumer threads that died with an exception
  (feature added in 2.1.0).

### Message flow
1. `SQS::Poller#run` receives messages.
2. Message body is parsed as either a bare S3 event or an SNS-wrapped S3 event.
3. For each S3 record: `S3::Downloader#copy_s3object_to_disk` writes to
   `temporary_directory`.
4. `S3SNSSQS::LogProcessor#process` reads (gunzip if needed), splits on `\n`,
   pushes lines into the configured codec, decorates the event with S3 metadata,
   and emits.
5. On success SQS message is deleted; on failure visibility timeout is used to
   requeue.

---

## 3. How to build & test — **the only supported way locally**

**⚠️ You need JRuby.** MRI Ruby (`/usr/bin/ruby`) will *not* work — the plugin
`require`s `java_import` and depends on `logstash-core-plugin-api` which only
resolves on JRuby.

The maintainer does **not** ship a Dockerfile or CI workflow (no `.github/`,
no `.travis.yml`). Use the official `jruby:9.4` image via `podman` (or `docker`).

### 3.1 Quick recipe (podman on macOS)

```sh
# from repo root
podman run --rm -it \
  -v "$PWD":/plugin -w /plugin \
  docker.io/library/jruby:9.4 \
  bash -lc "gem install bundler -v '~> 2.4' --no-doc && bundle install && bundle exec rspec --format documentation"
```

The first run downloads the JRuby image (~500 MB) and resolves ~200 gems.
Expect **5–15 minutes** on a cold cache. Subsequent runs with a mounted
`vendor/bundle` are seconds.

### 3.2 Cached run (recommended)

```sh
podman run --rm -it \
  -v "$PWD":/plugin -w /plugin \
  -e BUNDLE_PATH=/plugin/vendor/bundle \
  docker.io/library/jruby:9.4 \
  bash -lc "gem install bundler -v '~> 2.4' --no-doc && bundle install && bundle exec rspec"
```

Add `vendor/` to `.gitignore` (already ignored via `.vendor/`; consider `vendor/`).

### 3.3 What the current spec covers

`spec/inputs/s3sqs_spec.rb` is a smoke test. It only verifies the plugin loads
and `process` returns `true` on a real gzipped fixture. Most assertions are
commented out (see CHANGELOG 2.1.2 “CLEANUP: Remove tests”). **Do not treat
a green rspec as full validation.** Non-trivial changes should also be
smoke-tested inside a real Logstash pipeline.

### 3.4 Smoke test inside a real Logstash container

```sh
# Build the gem
podman run --rm -v "$PWD":/plugin -w /plugin docker.io/library/jruby:9.4 \
  gem build logstash-input-s3-sns-sqs.gemspec

# Install into official Logstash and start with minimal config.
# Notes:
#   --entrypoint ""  -> image WORKDIR is /home, override needed
#   --local          -> skip artifacts.elastic.co pack lookup
#   SSL_CERT_FILE    -> needed behind corporate TLS MITM (see plugin-dev skill §0.1)
podman run --rm -v "$PWD":/plugin -w /usr/share/logstash --entrypoint "" \
  -e SSL_CERT_FILE=/plugin/system-cas.pem \
  docker.elastic.co/logstash/logstash:9.4.4 \
  bash -lc "bin/logstash-plugin install --local /plugin/logstash-input-s3-sns-sqs-*.gem && bin/logstash -e 'input { s3snssqs { queue => \"dummy\" region => \"eu-central-1\" } } output { stdout {} }' --config.test_and_exit"
```

`--config.test_and_exit` validates that the plugin registers cleanly without
actually connecting to AWS.

---

## 4. Release process

Source of truth: `release.howto` (dated; commands below are the modernized
equivalent).

1. Bump `s.version` in `logstash-input-s3-sns-sqs.gemspec`.
2. Prepend a stanza to `CHANGELOG.md`. Follow the existing terse style
   (`##<version>` header, `-` bullets, feature/fix/security tags).
3. Commit with message `prepare release <version>`.
4. Build & push:
   ```sh
   # inside a jruby container with ~/.gem/credentials mounted
   podman run --rm -it -v "$PWD":/plugin -v "$HOME/.gem":/root/.gem -w /plugin \
     docker.io/library/jruby:9.4 \
     bash -lc "bundle install && bundle exec rake vendor && bundle exec rake publish_gem --trace"
   ```
5. Tag the release: `git tag v<version> && git push --tags`.
6. Verify on <https://rubygems.org/gems/logstash-input-s3-sns-sqs>.

**Never** run `publish_gem` from macOS host Ruby — the resulting gem may miss
JRuby-specific vendor artefacts.

---

## 5. Code conventions & gotchas

- **JRuby-only Ruby features.** The code uses `java_import` and Java `Zlib` for
  gzip perf. Do not switch to pure-Ruby gzip.
- **Ruby version drift.** JRuby 9.4+ tracks Ruby 3.1+. `File.exists?` (with `s`)
  was removed. Always use `File.exist?`. This bit us in issue #94 / PR #95.
- **Threading.** Consumer threads are managed by hand plus `concurrent-ruby`.
  Any exception must be caught and logged; otherwise the watcher will restart
  the thread forever. Do not swallow errors silently.
- **AWS SDK v3.** The plugin migrated from SDK v2 to v3 in 2.2.0. Any new AWS
  call must go through `S3::ClientFactory` (per-bucket region resolution).
- **Backwards config compat.** Deprecated flat `s3_*` options must still merge
  into `s3_options_by_bucket` (see CHANGELOG 2.0.0). Don’t remove without a
  major version bump.
- **UTF-8.** Log lines can contain broken UTF-8. `LogProcessor` uses
  `scrub`-style handling — preserve it (see 1.0.2, 2.1.2).
- **No file deletion from S3 by default.** The plugin intentionally leaves
  objects in place (users rely on S3 Lifecycle). `move_to_bucket` (PR #82)
  is the opt-in.

---

## 6. Reviewing PRs

Recommended checklist before merging any PR:

1. `git fetch origin pull/<N>/head:pr-<N> && git checkout pr-<N>`
2. Read the diff top-to-bottom. Prefer minimal, focused PRs.
3. Confirm compatibility with **JRuby 9.4+ / Ruby 3.1+** (no `File.exists?`,
   no `Fixnum`, no keyword-args regressions).
4. Run `bundle exec rspec` in a JRuby container (§3.1).
5. `--config.test_and_exit` smoke test inside `docker.elastic.co/logstash/logstash:9.4.4` (§3.4).
6. For AWS-touching changes: eyeball `S3::ClientFactory` and `SQS::Poller` for
   missed error rescues.
7. Update `CHANGELOG.md` on merge if the PR did not.

---

## 7. Hard rules for agents

- **Never** commit unless the user explicitly asks.
- **Never** bump the gem version or edit `CHANGELOG.md` unless doing a release.
- **Never** re-introduce `logstash-mixin-aws` — the mixin is intentionally
  inlined.
- **Never** run `publish_gem` — that is a maintainer action.
- **Never** use MRI Ruby to run tests; results are meaningless.
- Prefer podman over docker on this machine (`docker` is not installed;
  `podman` is at `/opt/homebrew/bin/podman`, machine `podman-machine-default`
  is running).
