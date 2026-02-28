# gallery-dl Queue

An interactive command-line wrapper around [gallery-dl](https://github.com/mikf/gallery-dl) that focuses on reliability, ergonomics, and session persistence.

Current version: **2.2**

## Highlights

- **Persistent Queue & Sessions** – Save and restore pending URLs, failed download metadata, completed URLs, and per-URL directory overrides. Crash-safe temp state is written on every URL change and can be recovered on restart.
- **Smart Worker Orchestration** – Pause/resume globally, auto-start workers after 5 s, live worker resize, and retry failed jobs across restarts.
- **Live Monitoring** – Real-time per-worker log streaming (`worker <id>` / `worker all`) with progress indicators and transfer-rate estimates.
- **Robust Failure Handling** – Automatic retries with configurable delays, failed-download tracking, `retry` / `retryall` commands, and intelligent duplicate suppression across sessions.
- **Clipboard Auto-Ingest** – Watch the clipboard for URLs matching a whitelisted domain and add them automatically. Combined with autofolder via the `auto` convenience command.
- **Autofolder** – Probe remote metadata (`gallery-dl -j --no-download --range 1`) to derive a sanitized sub-directory name (album/gallery/title) for each URL.
- **Download Validation** – Optional metadata precheck (`gallery-dl -j --no-download`) to detect bogus/0-byte items _before_ downloading; post-download size validation against expected sizes.
- **Flexible Directory Control** – Session-wide default directory, per-URL overrides, optional per-worker directories, and autofolder-derived paths.
- **Auto-Update** – Background check for new gallery-dl releases on GitHub; downloads and replaces the binary automatically (daily check, rate-limit aware).
- **CLI UX** – Command aliases, tab completion (with `pyreadline3` on Windows), async status updates, colorized output (via `colorama`), and a dynamic console title showing queue state.

## Key Commands

| Command | Description |
|---------|-------------|
| `start` | Start worker threads and begin downloading |
| `pause` / `resume` | Pause or resume all workers |
| `status` | Show detailed status report (workers, queue, transfer stats) |
| `worker <id>` / `worker all` | Follow live logs and progress for one or all workers |
| `workerdir [path]` | Set the session default download directory |
| `clipboard [on\|off]` | Toggle clipboard auto-ingest (learns whitelisted domain from next manual add) |
| `af [on\|off]` | Toggle autofolder mode (derive sub-directory from remote metadata) |
| `auto [on\|off]` | Convenience: toggle both autofolder and clipboard together |
| `save [file]` / `load [file]` | Persist or restore the queue state |
| `retry` | Re-queue all failed downloads |
| `retryall` | Re-queue all completed downloads for re-downloading |
| `pending` | List pending URLs |
| `urls` | Show all tracked URLs |
| `remove <url>` | Remove a URL from the pending list |
| `config [key] [value]` | View or change configuration at runtime |
| `history` | Show recent download history |
| `clearqueue` | Clear the pending queue |
| `cls` | Clear the terminal display |
| `quit` | Exit the application (auto-saves if enabled) |

## Workflow Overview

1. **Add URLs** directly at the prompt (the shell validates, deduplicates, and records session history).
2. **Set a download directory** with `workerdir <path>` (recommended).
3. **Optionally enable smart modes:**
   - `clipboard on` – arm clipboard monitoring; the first manual URL sets the whitelisted domain, then matching URLs are ingested automatically.
   - `af on` – enable autofolder to organize downloads by remote album/gallery name.
   - `auto on` – enable both clipboard and autofolder in one command.
4. **Start downloading** with `start` (or let the auto-start timer fire after 5 s when pending URLs exist).
5. **Monitor** with `worker <id>` or `worker all` to observe stdout/stderr and live progress. Press Ctrl+C to return to the prompt without interrupting downloads.
6. **Handle failures** – failed downloads are tracked automatically and can be retried with `retry`. Use `retryall` to re-download everything.
7. **Save & quit** – `save` to persist the session; `quit` auto-saves if `auto_save` is enabled. On crash/Ctrl+C, temp state is saved and offered for recovery on next start.

## Clipboard & Autofolder

### Clipboard Monitor

The clipboard monitor watches for URLs in the system clipboard and adds them to the queue automatically.

- **Armed mode** (default on first enable): The next manually added URL sets the whitelisted domain. After that, only URLs matching that domain are auto-ingested from the clipboard.
- **Whitelist persists** across enable/disable cycles within a session.
- The watcher and consumer run as separate threads: the watcher polls the clipboard (~0.6 s interval), the consumer processes URLs (including autofolder probes if enabled).

### Autofolder

When enabled, every URL added (manually or via clipboard) is probed with `gallery-dl -j --no-download --range 1` to obtain remote metadata. The album/gallery/title is used as a sanitized sub-directory name. Falls back to the default directory if no name can be determined.

### Auto Command

`auto on` enables both clipboard and autofolder. `auto off` disables both. `auto status` shows the state of each.

## Configuration

All options can be viewed and changed at runtime with `config [key] [value]`.

| Option | Default | Description |
|--------|---------|-------------|
| `workers` | `4` | Number of concurrent download worker threads (live resizable) |
| `max_retries` | `2` | Maximum retry attempts per URL |
| `retry_delay` | `60` | Seconds to wait between retries |
| `download_dir` | `None` | Default download directory (overridden by `workerdir`) |
| `gallery_dl_path` | `None` | Path to gallery-dl binary (auto-detected from PATH if unset) |
| `default_args` | `[]` | Extra arguments passed to every gallery-dl invocation |
| `auto_save` | `true` | Automatically save session on `quit` |
| `session_file` | `gallery_dl_session.json` | Default session file path |
| `config_file` | `gallery_dl_queue_config.json` | Queue configuration file path |
| `auto_status` | `false` | Print status after each add |
| `flat_directories` | `true` | Use flat directory layout (`extractor.directory=[]`) |
| `directory_template` | `None` | Custom `extractor.directory` template (overrides `flat_directories`) |
| `keep_failed` | `true` | Track failed downloads for later retry |
| `dump_json` | `false` | Dump gallery-dl JSON metadata before downloading |
| `precheck_no_download` | `true` | Run `gallery-dl -j --no-download` before downloading to validate metadata |
| `precheck_skip_download_on_fail` | `true` | Skip download if precheck detects suspicious items |
| `precheck_probe_remote_sizes` | `true` | Probe remote file sizes via HEAD/Range requests |
| `precheck_probe_limit` | `10` | Max number of remote files to probe per URL |
| `validate_expected_sizes` | `true` | Validate downloaded file sizes against precheck metadata |
| `expected_size_tolerance` | `0` | Allowed size deviation in bytes |

## Session Files

Session files are JSON formatted (format v3).

- URLs are stored grouped by path in a human-readable map.
- Shape: `<path> -> { "1": "<url>", "2": "<url>" }`
- `""` (empty string) represents "no directory / default".
- Sections: `pending_by_path`, `completed_by_path`, `failed_by_path`
- Additional fields: `all_urls`, `url_directory_overrides`, `worker_directories`, `broken`, `autofolder_enabled`, `config`

### Temp State (Crash Recovery)

A lightweight temp file (`<session_file>.temp.json`) is written on every URL add/complete. On startup, if unsaved URLs are found, the user is prompted to restore them. This survives crashes and Ctrl+C interrupts.

## Interrupt Handling

- **Ctrl+C at the main prompt**: Saves temp state, stops clipboard/title threads, terminates all gallery-dl processes, and exits cleanly.
- **Ctrl+C inside `worker`/`worker all`**: Returns to the main prompt without interrupting downloads.

## Requirements

- Python 3.9+
- `gallery-dl` binary accessible via PATH or configured with `config gallery_dl_path`
- Optional: `colorama` (auto-detected) for cross-platform colored output
- Optional: `pyreadline3` for tab completion on Windows

## Getting Started

```bash
python gallery-dl.queue.py
```

CLI help:

```bash
python gallery-dl.queue.py --help
```

From the prompt:

```bash
> config workers 4                  # tune worker count
> workerdir "F:\Downloads\Art"      # session default directory
> auto on                           # enable clipboard + autofolder
> https://example.com/some/gallery  # queue URLs directly (arms clipboard whitelist)
> start                             # workers also auto-start after 5s
> worker all                        # follow every worker's log + progress
```

For the full list of commands and aliases, type `?` inside the shell.

## Command Aliases

| Alias | Command |
|-------|---------|
| `run`, `go`, `begin` | `start` |
| `hold`, `stop` | `pause` |
| `continue`, `unpause`, `reload`, `recover`, `resume-session` | `resume` |
| `stat`, `info` | `status` |
| `log`, `tail`, `feed`, `workers` | `worker` |
| `wd`, `workdir`, `dir` | `workerdir` |
| `hist`, `recent` | `history` |
| `cfg`, `conf`, `settings` | `config` |
| `sv`, `write`, `export` | `save` |
| `ld`, `restore`, `open` | `load` |
| `queue`, `pend`, `list` | `pending` |
| `links`, `all` | `urls` |
| `rm`, `del`, `delete` | `remove` |
| `clear` | `cls` |
| `clq`, `clear-queue`, `flush`, `reset` | `clearqueue` |
| `exit`, `q`, `x` | `quit` |
| `redo`, `rerun`, `replay` | `retry` |
| `retry-all` | `retryall` |
| `clip`, `cb` | `clipboard` |
| `autofolder` | `af` |
| `automode` | `auto` |
