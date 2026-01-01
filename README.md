# gallery-dl Queue

An interactive command-line wrapper around [gallery-dl](https://github.com/mikf/gallery-dl) that focuses on reliability, ergonomics, and session persistence.

Current version: **2.1**

## Highlights

- **Persistent Queue & Sessions** – Save and restore pending URLs, failed download metadata, completed URLs, and per-URL directory overrides.
- **Smart Worker Orchestration** – Pause/resume globally, auto-start workers if pending URLs exist, and retry failed jobs across restarts.
- **Live Monitoring** – Real-time worker log streaming with progress indicators.
- **Robust Failure Handling** – Automatic retries with configurable delays, failed-download tracking, and `retry` command to requeue everything that failed (even across restarts).
- **Flexible Directory Control** – Session-wide default directory and per-URL overrides; optional per-worker directories when you really need them.
- **CLI UX** – Command aliases, async status updates, auto-update checks for gallery-dl, and colorized outputs that don’t interfere with workers.

## Key Commands

- `start` / `pause` / `resume` – Control worker threads.
- `worker <id>` / `worker all` – Follow live logs for one worker or all workers simultaneously.
- `workerdir [id] [path]` – Set the session default directory (recommended) or an optional per-worker directory.
- `retry` – Requeue every failed download tracked during this or prior sessions.
- `save <file>` / `load <file>` – Persist or restore the queue state (pending URLs, configuration, per-URL overrides, failed entries).

## Workflow Overview

1. Add URLs directly at the prompt (the shell validates, deduplicates, and records sesion history).
2. Use `workerdir` to set a session default directory (recommended).
3. Run `start` (or let the auto-start timer kick in) to spawn worker threads; each worker launches gallery-dl with enforced `base-directory` and progress capture.
4. Use `worker <id>` or `worker all` to observe stdout/stderr output and live progress without disrupting the download processes.
5. If a download fails, it’s tracked and can be retried explicitly or automatically across sessions.

## Requirements

- Python 3.9+
- `gallery-dl` binary accessible via PATH or configured in `config.gallery_dl_path`
- Optional: `colorama` (auto-detected) for cross-platform colored output

## Getting Started

```bash
python gallery-dl.queue.py
```

CLI help (prints a short explanation and exits):

```bash
python gallery-dl.queue.py --help
```

From the prompt:

```bash
> config workers 4                  # tune worker count
> workerdir "F:\\Downloads\\Art"      # session default directory
> https://example.com/some/gallery  # queue URLs directly
> start                             # workers will also auto-start after 5s
> worker all                        # follow every worker’s log + progress
```

Press `Ctrl+C` while inside `worker`/`worker all` views to return to the main prompt without interrupting downloads. Use `quit` when you’re finished (remember to `save` if you want to resume later). For the full list of commands (and all aliases), run `?` inside the shell to open the built-in help sheet.

## Session Files

Session files are JSON formatted.

- URLs are stored grouped by path in a human-readable map. Example key: `pending_by_path`.
- Shape: `<path> -> { "1": "<url>", "2": "<url>" }`
- `""` (empty string) is used for “no directory / default”.

## Command Aliases

Full list of aliases (alias → canonical command):

| Alias | Command | What it does |
| --- | --- | --- |
| `run` | `start` | Start workers and begin downloading |
| `go` | `start` | |
| `begin` | `start` | |
| `hold` | `pause` | Pause all workers |
| `stop` | `pause` | |
| `continue` | `resume` | Resume workers or reload last session when idle |
| `unpause` | `resume` | |
| `reload` | `resume` | |
| `recover` | `resume` | |
| `resume-session` | `resume` | |
| `stat` | `status` | Show detailed status report |
| `info` | `status` | |
| `log` | `worker` | Follow live stdout/stderr for a worker |
| `tail` | `worker` | |
| `feed` | `worker` | |
| `workers` | `worker` | |
| `wd` | `workerdir` | Set session default directory (or optional per-worker dirs) |
| `workdir` | `workerdir` | |
| `dir` | `workerdir` | |
| `hist` | `history` | Show recent download history |
| `recent` | `history` | |
| `cfg` | `config` | View or change configuration |
| `conf` | `config` | |
| `settings` | `config` | |
| `sv` | `save` | Save session to file |
| `write` | `save` | |
| `export` | `save` | |
| `ld` | `load` | Load session from file |
| `restore` | `load` | |
| `open` | `load` | |
| `queue` | `pending` | List pending URLs |
| `pend` | `pending` | |
| `list` | `pending` | |
| `links` | `urls` | Show all tracked URLs |
| `all` | `urls` | |
| `rm` | `remove` | Remove a URL from pending |
| `del` | `remove` | |
| `delete` | `remove` | |
| `clear` | `cls` | Clear the terminal display |
| `cls` | `cls` | |
| `clq` | `clearqueue` | Clear pending queue |
| `clearqueue` | `clearqueue` | |
| `clear-queue` | `clearqueue` | |
| `flush` | `clearqueue` | |
| `reset` | `clearqueue` | |
| `exit` | `quit` | Exit the shell |
| `q` | `quit` | |
| `x` | `quit` | |
| `redo` | `retry` | Re-queue only failed downloads |
| `rerun` | `retry` | |
| `retry-all` | `retry` | |
| `replay` | `retry` | |
| `retryall` | `retryall` | Re-queue all completed URLs |
| `clip` | `clipboard` | Toggle clipboard auto-ingest |
| `cb` | `clipboard` | |
