# UD Sync

This project aims at exploring and syncing UD-like morphosyntactic annotations of manually annotated Hungarian corpora.

## Installation

```bash
poetry install
```

## Usage

Create diff:
```bash
poetry run weasel run diff
```

Publish the DB to https://dbhub.io/oroszgy/hud_diff.sqlite

```bash
env $(cat .env | xargs) weasel run publish
```
