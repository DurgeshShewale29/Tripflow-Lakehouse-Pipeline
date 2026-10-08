# TripFlow Lakehouse Pipeline

Local medallion pipeline that lands trip data in Delta Lake and refines it from bronze through gold.

## Folder structure

`data/` is gitignored. The layer directories below are created locally; empty folders keep a `.gitkeep` until they hold files.

```
data/
  raw/
  bronze/
  silver/
  gold/
  quarantine/
src/
  common/
  bronze/
  silver/
  gold/
  optimize/
flows/
tests/
sql/
docs/
.github/workflows/
```

## Testing

From the project root, with the virtual environment active:

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

GitHub Actions runs the same `pytest -q` suite on Ubuntu.

