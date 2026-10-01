# Contributing

Thanks for your interest in improving this project. It's a proof of concept, so
contributions that make it more correct, more robust, or clearer are very
welcome.

## Getting set up

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp config/secrets.env.example config/secrets.env   # fill in for live runs
```

You can work on most things without Confluent credentials — the unit tests use
mocked API responses and an in-memory SQLite database.

## Running the tests

```bash
pytest -v
```

Please add or update tests for any behavior you change. The collectors and the
rules engine are designed to be testable without network access (see the
existing tests in `tests/` for the mocking patterns).

## Ground rules

- **Accuracy over guessing.** Don't hardcode Confluent API field names, metric
  names, limits, or behaviors you haven't verified against the current docs. If
  something is uncertain, say so in a comment and prefer the discovery endpoints
  (e.g. `/descriptors/metrics`) over assumptions.
- **Never commit secrets.** `config/secrets.env`, `.env`, and `data/` are
  gitignored — keep real keys, tokens, and org data out of commits and tests.
- **Treat cost figures as accrued estimates**, never final invoice values, in
  code, UI, and alerts.
- **Keep it least-privilege.** The service only needs read access to Confluent;
  don't add code that mutates Confluent resources.

## Reporting issues

For bugs, include what you ran, what you expected, what happened, and any logs
(with secrets redacted). For Confluent API behavior that seems wrong or
undocumented, see `docs/pain-points.md` for the format we use — documented facts
with links, observations with evidence, no inference.

## Submitting changes

1. Branch from `main`.
2. Make the change with tests and a clear commit message.
3. Open a pull request describing the what and the why.
