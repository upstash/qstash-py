# QStash Python SDK — Agent Instructions

## SDK Design Instructions

See `.github/instructions/qstash-sdk-design.instructions.md` for the canonical list of locations that must be updated when adding a new parameter.

## Checklist: Adding a New Parameter

When adding a new parameter to the SDK, follow this checklist **in full** before considering the work done.

### 1. Update all required locations

Refer to `.github/instructions/qstash-sdk-design.instructions.md` for the full list. The key rule: **every location listed there must be evaluated**. Not every parameter applies to every location (e.g. a request-only parameter won't appear in response dataclasses), but you must consciously check each one.

Common locations that get missed:

- **`qstash/asyncio/`** — The async variants (`asyncio/message.py`, `asyncio/schedule.py`) must mirror the sync API exactly. If you add a parameter to `MessageApi.publish`, you must also add it to `AsyncMessageApi.publish`, and so on for `publish_json`, `enqueue`, `enqueue_json`, `create`, `create_json`.
- **`convert_to_batch_messages`** in `qstash/message.py` — This function converts `BatchJsonRequest` to `BatchRequest`. If the parameter is in both TypedDicts, this function must copy it.
- **Response parsing** (`parse_schedule_response`, `parse_message_response`, `parse_logs_response`, `parse_dlq_message_response`) — If the API returns the parameter in responses, it must be added to the response dataclass and parse function.
- **Filter TypedDicts** (`LogFilter`, `DlqFilter`) and their param builders (`prepare_list_logs_request_params`, `prepare_list_dlq_messages_params`) — If the parameter is filterable.

### 2. Run all checks before submitting

Always run the full check suite **before** considering work complete:

```sh
poetry run pytest
poetry run ruff format .
poetry run ruff check .
poetry run mypy --show-error-codes .
```

mypy will catch missing arguments (like `[call-arg]` errors for missing named arguments). Running it would have caught the asyncio omissions immediately.

### 3. Verify asyncio parity

After making changes to any file in `qstash/`, diff the sync and async versions to confirm parity:

- `qstash/message.py` ↔ `qstash/asyncio/message.py`
- `qstash/schedule.py` ↔ `qstash/asyncio/schedule.py`
