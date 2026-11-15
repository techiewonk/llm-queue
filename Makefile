# Every check goes through scripts/ (see scripts/check.py --list); nothing calls a real LLM.
PY = .venv/Scripts/python
ifeq ($(OS),Windows_NT)
else
PY = .venv/bin/python
endif

.PHONY: up rebuild down status check check-dev check-all

up:
	$(PY) scripts/stack.py up

rebuild:
	$(PY) scripts/stack.py rebuild

down:
	$(PY) scripts/stack.py down

status:
	$(PY) scripts/stack.py status

# make check S="a1 a2"
check:
	$(PY) scripts/check.py $(S)

# the dev run (lint and tests once, scenarios at 100 %, gates quick)
check-dev:
	$(PY) scripts/check.py dev

# before a push
check-all:
	$(PY) scripts/check.py all
