PIP=../venv/bin/pip
PYTHON=../venv/bin/python

init:
	$(PIP) install -r requirements.lock

import:
	PYTHONPATH=../ $(PYTHON) importer/import_rac_diaries.py
	PYTHONPATH=../ $(PYTHON) importer/import_rac_gpt.py
	PYTHONPATH=../ $(PYTHON) importer/import_rac_gds.py

agent:
	PYTHONPATH=../ $(PYTHON) rag/agent.py "$(question)"