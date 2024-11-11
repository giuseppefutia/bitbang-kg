PIP=../venv/bin/pip
PYTHON=../venv/bin/python

init:
	$(PIP) install -r requirements.lock

import:
	PYTHONPATH=../ $(PYTHON) importer/import_rac_diaries.py
	PYTHONPATH=../ $(PYTHON) importer/import_rac_gpt.py
	PYTHONPATH=../ $(PYTHON) importer/import_rac_gds.py

agent-llm:
	PYTHONPATH=../ $(PYTHON) rag/agent_llm.py "$(question)"

agent-rag:
	PYTHONPATH=../ $(PYTHON) rag/agent_rag.py "$(question)"

agent-graph-rag:
	PYTHONPATH=../ $(PYTHON) rag/agent_graph_rag.py "$(question)"
