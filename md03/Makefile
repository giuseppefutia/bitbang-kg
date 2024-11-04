PIP=../venv/bin/pip
PYTHON=../venv/bin/python

init:
	$(PIP) install -r requirements.lock

import:
	@echo "Starting import process..."
	@start_time=$$(date +%s); \
	PYTHONPATH=../ $(PYTHON) importer/import_chi_people.py; \
	PYTHONPATH=../ $(PYTHON) importer/import_chi_people_cluster.py; \
	PYTHONPATH=../ $(PYTHON) importer/import_chi_licenses.py; \
	PYTHONPATH=../ $(PYTHON) importer/import_chi_contracts.py; \
	PYTHONPATH=../ $(PYTHON) importer/import_chi_orgs_cluster.py; \
	end_time=$$(date +%s); \
	total_time=$$((end_time - start_time)); \
	echo "Total import process time: $${total_time}s"
	@echo "Import process completed."

cdc:
	PYTHONPATH=../ $(PYTHON) importer/cdc_service.py

simulate:
	PYTHONPATH=../ $(PYTHON) importer/batch_new_node_simulation.py

clean:
	PYTHONPATH=../ $(PYTHON) importer/batch_new_node_clean.py