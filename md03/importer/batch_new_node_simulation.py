import logging
import sys
import time
from pathlib import Path
from datetime import datetime
import json

from util.base_importer import BaseImporter
from import_chi_people import ChicagoPeopleImporter
from importer.import_chi_people_cluster import ChicagoPeopleSimilarity
from cdc_service import CDCService

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class BatchProcessSimulator(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.cpi = ChicagoPeopleImporter(argv=sys.argv[1:])
        self.cps = ChicagoPeopleSimilarity(argv=sys.argv[1:])
    
    def start_cdc():
        pass

    def end_cdc():
        pass
    
    def import_batch(self):
        cpi = ChicagoPeopleImporter(argv=sys.argv[1:])
        base_path = cpi.source_dataset_path
        if not base_path:
            print("source path directory is mandatory. Setting it to default.")
            base_path = "../dataset/chicago/"
        
        base_path = Path(base_path)
        
        if not base_path.is_dir():
            print(base_path, "isn't a directory")
            sys.exit(1)

        owners_dat = base_path / "Owners_batch.csv"

        if not owners_dat.is_file():
            print(owners_dat, "doesn't exist in ", base_path)
            sys.exit(1)

        cpi.import_people_records(owners_dat)
        cpi.close()
    
    def catch_update(self, current_time):
        selectors = []
        cdc = CDCService(selectors)
        return cdc.detect_change(current_time)
    
    def apply_changes(self, updated=None, operation=None):
        if operation is None:
            sys.exit("Invalid operation.")
        if operation == "PROCESS_NEW_NODES":
            self.process_new_nodes(updated)
        elif operation == "MARK_AFFECTED_NODES":
            self.mark_affected_nodes(updated)
        elif operation == "REMOVE_RESOLVED_NODES":
            self.remove_resolved_nodes(updated)
        elif operation == "APPLY_NEW_RESOLUTION":
            self.resolve_new_records()
    
    def process_new_nodes(self, updated):
        events = [i['event'] for i in updated]
        new_nodes = [i["elementId"] for i in events]
        
        self.cps.create_people_similarity(new_nodes)
    
    def mark_affected_nodes(self, updated):
        events = [i['event'] for i in updated]
        start_nodes = [i["start"]["elementId"] for i in events if i["operation"] == "c"]
        end_nodes = [i["end"]["elementId"] for i in events if i["operation"] == "c"]
        affected = list(set(start_nodes + end_nodes))
        with self._driver.session(database=self.database) as session:
            session.run('MATCH (n:PersonRecord) WHERE elementId(n) in $affected SET n:Affected', {"affected": affected})
    
    def remove_resolved_nodes(self, updated):
        events = [i['event'] for i in updated]
        affected = [i["elementId"] for i in events]
        with self._driver.session(database=self.database) as session:
            session.run('MATCH (n:PersonRecord)-[:RECORD_RESOLVED_TO]->(p:Person) WHERE elementId(n) in $affected DETACH DELETE p', {"affected": affected})

    def resolve_new_records(self):
        self.cps.project_wcc_graph(node_label='Affected')
        
        # Run new WCC
        new_wcc_query = """
        WITH apoc.date.currentTimestamp() as timestamp
        CALL gds.wcc.stream('personWcc', {})
        YIELD nodeId, componentId

        WITH gds.util.asNode(nodeId) as n, componentId AS componentId, timestamp
        WITH n, apoc.text.join([toString(componentId), toString(timestamp)], "_") as component
        SET n.componentId = component
        """
        with self._driver.session(database=self.database) as session:
            session.run(new_wcc_query)
        
        # Create Person nodes, create connections, remove affected
        resolved_query = """
        MATCH (n:PersonRecord:Affected)
        WITH n, n.componentId as component
        MERGE (e:Person {clusterId: component})
        MERGE (n)-[:RECORD_RESOLVED_TO]->(e)
        REMOVE n:Affected
        """
        with self._driver.session(database=self.database) as session:
            session.run(resolved_query)
        # Delete projection
        self.cps.delete_wcc_projection()
    
    def clean_updates(self):
        with self._driver.session(database=self.database) as session:
            session.run('MATCH (n:PersonRecord) WHERE n.pk = 10000001 DETACH DELETE n')
        with self._driver.session(database=self.database) as session:
            session.run('MATCH (n:Affected) REMOVE n:Affected')

if __name__ == '__main__':
    simulator = BatchProcessSimulator(argv=sys.argv[1:])
    logging.info("Step 0 - Cleaning updates...")
    simulator.clean_updates()
    time.sleep(1)

    current_time = datetime.utcnow().isoformat()

    # Step 1 - Import
    logging.info("Step 1 - Importing records...")
    simulator.import_batch()

    # Step 2 - Create similarities
    logging.info("Step 2 - Detecting similarity between records...")
    catched_nodes = simulator.catch_update(current_time)
    current_time = datetime.utcnow().isoformat()
    simulator.apply_changes(catched_nodes, "PROCESS_NEW_NODES")

    # Step 3 - Mark affected nodes
    logging.info("Step 3 - Marking affected records...")
    catched_affected_nodes = simulator.catch_update(current_time)
    current_time = datetime.utcnow().isoformat()
    simulator.apply_changes(catched_affected_nodes, "MARK_AFFECTED_NODES")
    
    # Step 4 - Remove resolved entities
    logging.info("Step 4 - Removing resolved nodes touched by the affected ones...")
    catched_labeled_nodes = simulator.catch_update(current_time)
    current_time = datetime.utcnow().isoformat()
    simulator.apply_changes(catched_labeled_nodes, "REMOVE_RESOLVED_NODES")

    # Step 5 - Resolve new entities
    logging.info("Step 5 - Resolving new Person nodes...")
    simulator.apply_changes(operation="APPLY_NEW_RESOLUTION")
