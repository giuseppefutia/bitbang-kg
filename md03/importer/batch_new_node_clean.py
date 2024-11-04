import logging
import sys
import time

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class BatchProcessCleaner(BaseImporter):
        
    def clean_updates(self):
        with self._driver.session(database=self.database) as session:
            # Remove new nodes
            session.run("""MATCH (n:PersonRecord) WHERE n.newNode is NOT NULL DETACH DELETE n""")

            # Restore original information of record connected to the new cluster
            session.run("""
                        MATCH (p:Person)<-[:RECORD_RESOLVED_TO]-(r:PersonRecord)
                        WHERE p.newCluster IS NOT NULL
                        SET r.componentId = r.oldComponentId REMOVE r.oldComponentId
                        SET p.clusterId = r.componentId
                        SET p.fullNames = []
                        SET p.fullNames = coalesce(p.fullNames, []) + r.fullName
                        SET p.employerIds = []
                        SET p.employerIds = coalesce(p.employerIds, []) + r.employerId
                        SET p.titles = []
                        SET p.titles = coalesce(p.titles, []) + r.title
                        SET p.newCluster = NULL
                        SET p.name = reduce(shortest = head(p.fullNames), name IN p.fullNames | CASE WHEN size(name) < size(shortest) THEN name ELSE shortest END)
                        """)

if __name__ == '__main__':
    simulator = BatchProcessCleaner(argv=sys.argv[1:])
    
    logging.info("Cleaning database updates...")
    simulator.clean_updates()
    time.sleep(1)
