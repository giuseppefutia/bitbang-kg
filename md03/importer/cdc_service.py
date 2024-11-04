import logging
import json
import time
from threading import Thread

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class CDCService(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.cursor = self.current_change_id()
        self.selectors = []

    def apply_change(self, record):
        record_dict = {
            k: record.get(k)
            for k in ('id', 'txId', 'seq', 'event', 'metadata')
        }
        return record_dict
    
    def apply_change_test(self, record):
        record_dict = {
            k: record.get(k)
            for k in ('id', 'txId', 'seq', 'event', 'metadata')
        }
        print(json.dumps(record_dict, indent=2, default=repr))
        return record_dict

    def query_changes(self):
        with self._driver.session(database=self.database) as session:
            res = session.run('CALL db.cdc.query($cursor, $selectors)',
                              cursor=self.cursor, selectors=self.selectors)
            for record in res:
                self.apply_change_test(record)
            self.cursor = self.current_change_id()

    def earliest_change_id(self):
        with self._driver.session(database=self.database) as session:
            record = session.run('CALL db.cdc.earliest')
            return record.single()["id"]

    def current_change_id(self):
        with self._driver.session(database=self.database) as session:
            record = session.run('CALL db.cdc.current')
            return record.single()["id"]

    def detect_change(self, current_time):
        change_query = """
        CALL db.cdc.earliest() YIELD id AS earliestId
        CALL db.cdc.query(earliestId) YIELD txId, event, metadata
        WHERE datetime($current_time) < datetime(metadata.txCommitTime)
        RETURN txId, event, metadata ORDER BY metadata.txCommitTime DESC
        """
        with self._driver.session(database=self.database) as session:
            res = session.run(change_query, {"current_time": current_time})
            results = []
            for record in res:
                results.append(self.apply_change(record))
            return results
    
    def run(self):
        # Useful to test a CDC daemon
        while True:
            self.query_changes()
            time.sleep(5)

if __name__ == '__main__':
    # To define the selectors: https://neo4j.com/docs/cdc/current/procedures/selectors/
    selectors = [
        # {'select': 'n'}
    ]
    cdc = CDCService(selectors)
    logging.info("Waiting for changes...")
    cdc_thread = Thread(target=cdc.run, daemon=True)
    cdc_thread.start()
    cdc_thread.join()
    