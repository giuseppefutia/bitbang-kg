import sys
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class ChicagoPeopleImporter(BaseImporter):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)    
    
    @staticmethod
    def get_csv_size(owners_file, encoding="utf-8"):
        return sum(1 for row in ChicagoPeopleImporter.get_rows(owners_file))
    
    @staticmethod
    def get_rows(owners_file):
        owners = pd.read_csv(owners_file)
        owners['DATA_SOURCE'] = "OWNERS"
        owners['RECORD_TYPE'] = "PERSON"
        if 'RECORD_ID' not in owners:
            owners.insert(0, 'RECORD_ID', range(3000000, 3000000 + len(owners)))
        owners.replace({np.nan: None}, inplace=True)
        for _, row in owners.iterrows():
            yield row.to_dict()
    
    def set_constraints(self):
        queries = ["CREATE CONSTRAINT person_id IF NOT EXISTS FOR (node:Person) REQUIRE node.clusterId IS UNIQUE",
                   "CREATE CONSTRAINT person_record_pk IF NOT EXISTS FOR (node:PersonRecord) REQUIRE node.pk IS UNIQUE",
                   "CREATE INDEX person_record_component_id IF NOT EXISTS FOR (node:PersonRecord) ON (node.componentId)",
                   "CREATE INDEX person_record_employer_id IF NOT EXISTS FOR (node:PersonRecord) ON (node.employerId)",
                   "CREATE FULLTEXT INDEX person_record_fullName IF NOT EXISTS FOR (node:PersonRecord) ON EACH [node.fullName]"]

        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_people_records(self, owners_file):
        import_people_records_query = """
        UNWIND $batch as item
        MERGE (n:PersonRecord {pk: item.RECORD_ID})
        SET n.firstName = coalesce(item.`Owner First Name`, NULL)
        SET n.lastName = coalesce(item.`Owner Last Name`, NULL)
        SET n.middleName = coalesce(item.`Owner Middle Initial`, NULL)
        SET n.fullName = CASE
                            WHEN trim(apoc.text.regreplace(
                                coalesce(item.`Owner First Name`, '') + ' ' +
                                coalesce(item.`Owner Middle Initial`, '') + ' ' +
                                coalesce(item.`Owner Last Name`, ''), "\\s+", " ")) = ''
                            THEN NULL
                            ELSE apoc.text.capitalizeAll(toLower(trim(apoc.text.regreplace(
                                coalesce(item.`Owner First Name`, '') + ' ' +
                                coalesce(item.`Owner Middle Initial`, '') + ' ' +
                                coalesce(item.`Owner Last Name`, ''), "\\s+", " "))))
                         END
        SET n.source = item.DATA_SOURCE
        SET n.employerId = item.`Account Number`
        SET n.title = item.Title
        """
        size = self.get_csv_size(owners_file)
        self.batch_store(import_people_records_query, self.get_rows(owners_file), size=size)


if __name__ == '__main__':
    importing = ChicagoPeopleImporter(argv=sys.argv[1:])
    base_path = importing.source_dataset_path

    if not base_path:
        print("source path directory is mandatory. Setting it to default.")
        base_path = "../dataset/chicago/"

    base_path = Path(base_path)

    if not base_path.is_dir():
        print(base_path, "isn't a directory")
        sys.exit(1)

    owners_dat = base_path / "Business_Owners_20240103.csv"

    if not owners_dat.is_file():
        print(owners_dat, "doesn't exist in ", base_path)
        sys.exit(1)

    logging.info("Setting constraints...")
    importing.set_constraints()
    logging.info("Importing people records...")
    importing.import_people_records(owners_dat)
    importing.close()
