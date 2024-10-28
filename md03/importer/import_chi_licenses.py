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

class ChicagoLicensesImporter(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)

    @staticmethod
    def get_csv_size(licenses_file, encoding="utf-8"):
        return sum(1 for row in ChicagoLicensesImporter.get_rows(licenses_file))
    
    @staticmethod
    def get_rows(licenses_file):
        licenses = pd.read_csv(licenses_file)
        licenses['DATA_SOURCE'] = "LICENSES"
        licenses['RECORD_TYPE'] = "LICENSE"
        if 'RECORD_ID' not in licenses:
            licenses.insert(0, 'RECORD_ID', range(1000000, 1000000 + len(licenses)))
        licenses.replace({np.nan: None}, inplace=True)
        for _, row in licenses.iterrows():
            yield row.to_dict()
    
    def set_constraints(self):
        queries = ["CREATE CONSTRAINT license_record_pk IF NOT EXISTS FOR (node:LicenseRecord) REQUIRE node.pk IS UNIQUE",
                   "CREATE CONSTRAINT license_type_id IF NOT EXISTS FOR (node:LicenseType) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT organization_id IF NOT EXISTS FOR (node:Organization) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT address_id IF NOT EXISTS FOR (node:Address) REQUIRE node.id IS UNIQUE",
                   "CREATE FULLTEXT INDEX organization_name IF NOT EXISTS FOR (node:Organization) ON EACH [node.name]",
                   "CREATE INDEX organization_component_id IF NOT EXISTS FOR (node:Organization) ON (node.componentId)",
                   "CREATE INDEX organization_group_cluster_id IF NOT EXISTS FOR (node:OrganizationGroup) ON (node.clusterId)"]
        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_license_records(self, licenses_file):
        import_licenses_query = """
        UNWIND $batch as item
        MERGE (n:LicenseRecord {pk: item.RECORD_ID})
        SET n.id = item.`LICENSE ID`
        SET n.name = coalesce(item.`LEGAL NAME`, item.`DOING BUSINESS AS NAME`)
        SET n.businessName = coalesce(item.`DOING BUSINESS AS NAME`, '-')
        SET n.businessId = item.`ACCOUNT NUMBER`
        SET n.address = item.ADDRESS
        SET n.addressPostalCode = item.`ZIP CODE`
        SET n.addressState = item.STATE
        SET n.addressCity = item.CITY
        SET n.source = item.DATA_SOURCE
        SET n.amount = item.`Award Amount`
        SET n.date = item.`Approval Date`
        SET n.startDate = item.`LICENSE TERM START DATE`
        SET n.endDate = item.`LICENSE TERM EXPIRATION DATE`
        SET n.status = item.`LICENSE STATUS`
        SET n.code = item.`LICENSE CODE`
        SET n.number = item.`LICENSE NUMBER`
        SET n.siteNumber = item.`SITE NUMBER`
        SET n.latitude = item.LATITUDE
        SET n.longitude = item.LONGITUDE
        """
        size = self.get_csv_size(licenses_file)
        self.batch_store(import_licenses_query, self.get_rows(licenses_file), size=size)
    
    def import_license_type(self, licenses_file):
        import_license_type_query = """
        UNWIND $batch as item
        MERGE (n:LicenseType {id: item.`LICENSE CODE`})
        SET n.description = item.`LICENSE DESCRIPTION`
        """
        size = self.get_csv_size(licenses_file)
        self.batch_store(import_license_type_query, self.get_rows(licenses_file), size=size)
    
    def connect_license_to_type(self, licenses_file):
        connect_license_to_type_query = """
        UNWIND $batch as item
        MERGE (n:LicenseType {id: item.`LICENSE CODE`})
        MERGE (m:LicenseRecord {pk: item.RECORD_ID})
        MERGE (m)-[:HAS_LICENSE_TYPE]->(n)
        """
        size = self.get_csv_size(licenses_file)
        self.batch_store(connect_license_to_type_query, self.get_rows(licenses_file), size=size)
    
    def import_organization(self, licenses_file):
        import_organization_query = """
        UNWIND $batch as item
        MERGE (o:Organization {id: item.`ACCOUNT NUMBER`})
        SET o.names = apoc.coll.toSet(coalesce(o.names, []) + coalesce(item.`LEGAL NAME`, item.`DOING BUSINESS AS NAME`, []))
        SET o.otherNames = apoc.coll.toSet(coalesce(o.otherNames, []) + coalesce(item.`DOING BUSINESS AS NAME`, []))
        SET o.source = item.DATA_SOURCE
        SET o.addresses = apoc.coll.toSet(coalesce(o.addresses, []) + coalesce(item.ADDRESS, []))
        SET o.addressPostalCodes = apoc.coll.toSet(coalesce(o.addressPostalCodes, []) + coalesce(toString(item.`ZIP CODE`), [])) // Need to have the same data type
        SET o.addressStates = apoc.coll.toSet(coalesce(o.addressStates, []) + coalesce(item.STATE, []))
        SET o.addressCities = apoc.coll.toSet(coalesce(o.addressCities, []) + coalesce(item.CITY, []))

        MERGE (a:Address {id: item.ADDRESS})
        SET a.addressPostalCode = toString(item.`ZIP CODE`)
        SET a.addressState = item.STATE
        SET a.addressCity = item.CITY
        SET a.latitude = item.LATITUDE
        SET a.longitude = item.LONGITUDE

        MERGE (o)-[r:HAS_ADDRESS]->(a)
        SET r.source = item.DATA_SOURCE
        """
        size = self.get_csv_size(licenses_file)
        self.batch_store(import_organization_query, self.get_rows(licenses_file), size=size)
    
    def connect_org_to_license(self, licenses_file):
        connect_license_to_org_query = """
        UNWIND $batch as item
        MERGE (n:Organization {id: item.`ACCOUNT NUMBER`})
        SET n.name = trim(apoc.text.capitalize(reduce(shortest = head(n.names), name IN n.names | CASE WHEN size(name) < size(shortest) THEN name ELSE shortest END)))
        MERGE (m:LicenseRecord {pk: item.RECORD_ID})
        MERGE (n)-[:ORG_HAS_LICENSE]->(m)
        """
        size = self.get_csv_size(licenses_file)
        self.batch_store(connect_license_to_org_query, self.get_rows(licenses_file), size=size)
    
    def connect_people_to_org(self, licenses_file):
        connect_people_to_org_query = """
        UNWIND $batch as item
        MERGE (n:Organization {id: item.`ACCOUNT NUMBER`})
        WITH n, item
        MATCH (m:PersonRecord {employerId: item.`ACCOUNT NUMBER`})-[:RECORD_RESOLVED_TO]->(p:Person)
        MERGE (p)-[r:BELONGS_TO_ORG]->(n)
        SET r.roles = p.titles
        """
        size = self.get_csv_size(licenses_file)
        self.batch_store(connect_people_to_org_query, self.get_rows(licenses_file), size=size)

if __name__ == '__main__':
    importing = ChicagoLicensesImporter(argv=sys.argv[1:])
    base_path = importing.source_dataset_path

    if not base_path:
        print("source path directory is mandatory. Setting it to default.")
        base_path = "../dataset/chicago/"

    base_path = Path(base_path)

    if not base_path.is_dir():
        print(base_path, "isn't a directory")
        sys.exit(1)

    licenses_dat = base_path / "Business_Licenses_20240103.csv"

    if not licenses_dat.is_file():
        print(licenses_dat, "doesn't exist in ", base_path)
        sys.exit(1)

    logging.info("Setting constraints...")
    importing.set_constraints()
    logging.info("Importing license records...")
    importing.import_license_records(licenses_dat)
    logging.info("Importing license types...")
    importing.import_license_type(licenses_dat)
    logging.info("Connecting licenses to types...")
    importing.connect_license_to_type(licenses_dat)
    logging.info("Importing organizations...")
    importing.import_organization(licenses_dat)
    logging.info("Connecting organizations to licenses...")
    importing.connect_org_to_license(licenses_dat)
    logging.info("Connecting people to organizations...")
    importing.connect_people_to_org(licenses_dat)
    importing.close()
