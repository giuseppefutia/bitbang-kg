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

class ChicagoContractsImporter(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)    
    
    @staticmethod
    def get_csv_size(contracts_file, encoding="utf-8"):
        return sum(1 for row in ChicagoContractsImporter.get_rows(contracts_file))
    
    @staticmethod
    def get_rows(contracts_file):
        contracts = pd.read_csv(contracts_file)
        contracts['DATA_SOURCE'] = "CONTRACTS"
        contracts['RECORD_TYPE'] = "CONTRACT"
        contracts.insert(0, 'RECORD_ID', range(0, 0 + len(contracts)))
        if 'RECORD_ID' not in contracts:
            contracts.insert(0, 'RECORD_ID', range(0, 0 + len(contracts)))
        contracts.replace({np.nan: None}, inplace=True)
        for _, row in contracts.iterrows():
            yield row.to_dict()
    
    def set_constraints(self):
        queries = ["CREATE CONSTRAINT contract_record_pk IF NOT EXISTS FOR (node:ContractRecord) REQUIRE node.pk IS UNIQUE",
                   "CREATE INDEX contract_record_contract_id IF NOT EXISTS FOR (node:ContractRecord) ON (node.contractId)",
                   "CREATE CONSTRAINT contract_id IF NOT EXISTS FOR (node:Contract) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT procurement_type_id IF NOT EXISTS FOR (node:ProcurementType) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT department_id IF NOT EXISTS FOR (node:Department) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT contract_type IF NOT EXISTS FOR (node:ContractType) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT organization_id IF NOT EXISTS FOR (node:Organization) REQUIRE node.id IS UNIQUE",
                   "CREATE CONSTRAINT address_id IF NOT EXISTS FOR (node:Address) REQUIRE node.id IS UNIQUE",
                   "CREATE FULLTEXT INDEX organization_name IF NOT EXISTS FOR (node:Organization) ON EACH [node.name]"]
        for q in queries:
            with self._driver.session(database=self.database) as session:
                session.run(q)
    
    def import_contract_records(self, contracts_file):
        contract_records_query = """
        UNWIND $batch as item
        MERGE (n:ContractRecord {pk: item.RECORD_ID})
        SET n.name = item.`Purchase Order Description`
        SET n.amount = item.`Award Amount`
        SET n.startDate = item.`Start Date`
        SET n.endDate = item.`End Date`
        SET n.approvalDate = item.`Approval Date`
        SET n.pdfFile = item.`Contract PDF`
        SET n.vendorId = item.`Vendor ID`
        SET n.contractId = item.`Purchase Order (Contract) Number`
        SET n.specificationId = item.`Specification Number`
        SET n.source = item.DATA_SOURCE
        """
        size = self.get_csv_size(contracts_file)
        self.batch_store(contract_records_query, self.get_rows(contracts_file), size=size)
    
    def merge_vendors_and_orders(self, contracts_file):
        vendors_and_orders_query = """
        UNWIND $batch as item
        MERGE (n:ContractRecord {contractId: item.`Purchase Order (Contract) Number`})
        
        MERGE (m:Contract {id: item.`Purchase Order (Contract) Number`})
        SET m.names = apoc.coll.toSet(coalesce(m.names, [])  + coalesce(item.`Purchase Order Description`, []))
        
        MERGE (o:Organization {id: item.`Vendor ID`})
        SET o.names = apoc.coll.toSet(coalesce(o.names, [])  + coalesce(item.`Vendor Name`, []))
        SET o.source = item.DATA_SOURCE
        SET o.addresses = apoc.coll.toSet(coalesce(o.addresses, [])  + coalesce(item.`Address 1`, []) + coalesce(item.`Address 2`, []))
        SET o.addressPostalCodes = apoc.coll.toSet(coalesce(o.addressPostalCodes, [])  + coalesce(toString(item.Zip), []))
        SET o.addressStates = apoc.coll.toSet(coalesce(o.addressStates, [])  + coalesce(item.State, []))
        SET o.addressCities = apoc.coll.toSet(coalesce(o.addressCities, [])  + coalesce(item.City, []))
        SET o.name = trim(apoc.text.capitalize(reduce(shortest = head(o.names), name IN o.names | CASE WHEN size(name) < size(shortest) THEN name ELSE shortest END)))
        
        MERGE (a:Address {id: coalesce(item.`Address 1`, "Unknown)")})
        SET a.addressPostalCode = toString(item.Zip)
        SET a.addressState = item.State
        SET a.addressCity = item.City
        
        MERGE (t:ProcurementType {id: coalesce(item.`Procurement Type`, "Unknown")})
        
        MERGE (n)-[:INCLUDED_IN_CONTRACT]->(m)
        MERGE (n)-[:HAS_VENDOR]->(o)
        MERGE (n)-[:HAS_PROCUREMENT_TYPE]->(t)
        MERGE (o)-[r:HAS_ADDRESS]->(a)
        SET r.source = item.DATA_SOURCE
        """
        size = self.get_csv_size(contracts_file)
        self.batch_store(vendors_and_orders_query, self.get_rows(contracts_file), size=size)
    
    def merge_departments_contract_types(self, contracts_file):
        departments_contract_types_query = """
        UNWIND $batch as item
        MERGE (n:Contract {id: item.`Purchase Order (Contract) Number`})
        SET n.name = apoc.text.join(n.names, " + ")
        
        MERGE (m:Department {id: coalesce(item.Department, "Unknown")})
        MERGE (o:ContractType {id: coalesce(item.`Contract Type`, "Unknown")})

        MERGE (m)-[:ASSIGNS_CONTRACT]->(n)
        MERGE (n)-[:HAS_CONTRACT_TYPE]->(o)
        """
        size = self.get_csv_size(contracts_file)
        self.batch_store(departments_contract_types_query, self.get_rows(contracts_file), size=size)

if __name__ == '__main__':
    importing = ChicagoContractsImporter(argv=sys.argv[1:])
    base_path = importing.source_dataset_path

    if not base_path:
        print("source path directory is mandatory. Setting it to default.")
        base_path = "../dataset/chicago/"

    base_path = Path(base_path)

    if not base_path.is_dir():
        print(base_path, "isn't a directory")
        sys.exit(1)

    contracts_dat = base_path / "Contracts_20240103.csv"

    if not contracts_dat.is_file():
        print(contracts_dat, "doesn't exist in ", base_path)
        sys.exit(1)

    logging.info("Setting constraints...")
    importing.set_constraints()
    logging.info("Importing contract records...")
    importing.import_contract_records(contracts_dat)
    logging.info("Merging vendors and orders...")
    importing.merge_vendors_and_orders(contracts_dat)
    logging.info("Merging departments and contract types...")
    importing.merge_departments_contract_types(contracts_dat)
    importing.close()
