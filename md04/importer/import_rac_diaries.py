import sys
import json

from pathlib import Path

from util.base_importer import BaseImporter
from neo4j.exceptions import ClientError as Neo4jClientError

class RacDiariesImporter(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.set_constraints()

    def set_constraints(self):
        with self._driver.session(database=self.database) as session:
            query = """
                CREATE CONSTRAINT IF NOT EXISTS FOR (n:File) REQUIRE n.name IS NODE KEY;
                CREATE CONSTRAINT IF NOT EXISTS FOR (n:Page) REQUIRE n.id IS NODE KEY;
                CREATE TEXT INDEX node_entity_name IF NOT EXISTS FOR (n:Entity) ON (n.name);
                CREATE CONSTRAINT IF NOT EXISTS FOR (n:Person) REQUIRE n.name_normalized IS NODE KEY;
                CREATE CONSTRAINT IF NOT EXISTS FOR (n:Organization) REQUIRE n.name_normalized IS NODE KEY;
                CREATE CONSTRAINT IF NOT EXISTS FOR (n:Occupation) REQUIRE n.name_normalized IS NODE KEY;
                CREATE CONSTRAINT IF NOT EXISTS FOR (n:Title) REQUIRE n.name_normalized IS NODE KEY;
                CREATE TEXT INDEX IF NOT EXISTS FOR (n:Person) ON (n.name_normalized);
                CREATE TEXT INDEX IF NOT EXISTS FOR (n:Organization) ON (n.name_normalized);
                CREATE TEXT INDEX IF NOT EXISTS FOR (n:Occupation) ON (n.name_normalized);
                CREATE TEXT INDEX IF NOT EXISTS FOR (n:Title) ON (n.name_normalized);
                CREATE TEXT INDEX rel_text_entities IF NOT EXISTS FOR ()-[r:RELATED_TO_ENTITY]-() ON (r.type)"""
            for q in query.split(";"):
                try:
                    session.run(q)
                except Neo4jClientError as e:
                    # ignore if we already have the rule in place
                    if e.code != "Neo.ClientError.Schema.EquivalentSchemaRuleAlreadyExists":
                        raise e

    @staticmethod
    def count_diaries(diaries_file):
        return len(json.load(diaries_file.open()))

    @staticmethod
    def get_diaries(diaries_file):
        for diary in json.load(diaries_file.open()):
            diary["file_name"] = "_".join(diary['id'].split("_")[:-1])
            yield diary

    def import_diaries(self, diaries_file):
        import_diaries_query = """
        UNWIND $batch as item
        MERGE (f:File {name: item.file_name})
        MERGE (p:Page {id: item.id})
        SET p.page_idx = item.page_idx, p.text = item.text

        WITH f, p

        MERGE (f)-[:CONTAINS_PAGE]->(p)
        """
        size = self.count_diaries(diaries_file)
        self.batch_store(import_diaries_query, self.get_diaries(diaries_file), size=size)
    
if __name__ == '__main__':
    importing = RacDiariesImporter(argv=sys.argv[1:])
    base_path = importing.source_dataset_path

    if not base_path:
        print("source path directory is mandatory. Setting it to default.")
        base_path = "../dataset/rac/"

    base_path = Path(base_path)

    if not base_path.is_dir():
        print(base_path, "isn't a directory")
        sys.exit(1)

    rac_dat = base_path / "ww_1939.json"

    if not rac_dat.is_file():
        print(rac_dat, "doesn't exist in ", base_path)
        sys.exit(1)

    importing.set_constraints()
    importing.import_diaries(rac_dat)
