import sys
import os
import time
import json
import logging
import configparser
from pathlib import Path

from openai import OpenAI

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class RacFullKG(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.cache_folder: Path = None
        self.openai_model = None
        self.openai_key = None
        self.openai_url = None

    def read_prompt(self, path: Path):
        prompt_segments = {}
        with open(os.path.join(path, "gpt_prompt_task.txt"), 'r') as f:
            prompt_segments['task'] = f.read()
        with open(os.path.join(path, "gpt_prompt_example.txt"), 'r') as f:
            prompt_segments['example'] = f.read()
        with open(os.path.join(path, "gpt_prompt_example_output.txt"), 'r') as f:
            prompt_segments['example_output'] = f.read()
        return prompt_segments

    def openai_query(self, gpt_client, prompt_segments: dict, query: str, key: str = None):
        # use the cached response if possible
        if self.cache_folder is not None and key is not None:
            if (self.cache_folder / f"{key}.json").is_file():
                return json.load((self.cache_folder / f"{key}.json").open())

        messages = [{"role": "system", "content": prompt_segments['task']},
                    {"role": "user", "content": prompt_segments['example']},
                    {"role": "assistant", "content": prompt_segments['example_output']},
                    {"role": "user", "content": query}]

        t_start = time.time()
        response = gpt_client.chat.completions.create(model=self.openai_model, messages=messages, temperature=0.,
                                                      max_tokens=2000)
        print(response.choices[0].message.content)
        print(f"\nTime: {round(time.time() - t_start, 1)} sec\n")
        ret = response.choices[0].message.content

        # cache the response
        if self.cache_folder is not None and key is not None:
            if self.cache_folder.is_dir():
                json.dump(ret, (self.cache_folder / f"{key}.json").open("w"))

        return ret

    @staticmethod
    def parse_gpt_output(output: str) -> dict:
        ents, rels = list(), list()
        failed = False
        try:
            d = json.loads(output)
            if 'entities' not in d or 'relations' not in d:
                print(f"ERROR: GPT output does not have expected format!\n{d}")
                failed = True
            else:
                for label, arr in d['entities'].items():
                    for e in arr:
                        e['label_gpt'] = label
                        e['label'] = "".join([x[0].upper() + x[1:].lower() for x in label.strip().split()])
                        ents.append(e)
                for rel_type, arr in d['relations'].items():
                    for r in arr:
                        if 'type' in r:
                            r['conversation_type'] = r['type']
                        r['type_gpt'] = rel_type
                        r['type'] = "_".join(rel_type.strip().upper().split())
                        rels.append(r)
        except Exception as e:
            print(f"ERROR: Couldn't parse GPT output!\n{e}")
            print(output)
            failed = True
        return {'entities': ents, 'relations': rels, 'failed': failed}

    def store_to_neo4j(self, doc_id: int, entities: list, relations: list, run: str):
        QUERY_WRITE_ENTS = """
        MATCH (n)
        WHERE id(n) = $id
        SET n:GPTProcessed,
        n.GPT_metadata_entities = '{"GPTMetadataEntities":' + apoc.convert.toJson($entities) + "}", 
        n.prompt_version = "1"
        WITH n
    
        UNWIND $entities AS ent
    
        CREATE (e:Entity)
        SET e += ent,
        e.all_properties = apoc.convert.toJson(ent)
    
        WITH n, e
    
        MERGE (n)-[r:MENTIONS_ENTITY]->(e)
        ON CREATE SET r.runs = [$run]
        ON MATCH SET r.runs = apoc.coll.toSet(r.runs + [$run])
    
        RETURN count(*) AS count
        """

        QUERY_WRITE_RELS = """
        MATCH (n)
        WHERE id(n) = $id
        SET n:GPTProcessed,
        n.GPT_metadata_relations = '{"GPTMetadataRelations":' + apoc.convert.toJson($relations) + "}", 
        n.prompt_version = "1"
        WITH n
    
        UNWIND $relations AS rel
    
        MATCH (n)-[:MENTIONS_ENTITY]->(e1:Entity {id: rel.source})
        MATCH (n)-[:MENTIONS_ENTITY]->(e2:Entity {id: rel.target})
    
        MERGE (e1)-[r:RELATED_TO_ENTITY {type: rel.type}]->(e2)
        ON CREATE SET r.runs = [$run]
        ON MATCH SET r.runs = apoc.coll.toSet(r.runs + [$run])
        SET r += rel, r.all_properties = apoc.convert.toJson(rel)
    
        RETURN count(*) AS count
        """

        QUERY_TITLES = """
        MATCH (n)-[:MENTIONS_ENTITY]->(e)
        WHERE id(n) = $id AND e.titles IS NOT Null
    
        UNWIND e.titles AS tit
    
        CREATE (t:Entity {name: tit})
        MERGE (n)-[:MENTIONS_ENTITY]->(t)
        MERGE (e)-[:RELATED_TO_ENTITY {type: "HAS_TITLE"}]->(t)
        """

        with self._driver.session(database=self.database) as session:
            session.run(QUERY_WRITE_ENTS, id=doc_id, entities=entities, run=run)
            session.run(QUERY_TITLES, id=doc_id, entities=entities)
            session.run(QUERY_WRITE_RELS, id=doc_id, relations=relations, run=run)

    def process_diaries_gpt(self, gpt_prompt_segments: dict, n_docs: int = 100,
                            run: str = "run 1"):
        query_read = """
        MATCH (p:Page)
        WHERE NOT p:GPTProcessed
        RETURN id(p) AS id,p.id as key, p.text AS text 
        ORDER BY p.page_idx 
        LIMIT $limit
        """

        client = OpenAI(
            base_url=self.openai_url,
            api_key=self.openai_key,
        )

        print("Reading pages")
        with self._driver.session(database=self.database) as session:
            res = session.run(query_read, limit=n_docs)
            pages = res.data()
        print(f"Read {len(pages)} pages")

        # run GPT & store to Neo4j
        failed_pages = list()
        for p in pages:
            print(f"Processing page {p['id']}")
            output = self.openai_query(client, gpt_prompt_segments, p['text'], key=p['key'])
            gpt_parsed = self.parse_gpt_output(output)
            if gpt_parsed['failed']:
                failed_pages.append(p)
            else:  # store only fully-correct outputs, rerun the pages with failures later
                print(
                    f"Storing {len(gpt_parsed['entities'])} entities and {len(gpt_parsed['relations'])} relations from page {p['id']} to Neo4j")
                self.store_to_neo4j(p['id'], gpt_parsed['entities'], gpt_parsed['relations'], run)

        print(f"\n=== Finished processing {len(pages)} pages, {len(failed_pages)} pages had output format issue")
        if len(failed_pages) == 0:
            return

        print("Rerunning pages with failures ...")

        for p in failed_pages:
            print(f"Processing page {p['id']}")
            output = self.openai_query(client, gpt_prompt_segments, p['text'], key=p['key'] + "_retry")
            gpt_parsed = self.parse_gpt_output(output)
            # even if some relations have incorrect format again, store at least the good ones this time
            print(
                f"Storing {len(gpt_parsed['entities'])} entities and {len(gpt_parsed['relations'])} relations from page {p['id']} to Neo4j")
            self.store_to_neo4j(p['id'], gpt_parsed['entities'], gpt_parsed['relations'], run)

    def cleanse_stability_test(self, keep_run: str):
        CLEANSING_QUERIES = ["""
        MATCH (p:GPTProcessed)-[r:MENTIONS_ENTITY]->(e)
        WHERE NOT $keep_run IN r.runs
        DETACH DELETE e""",
        """
        MATCH (p:GPTProcessed)-[:MENTIONS_ENTITY]->(e)-[r:RELATED_TO_ENTITY]->(e2)
        WHERE NOT $keep_run IN r.runs
        DELETE r 
        """]
        with self._driver.session(database=self.database) as session:
            for query in CLEANSING_QUERIES:
                session.run(query, keep_run=keep_run)

    def normalize_entities(self):
        # Cleanse person names: sometimes, title/degree of a person is identified as part of person name - strip them
        REMOVE_TITLES = ["dr.", "prof.", "dean", "president", "pres.", "sir", "mr.", "mrs."]
        QUERY_NORM_PERSONS = """
        MATCH (e:Entity {label: "Person"})
        WITH e, CASE WHEN ANY(title IN $remove_titles WHERE toLower(e.name) STARTS WITH title) 
            THEN apoc.text.join(split(e.name, " ")[1..], " ") 
            ELSE e.name END AS name
        SET e.name_normalized = name
        """

        # Lowercase occupations: they represent research disciplines, technologies etc. - case is irrelevant
        QUERY_NORM_OCCUPATIONS = """MATCH (e:Entity {label: "Occupation"})
            SET e.name_normalized = toLower(e.name)
            """

        with self._driver.session(database=self.database) as session:
            print("Normalising Person names")
            session.run(QUERY_NORM_PERSONS, remove_titles=REMOVE_TITLES)
            print("Normalising Occupations")
            session.run(QUERY_NORM_OCCUPATIONS)

    def resolve_entities(self):
        ### ER of Persons
        #   - update properties `name_normalized` so that in the KG creation step, the resolved entities get merged

        # resolve surnames to full names - the same page (e.g. "George E. Hale" and "Hale")
        #   - very high likelihood that it's the same person
        QUERY_RESOLVE_SAME_PAGE = """
        MATCH (e2:Entity {label: "Person"})<-[:MENTIONS_ENTITY]-(p:Page)-[:MENTIONS_ENTITY]->(e1:Entity {label: "Person"})
        WHERE e1.name_normalized ENDS WITH " " + e2.name_normalized AND size(e1.name) > 2 AND size(e2.name) > 2
        SET e2.name_normalized = e1.name_normalized // set new name to full name
        """

        # resolve surnames to full names - previous page (e.g. "Lewis" at page 17 to "Ivey F. Lewis" at page 16)
        #   - many diary entries span multiple pages and full names are usually mentioned only once at the beginning
        QUERY_RESOLVE_PREVIOUS_PAGE = """
        MATCH (f:File)-[:CONTAINS_PAGE]->(p1:Page)-[:MENTIONS_ENTITY]->(e1:Entity {label: "Person"})
        MATCH (f)-[:CONTAINS_PAGE]->(p2:Page)-[:MENTIONS_ENTITY]->(e2:Entity {label: "Person"})
        WHERE p1.page_idx = p2.page_idx - 1 AND e1.name_normalized ENDS WITH " " + e2.name_normalized AND size(e1.name) > 2 AND size(e2.name) > 2
        SET e2.name_normalized = e1.name_normalized // set new name to full name from previous page
        """

        print("Resolving Persons - same and previous page")
        with self._driver.session(database=self.database) as session:
            session.run(QUERY_RESOLVE_SAME_PAGE)
            session.run(QUERY_RESOLVE_PREVIOUS_PAGE)

        # leverage outputs of RE, examples:
        #   - "I. B. Conant" (page 79) - WORKS_FOR -> "Harvard University" <- WORKS_FOR - "Conant" (page 52)
        #   - "A. Bearden" (page 69) - WORKS_FOR -> "Johns Hopkins" <- WORKS_FOR - "J. A. Bearden" (page 68)
        #       and HAS_TITLE relation to "Associate Professor of Physics"
        #   - "D. H. Andrews" (page  67) - WORKS_ON -> "specific heats of organic compounds" <- WORKS_ON - "Andrews" (page 79)

        QUERY_SIMILAR_OCC = """
        MATCH (e1:Entity {label: "Occupation"}), (e2:Entity {label: "Occupation"})
        WHERE e1 <> e2 AND e1.name_normalized CONTAINS e2.name_normalized AND NOT e2.name_normalized IN ["research"]
        MERGE (e1)-[:META_SIMILAR]->(e2)
        """
        QUERY_SIMILAR_ORG = """
        MATCH (e1:Entity {label: "Organization"}), (e2:Entity {label: "Organization"})
        WHERE e1 <> e2 AND toLower(e1.name) CONTAINS toLower(e2.name) AND NOT toLower(e2.name) IN ["university", "foundation"]
        MERGE (e1)-[:META_SIMILAR]->(e2)
        """

        with self._driver.session(database=self.database) as session:
            session.run(QUERY_SIMILAR_OCC)
            session.run(QUERY_SIMILAR_ORG)

        # Prepare for resolution of surnames - the same surname alone is not enough to resolve, we need additional evidence
        QUERY_RESOLVE_PER_SURNAMES = """
            WITH ["WORKS_FOR", "WORKS_ON", "HAS_TITLE"] AS er_relations
            MATCH (e1:Entity {label: "Person"}), (e2:Entity {label: "Person"})
            WHERE e1.name_normalized ENDS WITH " " + e2.name_normalized AND size(e1.name) > 2 AND size(e2.name) > 2
            WITH er_relations, e1, e2
            MATCH path=(e1)-[r1:RELATED_TO_ENTITY]->()-[:META_SIMILAR]-()<-[r2:RELATED_TO_ENTITY]-(e2)
            WHERE r1.type IN er_relations AND r2.type IN er_relations
            MERGE (e1)-[:META_RESOLVED_PER]-(e2)
            ////////SET e2.name_normalized = e1.name_normalized
            """

        # Prepare for resolution of Persons - consider specifics of person names ("James T. Kirk" <=> "J. T. Kirk" <=> "James Kirk" <=> "J. Kirk")
        #   - for more stringent approach, replace the 1st MATCH with the commented MATCH and uncomment the additional conditions in WHERE clause
        QUERY_RESOLVE_PER_SIM = """MATCH (e1:Entity {label: "Person"}), (e2:Entity {label: "Person"})
        //MATCH (e1:Entity {label: "Person"})-[r1:RELATED_TO_ENTITY]->()-[:META_SIMILAR]-()<-[r2:RELATED_TO_ENTITY]-(e2:Entity {label: "Person"})
        WHERE e1.name_normalized ENDS WITH " " + split(e2.name_normalized, " ")[-1] AND size(split(e1.name_normalized, " ")) > 1 AND size(split(e2.name_normalized, " ")) > 1
              //AND r1.type IN ["WORKS_FOR", "WORKS_ON"] AND r2.type IN ["WORKS_FOR", "WORKS_ON"]
    
        WITH e1, e2, split(e1.name_normalized, " ")[0..-1] AS firsts1, split(e2.name_normalized, " ")[0..-1] AS firsts2
        WITH e1, e2, (CASE 
          WHEN size(firsts1) = size(firsts2) THEN ALL(iii IN range(0, size(firsts1) - 1) WHERE substring(firsts1[iii], 0, 1) = substring(firsts2[iii], 0, 1) AND (size(firsts1[iii]) = 1 OR size(firsts2[iii]) = 1 OR (size(firsts1[iii]) = 2 AND substring(firsts1[iii], size(firsts1[iii]) - 1, size(firsts1[iii])) = ".") OR (size(firsts2[iii]) = 2 AND substring(firsts2[iii], size(firsts2[iii]) - 1, size(firsts2[iii])) = "."))) 
          WHEN (size(firsts1) = 1 AND size(firsts2) = 2) OR (size(firsts1) = 2 AND size(firsts2) = 1) THEN substring(firsts1[0], 0, 1) = substring(firsts2[0], 0, 1) AND (size(firsts1[0]) = 1 OR size(firsts2[0]) = 1 OR (size(firsts1[0]) = 2 AND substring(firsts1[0], size(firsts1[0]) - 1, size(firsts1[0])) = ".") OR (size(firsts2[0]) = 2 AND substring(firsts2[0], size(firsts2[0]) - 1, size(firsts2[0])) = ".")) // when middle name is skipped for one person
          ELSE false END) AS are_similar
        WHERE are_similar = True
    
        MERGE (e1)-[:META_PERSONS_SIMILAR]-(e2)
        """
        QUERY_PER_SIM_GRAPH = """MATCH (e1:Entity {label: "Person"})-[r:META_PERSONS_SIMILAR]->(e2)
        WITH gds.graph.project('graph', e1, e2,
          {
            sourceNodeLabels: labels(e1),
            targetNodeLabels: labels(e2),
            relationshipType: type(r)
          },
          {undirectedRelationshipTypes: ['*']}
        ) AS g
        RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
        """
        QUERY_RESOLVE_PER = """MATCH (p:Entity)
        WHERE p.resolution_wcc IS NOT Null
        WITH p.resolution_wcc AS wcc, p.name_normalized AS name
        ORDER BY size(name) DESC
        WITH wcc, collect(name)[0] AS wcc_name
        MATCH (p:Entity)
        WHERE p.resolution_wcc = wcc
        SET p.name_normalized = wcc_name"""

        print("Resolving person names based on string similarity")
        with self._driver.session(database=self.database) as session:
            session.run(QUERY_RESOLVE_PER_SURNAMES)
            session.run(QUERY_RESOLVE_PER_SIM)
            session.run(QUERY_PER_SIM_GRAPH)
            session.run(
                "CALL gds.wcc.write('graph', { writeProperty: 'resolution_wcc' }) YIELD nodePropertiesWritten, componentCount")
            session.run("CALL gds.graph.drop('graph') YIELD graphName")
            session.run(QUERY_RESOLVE_PER)

    def create_kg(self):
        # Reset the KG
        QUERY_DELETE_KG = """MATCH (n) 
        WHERE n:Person OR n:Organization OR n:Occupation OR n:Title
        DETACH DELETE n
        """
        print("Cleansing the KG")
        with self._driver.session(database=self.database) as session:
            session.run(QUERY_DELETE_KG)

        RENAME_ENTS = {'Technology': 'Occupation'}
        RENAME_RELS = {'TALKED_TO': 'TALKED_WITH',
                       'MET': 'TALKED_WITH',
                       'TOOK_LUNCHEON_WITH': 'TALKED_WITH',
                       'MENTIONED': 'TALKED_WITH',
                       'MENTIONS': 'TALKED_WITH'
                       }
        SCHEMA = [
            ['Person', 'WORKS_FOR', 'Organization'],
            ['Person', 'WORKS_ON', 'Occupation'],
            ['Person', 'HAS_TITLE', 'Title'],
            ['Person', 'TALKED_WITH', 'Person'],
            ['Person', 'TALKED_ABOUT', 'Person'],
            ['Person', 'STUDENT_OF', 'Person'],
            ['Person', 'WORKS_WITH', 'Person']
        ]

        QUERY_CREATE_KG = """
        MATCH (p:Page)-[:MENTIONS_ENTITY]->(e1:Entity)-[r:RELATED_TO_ENTITY]->(e2)
        WITH p, e1, r, e2, 
        CASE WHEN $rename_ents[e1.label] is Null THEN e1.label ELSE $rename_ents[e1.label] END AS label1,
        CASE WHEN $rename_rels[r.type] is Null THEN r.type ELSE $rename_rels[r.type] END AS relation,
        CASE WHEN $rename_ents[e2.label] is Null THEN e2.label ELSE $rename_ents[e2.label] END AS label2
    
        UNWIND $schema AS kg_rel
    
        WITH p, e1, r, e2, label1, relation, label2, kg_rel
        WHERE label1 = kg_rel[0] AND label2 = kg_rel[2] AND relation = kg_rel[1]
    
        CALL apoc.merge.node([label1], {name_normalized: toLower(coalesce(e1.name_normalized, e1.name))}, {name: coalesce(e1.name_normalized, e1.name), count: 0}) YIELD node AS n1
        SET n1.count = n1.count + 1,
        n1.titles = CASE WHEN e1.titles IS NOT Null THEN e1.titles ELSE Null END,
        n1.type = CASE WHEN e1.type IS NOT Null THEN e1.type ELSE Null END
        WITH p, e1, r, e2, label1, relation, label2, n1
        CALL apoc.merge.relationship(e1, "MAPPED_TO_" + toUpper(label1), {}, {}, n1) YIELD rel
        WITH p, e1, r, e2, label1, relation, label2, n1
    
        CALL apoc.merge.node([label2], {name_normalized: toLower(coalesce(e2.name_normalized, e2.name))}, {name: coalesce(e2.name_normalized, e2.name), count: 0}) YIELD node AS n2
        SET n2.count = n2.count + 1,
        n2.titles = CASE WHEN e2.titles IS NOT Null THEN e2.titles ELSE Null END,
        n2.type = CASE WHEN e2.type IS NOT Null THEN e2.type ELSE Null END
        WITH p, e1, r, e2, label1, relation, label2, n1, n2
        CALL apoc.merge.relationship(e2, "MAPPED_TO_" + toUpper(label2), {}, {}, n2) YIELD rel
        WITH p, e1, r, e2, label1, relation, label2, n1, n2
    
        CALL apoc.merge.relationship(n1, relation, {}, {count: 0, orig_ids: []}, n2) YIELD rel
        SET rel.count = rel.count + 1, rel.orig_ids = rel.orig_ids + [id(r)],
        rel.sentiment = CASE WHEN r.sentiment IS NOT Null THEN r.sentiment ELSE Null END,
        rel.conversation_type = CASE WHEN r.conversation_type IS NOT Null THEN r.conversation_type ELSE Null END
    
        RETURN count(distinct rel) AS n_rels
        """

        print("Creating the KG ...")
        with self._driver.session(database=self.database) as session:
            res = session.run(QUERY_CREATE_KG, rename_ents=RENAME_ENTS, rename_rels=RENAME_RELS, schema=SCHEMA)
            print(f"Created {res.data()[0]['n_rels']} KG relations")

        # create similarities
        QUERY_SIM_ORG = """
        MATCH (e1:Organization), (e2:Organization)
        WHERE e1<>e2 AND e1.name_normalized CONTAINS e2.name_normalized AND NOT e2.name_normalized IN ["university", "foundation"]
        MERGE (e1)-[:SIMILAR_ORGANIZATION]->(e2)
        """

        QUERY_SIM_OCC = """
        MATCH (e1:Occupation), (e2:Occupation)
        WHERE e1<>e2 AND e1.name_normalized CONTAINS e2.name_normalized AND NOT e2.name_normalized IN ["research"]
        MERGE (e1)-[:SIMILAR_OCCUPATION]->(e2)
        """

        with self._driver.session(database=self.database) as session:
            print("Creating Organization similarities")
            session.run(QUERY_SIM_ORG)
            print("Creating Occupation similarities")
            session.run(QUERY_SIM_OCC)

if __name__ == '__main__':
    importing = RacFullKG(argv=sys.argv[1:])

    base_path = importing.source_dataset_path
    if not base_path:
        print("source path directory is mandatory. Setting it to default.")
        base_path = "../dataset/rac/"

    base_path = Path(base_path)

    if not base_path.is_dir():
        print(base_path, "isn't a directory")
        sys.exit(1)
    
    # OpenAI API details
    config = configparser.ConfigParser()
    config_file = os.path.join(os.path.dirname(__file__), '../../', 'config.ini')
    config.read(config_file)
    openai_params = config['openai']
    importing.openai_key = openai_params.get('apikey')
    importing.openai_url = openai_params.get('url')
    importing.openai_model = openai_params.get('model')

    # Set up a cache folder for LLM responses
    importing.cache_folder = Path("../dataset/rac/cache_llm")
    importing.cache_folder.mkdir(exist_ok=True)

    # Set prompt
    prompt_segments = importing.read_prompt(base_path)
    
    # Process diaries
    importing.process_diaries_gpt(prompt_segments, n_docs=100)

    # Normalise entities
    importing.normalize_entities()

    # Resolve entities such as person names
    importing.resolve_entities()

    # Create a final clean KG
    importing.create_kg()
