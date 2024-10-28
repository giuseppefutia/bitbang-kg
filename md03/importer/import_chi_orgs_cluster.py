import sys
import logging

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class ChicagoOrgsSimilarity(BaseImporter):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.batch_size = 500
    
    def get_record_rows(self, nodes=None):
        full_name_query = """
        MATCH (n:Organization)
        WHERE NOT (n.name IS NULL OR n.name = " " OR n.name = "?") AND
              NOT n:RecordProcessed AND
              ($nodes IS NULL OR elementId(n) in $nodes)
        RETURN n.id as id
        """
        with self._driver.session(database=self.database) as session:
            result = session.run(full_name_query, {"nodes": nodes})
            for record in iter(result):
                 yield dict(record)
    
    def count_record_rows(self, nodes=None):
        full_name_query = """
        MATCH (n:Organization)
        WHERE NOT (n.name IS NULL OR n.name = " " OR n.name = "?") AND
              NOT n:RecordProcessed AND
              ($nodes IS NULL OR elementId(n) in $nodes)
        RETURN COUNT (*) as rows
        """
        with self._driver.session(database=self.database) as session:
            return session.run(full_name_query, {"nodes": nodes}).single()["rows"]
    
    def get_cluster_rows(self):
        cluster_query = """MATCH (n:OrganizationGroup) 
                           RETURN DISTINCT n.clusterId as id"""
        with self._driver.session(database=self.database) as session:
            result = session.run(cluster_query)
            for record in iter(result):
                 yield dict(record)
    
    def count_cluster_rows(self):
        count_cluster_query = """
        MATCH (n:OrganizationGroup)
        RETURN COUNT(DISTINCT n.clusterId) as rows
        """
        with self._driver.session(database=self.database) as session:
            return session.run(query=count_cluster_query).single()["rows"]

    def create_org_similarity_by_address(self, nodes=None):
        # TODO: Check the "and" issue. 
        # Needed to add this here: [x IN name_words WHERE size(trim(x)) > 2 AND trim(x) IS NOT NULL AND toLower(x) <> 'and']
        # But it should be filtered here: trim(apoc.text.regreplace(o.name, '(?i)\\b(?:co|ltd|inc|corp|llc|llp|pvt|gmbh|s.a.|s.l.|and)\\b', ''))
        create_org_similarity_by_address_query = """
        UNWIND $batch as item
        MATCH (o:Organization {id: item.id})
        SET o:RecordProcessed
        WITH o, 
            // Preprocess the organization name by removing common suffixes like 'Co', 'Ltd', 'Inc', 'Corp' and Neo4j lucene keywords like 'and'
            trim(apoc.text.regreplace(o.name, '(?i)\\b(?:co|ltd|inc|corp|llc|llp|pvt|gmbh|s.a.|s.l.|and|not)\\b', '')) as clean_name
        WITH o, clean_name, 
            // Remove non-alphanumeric characters and split words
            apoc.text.split(apoc.text.regreplace(clean_name, '[^a-zA-Z0-9\\s]', ''), "\\s+") as name_words
        WITH o, clean_name, 
            // Filter: only include words that are at least 3 characters long and not null/empty
            [x IN name_words WHERE size(trim(x)) > 2 AND trim(x) IS NOT NULL AND NOT toLower(x) IN ['and', 'not']] as valid_name_words
        WHERE size(valid_name_words) > 0
        CALL db.index.fulltext.queryNodes(
            "organization_name",
            apoc.text.join([x IN valid_name_words | trim(x) + "~0.3"], " AND ")
        )
        YIELD node, score
        WHERE node <> o AND NOT EXISTS ((node)-[:IS_SIMILAR_TO]-(o))
        WITH o, clean_name, node, 
            // Clean the full name of the other node by removing common terms, Lucene keywords, and 'AND'
            trim(apoc.text.regreplace(
                node.name, 
                '(?i)\\b(?:co|ltd|inc|corp|llc|llp|pvt|gmbh|s.a.|s.l.|and|not)\\b', ''
            )) as clean_node_name
        WITH o, clean_name, node, clean_node_name, apoc.text.sorensenDiceSimilarity(clean_name, clean_node_name) as simil
        WHERE simil > 0.3
        WITH o, node, simil
        MATCH (o)-[:HAS_ADDRESS]->(a:Address)<-[:HAS_ADDRESS]-(node)
        MERGE (node)-[r:IS_SIMILAR_TO {method: "SIMILAR_NAME+SAME_ADDRESS"}]->(o)
        ON CREATE SET r.score = simil
        """
        size = self.count_record_rows()
        self.batch_store(create_org_similarity_by_address_query, self.get_record_rows(nodes), size=size)
    
    def project_graph(self, node_label='Organization'):
        project_query = """
        CALL gds.graph.project(
            'organizationResolved',
            [$node_label],
            ['IS_SIMILAR_TO']
        ) 
        YIELD graphName, nodeCount, relationshipCount
        RETURN graphName, nodeCount, relationshipCount
        """
        with self._driver.session(database=self.database) as session:
            session.run(project_query, {"node_label": node_label})
    
    def run_WCC(self):
        wcc_query = """
        CALL gds.wcc.write('organizationResolved', { writeProperty: 'componentId' })
        YIELD nodePropertiesWritten, componentCount;
        """
        with self._driver.session(database=self.database) as session:
            session.run(query=wcc_query)
    
    def delete_projection(self):
        delete_query = """CALL gds.graph.drop('organizationResolved')"""
        with self._driver.session(database=self.database) as session:
            session.run(query=delete_query)
    
    def create_record_clusters(self, node_label = 'Organization'):
        # TODO: To be improved
        organization_query = """
        CALL apoc.periodic.iterate("MATCH (n:Organization) RETURN DISTINCT n.componentId as id",
                                   "MERGE (n:OrganizationGroup {{clusterId: id}})", {{batchSize:10000}})
        YIELD batches, total return batches, total
        """.format(node_label)
        with self._driver.session(database=self.database) as session:
            session.run(organization_query, {"node_label": node_label})
    
    def create_connections_to_clusters(self):
        connections_to_clusters_query = """
        UNWIND $batch as item
        MATCH (p:Organization {componentId: item.id})
        MATCH (c:OrganizationGroup {clusterId: item.id})
        MERGE (p)-[:BELONGS_TO_ORG_GROUP]->(c)
        SET c.ids = apoc.coll.toSet(coalesce(c.ids, []) + coalesce(toString(p.id), []))
        SET c.names = apoc.coll.toSet(coalesce(c.names, []) + coalesce(p.name, []))
        SET c.sources = apoc.coll.toSet(coalesce(c.sources, []) + coalesce(p.source, []))
        """
        size = self.count_cluster_rows()
        self.batch_store(connections_to_clusters_query, self.get_cluster_rows(), size=size)
    
    def create_final_names_of_clusters(self):
        final_name_to_clusters_query = """
        UNWIND $batch as item
        MATCH (c:OrganizationGroup {clusterId: item.id})
        WITH c, reduce(shortest = head(c.names), name IN c.names | CASE WHEN size(name) < size(shortest) THEN name ELSE shortest END) AS shortestName
        SET c.name = shortestName
        """
        size = self.count_cluster_rows()
        self.batch_store(final_name_to_clusters_query, self.get_cluster_rows(), size=size)


if __name__ == '__main__':
    importing = ChicagoOrgsSimilarity(argv=sys.argv[1:])
    logging.info("Creating similarity IS_SIMILAR_TO relationships...")
    importing.create_org_similarity_by_address()
    logging.info("Creating graph projection...")
    importing.project_graph()
    logging.info("Running WCC algorithm...")
    importing.run_WCC()
    logging.info("Deleting projection...")
    importing.delete_projection()
    logging.info("Creating organization clusters...")
    importing.create_record_clusters()
    logging.info("Creating connections to clusters...")
    importing.create_connections_to_clusters()
    logging.info("Creating cluster names...")
    importing.create_final_names_of_clusters()
    importing.close()
