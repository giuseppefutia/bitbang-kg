import sys
import logging
import time

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class ChicagoPeopleSimilarity(BaseImporter):

    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
        self.batch_size = 500
    
    def get_record_rows(self, nodes=None):
        full_name_query = """
        MATCH (n:PersonRecord)
        WHERE NOT (n.fullName IS NULL OR n.fullName = " " OR n.fullName = "?") AND
              NOT n:RecordProcessed AND
              ($nodes IS NULL OR elementId(n) in $nodes)
        RETURN n.pk as pk
        """
        with self._driver.session(database=self.database) as session:
            result = session.run(full_name_query, {"nodes": nodes})
            for record in iter(result):
                yield dict(record)
    
    def count_record_rows(self, nodes=None):
        full_name_query = """
        MATCH (n:PersonRecord)
        WHERE NOT (n.fullName IS NULL OR n.fullName = " " OR n.fullName = "?") AND
              NOT n:RecordProcessed AND
              ($nodes IS NULL OR elementId(n) in $nodes)
        RETURN COUNT (*) as rows
        """
        with self._driver.session(database=self.database) as session:
            return session.run(full_name_query, {"nodes": nodes}).single()["rows"]
    
    def get_cluster_rows(self):
        cluster_query = """MATCH (n:Person) 
                           RETURN DISTINCT n.clusterId as id"""
        with self._driver.session(database=self.database) as session:
            result = session.run(cluster_query,)
            for record in iter(result):
                yield dict(record)
    
    def count_cluster_rows(self):
        count_cluster_query = """
        MATCH (n:Person)
        RETURN COUNT(DISTINCT n.clusterId) as rows
        """
        with self._driver.session(database=self.database) as session:
            return session.run(query=count_cluster_query).single()["rows"]

    def create_people_similarity(self, nodes=None):
        create_people_similarity_query = """
        UNWIND $batch as item
        MATCH (p:PersonRecord {pk: item.pk})
        SET p:RecordProcessed
        WITH p, p.fullName as name, apoc.text.split(apoc.text.regreplace(p.fullName ,'[^a-zA-Z0-9\s]', ''), "\\s+") as name_words
        WHERE size(name_words) > 0
        CALL db.index.fulltext.queryNodes(
            "person_record_fullName",
             apoc.text.join([x IN name_words | trim(x) + "~0.65"], " AND ") ) 
        YIELD node, score
        WITH p, name, node
        WHERE p <> node
        WITH p, node, apoc.text.sorensenDiceSimilarity(name, node.fullName) as simil
        WHERE simil > 0.695
        WITH p, node, simil
        MERGE (node)-[r:IS_SIMILAR_TO {method: "SIMILAR_NAME"}]->(p)
        ON CREATE SET r.score = simil
        """
        size = self.count_record_rows()
        self.batch_store(create_people_similarity_query, self.get_record_rows(nodes), size=size)
    
    def project_wcc_graph(self, node_label='PersonRecord'):
        project_query = """
        CALL gds.graph.project(
            'personWcc',
            [$node_label],
            ['IS_SIMILAR_TO']
        ) 
        YIELD graphName, nodeCount, relationshipCount
        RETURN graphName, nodeCount, relationshipCount
        """
        with self._driver.session(database=self.database) as session:
            session.run(project_query, {"node_label": node_label})
    
    def run_wcc(self):
        wcc_query = """
        CALL gds.wcc.write('personWcc', { writeProperty: 'componentId' })
        YIELD nodePropertiesWritten, componentCount;
        """
        with self._driver.session(database=self.database) as session:
            session.run(query=wcc_query)
    
    def delete_wcc_projection(self):
        delete_query = """CALL gds.graph.drop('personWcc')"""
        with self._driver.session(database=self.database) as session:
            session.run(query=delete_query)

    def create_record_clusters(self, node_label = 'PersonRecord'):
        # TODO: To be improved
        person_query = """
        CALL apoc.periodic.iterate("MATCH (n:PersonRecord) RETURN DISTINCT n.componentId as id",
                                   "MERGE (n:Person {{clusterId: id}})", {{batchSize:10000}})
        YIELD batches, total return batches, total
        """.format(node_label)
        with self._driver.session(database=self.database) as session:
            session.run(person_query, {"node_label": node_label})
    
    def create_connections_to_clusters(self):
        connections_to_clusters_query = """
        UNWIND $batch as item
        MATCH (p:PersonRecord {componentId: item.id})
        MATCH (c:Person {clusterId: item.id})
        MERGE (p)-[:RECORD_RESOLVED_TO]->(c)
        SET c.fullNames = coalesce(c.fullNames, []) + p.fullName
        SET c.employerIds = coalesce(c.employerIds, []) + p.employerId
        SET c.titles = coalesce(c.titles, []) + p.title
        """
        size = self.count_cluster_rows()
        self.batch_store(connections_to_clusters_query, self.get_cluster_rows(), size=size)
    
    def create_final_names_of_clusters(self):
        final_name_to_clusters_query = """
        UNWIND $batch as item
        MATCH (c:Person {clusterId: item.id})
        WITH c, reduce(shortest = head(c.fullNames), name IN c.fullNames | CASE WHEN size(name) < size(shortest) THEN name ELSE shortest END) AS shortestName
        SET c.name = shortestName
        """
        size = self.count_cluster_rows()
        self.batch_store(final_name_to_clusters_query, self.get_cluster_rows(), size=size)
    
    def project_louvain_graph(self):
        louvain_project_query = """
        MATCH (source:PersonRecord)-[r:IS_SIMILAR_TO]->(target)
        WITH source, target, collect(r.score) as scores
        WITH source, target, apoc.coll.sum(scores) as total_score
        WITH gds.graph.project(
            'personLouvain',
            source,
            target,
            {relationshipProperties: {total_score: total_score}}
        ) as g
        RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
        """
        with self._driver.session(database=self.database) as session:
            session.run(query=louvain_project_query)
    
    def run_louvain(self):
        louvain_query = """
        CALL gds.louvain.write('personLouvain', {
                relationshipWeightProperty: 'total_score',
                writeProperty: 'louvainIntermediateCommunities',
                includeIntermediateCommunities: true
            }
        )
        YIELD communityCount, modularity, modularities
        """
        with self._driver.session(database=self.database) as session:
            session.run(query=louvain_query)
    
    def set_louvain_cluster(self):
        louvain_query = """
        MATCH (p:PersonRecord)
        SET p.louvain = toIntegerList(p.louvainIntermediateCommunities)[0]
        """
        with self._driver.session(database=self.database) as session:
            session.run(query=louvain_query)
    
    def delete_louvain_projection(self):
        delete_query = """CALL gds.graph.drop('personLouvain')"""
        with self._driver.session(database=self.database) as session:
            session.run(query=delete_query)

if __name__ == '__main__':
    importing = ChicagoPeopleSimilarity(argv=sys.argv[1:])
    logging.info("Creating similarity IS_SIMILAR_TO relationships...")
    importing.create_people_similarity()
    logging.info("Creating WCC graph projection...")
    importing.project_wcc_graph()
    time.sleep(5)
    logging.info("Running WCC algorithm...")
    importing.run_wcc()
    time.sleep(5)
    logging.info("Deleting WCC projection...")
    importing.delete_wcc_projection()
    time.sleep(5)
    logging.info("Creating person clusters...")
    importing.create_record_clusters()
    logging.info("Creating connections to clusters...")
    importing.create_connections_to_clusters()
    logging.info("Creating cluster names...")
    importing.create_final_names_of_clusters()
    logging.info("Creating Louvain projection....")
    importing.project_louvain_graph()
    time.sleep(5)
    logging.info("Running Louvain algorithm...")
    importing.run_louvain()
    time.sleep(5)
    logging.info("Set Louvain cluster...")
    importing.set_louvain_cluster()
    logging.info("Deleting Louvain projection...")
    importing.delete_louvain_projection()
    time.sleep(5)
    importing.close()
