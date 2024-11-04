import sys
import logging

from util.base_importer import BaseImporter

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

class RacGds(BaseImporter):
    def __init__(self, argv):
        super().__init__(command=__file__, argv=argv)
    
    def run_gds(self):
        QUERY_GRAPH_PROJECTION = """
        MATCH (e1:Person)-[r:TALKED_ABOUT|TALKED_WITH|WORKS_WITH]->(e2:Person)
        WHERE e1.name <> "WW" AND e2.name <> "WW" AND e1.name <> "Warren Weaver" AND e2.name <> "Warren Weaver" // WW is Warren Weaver, the author of these diaries
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

        QUERY_DROP_PROJECTION = "CALL gds.graph.drop('graph') YIELD graphName"

        QUERY_WCC = """
        CALL gds.wcc.write('graph', { writeProperty: 'wcc' })
        YIELD nodePropertiesWritten, componentCount
        """

        # run WCC to identify the largest connected component
        print("Running WCC to identify isolated sub-graphs")
        with self._driver.session(database=self.database) as session:
            session.run(QUERY_GRAPH_PROJECTION)
            session.run(QUERY_WCC)
            session.run(QUERY_DROP_PROJECTION)

        QUERY_GRAPH_PROJECTION_SELECTED = """
        MATCH (p:Person)
        WHERE p.wcc is not Null
        WITH p.wcc AS component, count(*) AS num
        ORDER BY num DESC
        LIMIT 1
    
        WITH component AS largest_wcc
    
        MATCH (e1:Person)-[r:TALKED_ABOUT|TALKED_WITH|WORKS_WITH]->(e2:Person)
        WHERE e1.name <> "WW" AND e2.name <> "WW" AND e1.name <> "Warren Weaver" AND e2.name <> "Warren Weaver" AND e1.wcc = largest_wcc AND e2.wcc = largest_wcc
        WITH gds.graph.project('graph', e1, e2,
            {
                sourceNodeLabels: labels(e1),
                targetNodeLabels: labels(e2),
                relationshipType: type(r)
                //relationshipProperties: r { .count }
            },
            {undirectedRelationshipTypes: ['*']}
        ) AS g
        RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
        """

        QUERY_PAGERANK = """
        CALL gds.pageRank.write('graph', {
          maxIterations: 100,
          dampingFactor: 0.85,
          writeProperty: $property
        })
        YIELD nodePropertiesWritten, ranIterations
        """

        QUERY_EIGENVECTOR = """
        CALL gds.eigenvector.write('graph', {
          maxIterations: 100,
          writeProperty: $property
        })
        YIELD nodePropertiesWritten, ranIterations
        """

        QUERY_BETWEENNESS = """
        CALL gds.betweenness.write('graph', { 
          writeProperty: 'betweenness'
        })
        YIELD centralityDistribution, nodePropertiesWritten
        RETURN centralityDistribution.min AS minimumScore, centralityDistribution.mean AS meanScore, nodePropertiesWritten
        """

        QUERY_LOUVAIN = """
        CALL gds.louvain.write('graph', { writeProperty: 'community' }) 
        YIELD communityCount, modularity, modularities
        """

        print("Running centrality algorithms ...")
        with self._driver.session(database=self.database) as session:
            session.run(QUERY_GRAPH_PROJECTION_SELECTED)
            print("\tPageRank")
            session.run(QUERY_PAGERANK, property="pagerank")
            print("\teigenvector centrality")
            session.run(QUERY_EIGENVECTOR, property="eigenvector")
            print("\tbetweenness centrality")
            session.run(QUERY_BETWEENNESS)
            print("\tLouvain community detection")
            session.run(QUERY_LOUVAIN)
            session.run(QUERY_DROP_PROJECTION)

        QUERY_PROJECTION_INFLUENCERS = """
        MATCH (p:Person)
        WHERE p.wcc is not Null
        WITH p.wcc AS component, count(*) AS num
        ORDER BY num DESC
        LIMIT 1
    
        WITH component AS largest_wcc
    
        MATCH (e1:Person)-[r:TALKED_ABOUT|TALKED_WITH|WORKS_WITH]->(e2:Person)
        WHERE e1.name <> "WW" AND e2.name <> "WW" AND e1.name <> "Warren Weaver" AND e2.name <> "Warren Weaver" AND e1.wcc = largest_wcc AND e2.wcc = largest_wcc
        WITH gds.graph.project('graph', e1, e2,
            {
                sourceNodeLabels: labels(e2), // we want to inverse the TALKED_ABOUT relations for influencer analysis
                targetNodeLabels: labels(e1),
                relationshipType: type(r)
            },
            {undirectedRelationshipTypes: ['TALKED_WITH', 'WORKS_WITH']}
        ) AS g
        RETURN g.graphName AS graph, g.nodeCount AS nodes, g.relationshipCount AS rels
        """

        print("Running influencers analysis ...")
        with self._driver.session(database=self.database) as session:
            session.run(QUERY_PROJECTION_INFLUENCERS)
            print("\tPageRank")
            session.run(QUERY_PAGERANK, property="pr_influencers")
            print("\teigenvector centrality")
            session.run(QUERY_EIGENVECTOR, property="eigen_influencers")
            session.run(QUERY_DROP_PROJECTION)

if __name__ == '__main__':
    importing = RacGds(argv=sys.argv[1:])
    importing.run_gds()
