/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package eu.socialsensor.graphdatabases;

import eu.socialsensor.insert.GafferMassiveInsertion;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEdges;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllEntities;
import uk.gov.gchq.gaffer.operation.impl.get.GetRelatedEdges;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static eu.socialsensor.main.GraphDatabaseType.GAFFER_ACCUMULO;

public class GafferGraphDatabase extends GraphDatabaseBase<CloseableIterable<Entity>, CloseableIterable<Edge>, Entity, Edge> {

    private Graph graph = null;

    public GafferGraphDatabase(final BenchmarkConfiguration config, final File dbStorageDirectory) {
        super(GAFFER_ACCUMULO, dbStorageDirectory);
    }

    @Override
    public Entity getOtherVertexFromEdge(final Edge edge, final Entity entity) {
        if (edge.getDestination().equals(entity)) {
            return (Entity) edge.getSource();
        } else {
            return (Entity) edge.getDestination();
        }
    }

    @Override
    public Entity getSrcVertexFromEdge(final Edge edge) {
        return (Entity) edge.getSource();
    }

    @Override
    public Entity getDestVertexFromEdge(final Edge edge) {
        return (Entity) edge.getDestination();
    }

    @Override
    public Entity getVertex(final Integer i) {
        return null;
    }

    @Override
    public CloseableIterable<Edge> getAllEdges() {
        try {
            return graph.execute(new GetAllEdges(), new User());
        } catch (OperationException e) {
            e.printStackTrace();
        }

        return (CloseableIterable<Edge>) org.apache.commons.collections.iterators.EmptyIterator.INSTANCE;
    }

    @Override
    public CloseableIterable<Edge> getNeighborsOfVertex(final Entity v) {

        try {
            return graph.execute(new GetRelatedEdges.Builder<>().addSeed(new EntitySeed(v))
                                                                .build(), new User());
        } catch (final OperationException e) {
            e.printStackTrace();
        }

        return (CloseableIterable<Edge>) org.apache.commons.collections.iterators.EmptyIterator.INSTANCE;
    }

    @Override
    public boolean edgeIteratorHasNext(final CloseableIterable<Edge> it) {
        return it.iterator().hasNext();
    }

    @Override
    public Edge nextEdge(final CloseableIterable<Edge> it) {
        return it.iterator().next();
    }

    @Override
    public void cleanupEdgeIterator(final CloseableIterable<Edge> it) {
        // empty
    }

    @Override
    public CloseableIterable<Entity> getVertexIterator() {
        try {
            return graph.execute(new GetAllEntities(), new User());
        } catch (OperationException e) {
            e.printStackTrace();
        }

        return (CloseableIterable<Entity>) org.apache.commons.collections.iterators.EmptyIterator.INSTANCE;
    }

    @Override
    public boolean vertexIteratorHasNext(final CloseableIterable<Entity> it) {
        return it.iterator().hasNext();
    }

    @Override
    public Entity nextVertex(final CloseableIterable<Entity> it) {
        return it.iterator().next();
    }

    @Override
    public void cleanupVertexIterator(final CloseableIterable<Entity> it) {
        // empty
    }

    @Override
    public void open() {
        graph = new Graph.Builder()
                .addSchema(new Schema.Builder()
                        .type("string", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .entity("entity", new SchemaEntityDefinition.Builder()
                                .vertex("string")
                                .build())
                        .build())
                .build();
    }

    @Override
    public void createGraphForSingleLoad() {
        graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(GafferGraphDatabase.class, "configuration/accumulostore.properties"))
                .addSchema(new Schema.Builder()
                        .type("string", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                        .entity("entity", new SchemaEntityDefinition.Builder()
                                .vertex("string")
                                .build())
                        .edge("edge", new SchemaEdgeDefinition.Builder()
                                .destination("string")
                                .source("string")
                                .build())
                        .build())
                .build();
    }

    @Override
    public void massiveModeLoading(final File dataPath) {
        Insertion gafferMassiveInsertion = new GafferMassiveInsertion(graph);
        gafferMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(final File dataPath, final File resultsPath, final int scenarioNumber) {

    }

    @Override
    public void createGraphForMassiveLoad() {
        graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(GafferGraphDatabase.class, "configuration/accumulostore.properties"))
                .addSchema(new Schema.Builder()
                        .edge("edge", new SchemaEdgeDefinition.Builder()
                                .source("string")
                                .destination("string")
                                .directed(Boolean.class)
                                .build())
                        .entity("entity", new SchemaEntityDefinition.Builder()
                                .vertex("string")
                                .build())
                        .type("string", new TypeDefinition.Builder()
                                .clazz(String.class)
                                .serialiser(new StringSerialiser())
                                .build())
                        .build())
                .build();
    }

    @Override
    public void shutdown() {
        // empty
    }

    @Override
    public void delete() {
        // empty
    }

    @Override
    public void shutdownMassiveGraph() {
        // empty
    }

    @Override
    public void shortestPath(final Entity fromNode, final Integer node) {

    }

    @Override
    public int getNodeCount() {
        return 0;
    }

    @Override
    public Set<Integer> getNeighborsIds(final int nodeId) {
        return null;
    }

    @Override
    public double getNodeWeight(final int nodeId) {
        return 0;
    }

    @Override
    public void initCommunityProperty() {

    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(final int nodeCommunities) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(final int community) {
        return null;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(final int nodeCommunity) {
        return null;
    }

    @Override
    public double getEdgesInsideCommunity(final int nodeCommunity, final int communityNodes) {
        return 0;
    }

    @Override
    public double getCommunityWeight(final int community) {
        return 0;
    }

    @Override
    public double getNodeCommunityWeight(final int nodeCommunity) {
        return 0;
    }

    @Override
    public void moveNode(final int from, final int to) {

    }

    @Override
    public double getGraphWeightSum() {
        return 0;
    }

    @Override
    public int reInitializeCommunities() {
        return 0;
    }

    @Override
    public int getCommunityFromNode(final int nodeId) {
        return 0;
    }

    @Override
    public int getCommunity(final int nodeCommunity) {
        return 0;
    }

    @Override
    public int getCommunitySize(final int community) {
        return 0;
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(final int numberOfCommunities) {
        return null;
    }

    @Override
    public boolean nodeExists(final int nodeId) {
        return false;
    }

}
