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

package eu.socialsensor.insert;

import eu.socialsensor.main.GraphDatabaseType;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.user.User;
import java.util.HashMap;
import java.util.Map;

public class GafferMassiveInsertion extends InsertionBase<String> {

    protected final Map<String, Entity> cache = new HashMap<>();
    private final Graph graph;

    public GafferMassiveInsertion(final Graph graph) {
        super(GraphDatabaseType.GAFFER_ACCUMULO, null);
        this.graph = graph;
    }

    @Override
    protected String getOrCreate(final String key) {
        Entity entity = cache.get(key);
        if (entity == null) {
            entity = new Entity.Builder().vertex(key)
                                         .group("entity")
                                         .build();

            try {
                graph.execute(new AddElements.Builder().elements(entity)
                                                       .build(), new User());
            } catch (final OperationException oe) {
                oe.printStackTrace();
            }

            cache.put(key, entity);
        }
        return (String) entity.getVertex();
    }

    @Override
    protected void relateNodes(final String src, final String dest) {
        final Edge edge = new Edge.Builder().source(src)
                                            .dest(dest)
                                            .group("edge")
                                            .build();

        try {
            graph.execute(new AddElements.Builder().elements(edge)
                                                   .build(), new User());
        } catch (final OperationException oe) {
            oe.printStackTrace();
        }
    }
}
