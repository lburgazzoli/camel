/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.qdrant;

import java.net.http.HttpResponse;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import io.qdrant.client.VectorsFactory;
import io.qdrant.client.grpc.Collections;
import io.qdrant.client.grpc.Points;
import org.apache.camel.Exchange;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import static io.qdrant.client.ConditionFactory.matchKeyword;
import static io.qdrant.client.PointIdFactory.id;
import static io.qdrant.client.ValueFactory.value;
import static org.assertj.core.api.Assertions.assertThat;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QdrantComponentTest extends QdrantTestSupport {
    @Override
    protected void doPreSetup() throws Exception {
        HttpResponse<byte[]> resp = QDRANT.put("/collections/upsert", Map.of(
                "vectors", Map.of(
                        "size", 2,
                        "distance", Collections.Distance.Cosine)));

        assertThat(resp.statusCode()).isEqualTo(200);
    }

    @Test
    @Order(1)
    public void upsert() {
        Exchange result = fluentTemplate.to("qdrant:upsert")
                .withHeader(Qdrant.Headers.ACTION, QdrantAction.UPSERT)
                .withBody(
                        Points.PointStruct.newBuilder()
                                .setId(id(8))
                                .setVectors(VectorsFactory.vectors(List.of(3.5f, 4.5f)))
                                .putAllPayload(Map.of(
                                        "foo", value("hello"),
                                        "bar", value(1)))
                                .build())
                .request(Exchange.class);

        assertThat(result).isNotNull();
        assertThat(result.getException()).isNull();

        assertThat(result.getIn().getHeaders())
                .hasEntrySatisfying(Qdrant.Headers.OPERATION_ID, v -> {
                    assertThat(v).isNotNull();
                });
        assertThat(result.getIn().getHeaders()).hasEntrySatisfying(Qdrant.Headers.OPERATION_STATUS, v -> {
            assertThat(v).isEqualTo(Points.UpdateStatus.Completed.name());
        });
        assertThat(result.getIn().getHeaders()).hasEntrySatisfying(Qdrant.Headers.OPERATION_STATUS_VALUE, v -> {
            assertThat(v).isEqualTo(Points.UpdateStatus.Completed.getNumber());
        });
    }

    @Test
    @Order(2)
    @SuppressWarnings({ "unchecked" })
    public void retrieve() {
        Exchange result = fluentTemplate.to("qdrant:upsert")
                .withHeader(Qdrant.Headers.ACTION, QdrantAction.RETRIEVE)
                .withBody(id(8))
                .request(Exchange.class);

        assertThat(result).isNotNull();
        assertThat(result.getException()).isNull();

        assertThat(result.getIn().getBody()).isInstanceOfSatisfying(Collection.class, c -> {
            assertThat(c).hasSize(1);
            assertThat(c).hasOnlyElementsOfType(Points.RetrievedPoint.class);
        });
    }

    @Test
    @Order(3)
    public void delete() {
        Exchange result = fluentTemplate.to("qdrant:upsert")
                .withHeader(Qdrant.Headers.ACTION, QdrantAction.DELETE)
                .withBody(
                        Points.Filter.newBuilder()
                                .addMust(matchKeyword("foo", "hello"))
                                .build())
                .request(Exchange.class);

        assertThat(result).isNotNull();
        assertThat(result.getException()).isNull();

        assertThat(result.getIn().getHeaders())
                .hasEntrySatisfying(Qdrant.Headers.OPERATION_ID, v -> {
                    assertThat(v).isNotNull();
                });
        assertThat(result.getIn().getHeaders()).hasEntrySatisfying(Qdrant.Headers.OPERATION_STATUS, v -> {
            assertThat(v).isEqualTo(Points.UpdateStatus.Completed.name());
        });
        assertThat(result.getIn().getHeaders()).hasEntrySatisfying(Qdrant.Headers.OPERATION_STATUS_VALUE, v -> {
            assertThat(v).isEqualTo(Points.UpdateStatus.Completed.getNumber());
        });
    }

    @Test
    @Order(4)
    public void retrieveAfterDelete() {
        Exchange result = fluentTemplate.to("qdrant:upsert")
                .withHeader(Qdrant.Headers.ACTION, QdrantAction.RETRIEVE)
                .withBody(id(8))
                .request(Exchange.class);

        assertThat(result).isNotNull();
        assertThat(result.getException()).isNull();

        assertThat(result.getIn().getBody()).isInstanceOfSatisfying(Collection.class, c -> {
            assertThat(c).hasSize(0);
        });
    }
}
