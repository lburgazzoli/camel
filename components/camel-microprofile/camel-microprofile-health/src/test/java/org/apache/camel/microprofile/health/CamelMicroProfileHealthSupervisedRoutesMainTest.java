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
package org.apache.camel.microprofile.health;

import java.util.Map;
import java.util.Optional;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.apache.camel.CamelContext;
import org.apache.camel.Component;
import org.apache.camel.Consumer;
import org.apache.camel.Endpoint;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.health.HealthCheckHelper;
import org.apache.camel.health.HealthCheckRegistry;
import org.apache.camel.health.HealthCheckResultBuilder;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.health.AbstractHealthCheck;
import org.apache.camel.impl.health.ComponentsHealthCheckRepository;
import org.apache.camel.main.SimpleMain;
import org.apache.camel.support.DefaultComponent;
import org.apache.camel.support.DefaultConsumer;
import org.apache.camel.support.DefaultEndpoint;
import org.apache.camel.support.DefaultProducer;
import org.eclipse.microprofile.health.HealthCheckResponse.Status;
import org.junit.jupiter.api.Test;

import io.smallrye.health.SmallRyeHealth;
import io.smallrye.health.SmallRyeHealthReporter;

import static org.apache.camel.microprofile.health.CamelMicroProfileHealthTestHelper.getHealthJson;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class CamelMicroProfileHealthSupervisedRoutesMainTest {
    private final SmallRyeHealthReporter reporter = new SmallRyeHealthReporter();

    @Test
    public void testSupervisedRouteHealthChecks() throws Exception {
        CamelContext context = new DefaultCamelContext();
        CamelMicroProfileHealthCheckRegistry registry = new CamelMicroProfileHealthCheckRegistry(context);
        context.addComponent("my", new MyComponent());
        context.setExtension(HealthCheckRegistry.class, registry);
        context.getRouteController().supervising();
        context.addRoutes(new RouteBuilder() {
            @Override
            public void configure() throws Exception {
                from("my:start").routeId("healthyRoute")
                        .setBody(constant("Hello Camel MicroProfile Health"));
            }
        });

        SimpleMain main = new SimpleMain(context);
        main.addInitialProperty("camel.health.routes-enabled", "true");
        main.addInitialProperty("camel.health.consumers-enabled", "true");
        main.addInitialProperty("camel.health.components-enabled", "true");
        main.start();

        try {
            SmallRyeHealth health = reporter.getHealth();

            JsonObject healthObject = getHealthJson(reporter, health);
            assertEquals(Status.UP.name(), healthObject.getString("status"));

            JsonArray checks = healthObject.getJsonArray("checks");
            assertEquals(4, checks.size());

            Optional<JsonObject> camelRoutesCheck = findHealthCheck("camel-routes", checks);
            camelRoutesCheck.ifPresentOrElse(check -> {
                assertEquals(Status.UP.toString(), check.getString("status"));
            }, () -> fail("Expected camel-routes check not found in health output"));

            Optional<JsonObject> camelConsumersCheck = findHealthCheck("camel-consumers", checks);
            camelConsumersCheck.ifPresentOrElse(check -> {
                assertEquals(Status.UP.toString(), check.getString("status"));
            }, () -> fail("Expected camel-consumers check not found in health output"));

            Optional<JsonObject> camelComponentsCheck = findHealthCheck("camel-components", checks);
            camelComponentsCheck.ifPresentOrElse(check -> {
                assertEquals(Status.UP.toString(), check.getString("status"));
            }, () -> fail("Expected camel-components check not found in health output"));
        } finally {
            main.stop();
        }
    }

    private Optional<JsonObject> findHealthCheck(String name, JsonArray checks) {
        return checks.stream()
                .map(JsonValue::asJsonObject)
                .filter(jsonObject -> jsonObject.getString("name").equals(name))
                .findFirst();
    }


    private static class MyComponent extends DefaultComponent {
        @Override
        protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) throws Exception {
            return new MyEndpoint(uri, this);
        }
    }

    private static class MyEndpoint extends DefaultEndpoint {
        public MyEndpoint(String endpointUri, Component component) {
            super(endpointUri, component);
        }

        @Override
        public Producer createProducer() throws Exception {
            return new DefaultProducer(this) {
                @Override
                protected void doStart() throws Exception {
                    super.doStart();

                    var repo = HealthCheckHelper.getHealthCheckRepository(
                        getCamelContext(),
                        ComponentsHealthCheckRepository.REPOSITORY_NAME,
                        ComponentsHealthCheckRepository.class);

                    if (repo != null) {
                        repo.addHealthCheck(new MyHealthCheck("my-producer-hc"));
                    }
                }

                @Override
                public void process(Exchange exchange) throws Exception {
                }
            };
        }

        @Override
        public Consumer createConsumer(Processor processor) throws Exception {
            return new DefaultConsumer(this, processor) {
                @Override
                protected void doStart() throws Exception {
                    super.doStart();

                    var repo = HealthCheckHelper.getHealthCheckRepository(
                        getCamelContext(),
                        ComponentsHealthCheckRepository.REPOSITORY_NAME,
                        ComponentsHealthCheckRepository.class);

                    if (repo != null) {
                        repo.addHealthCheck(new MyHealthCheck("my-consumer-hc"));
                    }
                }
            };
        }
    }

    private static class MyHealthCheck extends AbstractHealthCheck {
        public MyHealthCheck(String id) {
            super(id);
        }

        @Override
        protected void doCall(HealthCheckResultBuilder builder, Map<String, Object> options) {
            builder.up();
        }

        @Override
        public boolean isLiveness() {
            return false;
        }
    }
}
