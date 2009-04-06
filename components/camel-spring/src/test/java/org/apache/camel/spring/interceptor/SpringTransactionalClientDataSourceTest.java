/**
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
package org.apache.camel.spring.interceptor;

import javax.sql.DataSource;

import org.apache.camel.RuntimeCamelException;
import org.apache.camel.spring.SpringTestSupport;
import org.springframework.context.support.AbstractXmlApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Unit test to demonstrate the transactional client pattern.
 */
public class SpringTransactionalClientDataSourceTest extends SpringTestSupport {

    protected JdbcTemplate jdbc;

    protected AbstractXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext(
            "/org/apache/camel/spring/interceptor/springTransactionalClientDataSource.xml");
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // create database and insert dummy data
        final DataSource ds = getMandatoryBean(DataSource.class, "dataSource");
        jdbc = new JdbcTemplate(ds);
        jdbc.execute("create table books (title varchar(50))");
        jdbc.update("insert into books (title) values (?)", new Object[] {"Camel in Action"});
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        jdbc.execute("drop table books");
    }

    public void testTransactionSuccess() throws Exception {
        template.sendBody("direct:okay", "Hello World");

        int count = jdbc.queryForInt("select count(*) from books");
        assertEquals("Number of books", 3, count);
    }

    public void testTransactionRollback() throws Exception {
        try {
            template.sendBody("direct:fail", "Hello World");
        } catch (RuntimeCamelException e) {
            // expeced as we fail
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertEquals("We don't have Donkeys, only Camels", e.getCause().getMessage());
        }

        int count = jdbc.queryForInt("select count(*) from books");
        assertEquals("Number of books", 1, count);
    }

}