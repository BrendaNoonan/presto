/*
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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.PlanAssert.PlanMatcher;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.PlanAssert.aliasPair;
import static com.facebook.presto.sql.planner.PlanAssert.anyNode;
import static com.facebook.presto.sql.planner.PlanAssert.anyNodeTree;
import static com.facebook.presto.sql.planner.PlanAssert.filterNode;
import static com.facebook.presto.sql.planner.PlanAssert.joinNode;
import static com.facebook.presto.sql.planner.PlanAssert.node;
import static com.facebook.presto.sql.planner.PlanAssert.projectNode;
import static com.facebook.presto.sql.planner.PlanAssert.semiJoinNode;
import static com.facebook.presto.sql.planner.PlanAssert.tableScanNode;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestLogicalPlanner
{
    private final LocalQueryRunner queryRunner;

    public TestLogicalPlanner()
    {
        this.queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .build());

        queryRunner.createCatalog(queryRunner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(queryRunner.getNodeManager(), 1),
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testPlanDsl()
    {
        assertPlan("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)",
                node(OutputNode.class,
                        node(FilterNode.class,
                                tableScanNode("orders"))));

        String simpleJoinQuery = "SELECT o.orderkey FROM orders o, lineitem l WHERE l.orderkey = o.orderkey";
        assertPlan(simpleJoinQuery,
                anyNode(
                        anyNode(
                                node(JoinNode.class,
                                        anyNode(tableScanNode("orders")),
                                        anyNode(tableScanNode("lineitem"))))));

        assertPlan(simpleJoinQuery,
                anyNodeTree(
                        node(JoinNode.class,
                                anyNodeTree(),
                                anyNodeTree())));

        assertPlan(simpleJoinQuery, anyNodeTree(node(TableScanNode.class)));

        assertPlan("SELECT * FROM orders WHERE orderkey = (SELECT orderkey FROM lineitem ORDER BY orderkey LIMIT 1)",
                anyNodeTree(
                        joinNode(ImmutableList.of(aliasPair("H1", "H2"), aliasPair("X", "Y")),
                                projectNode(
                                        tableScanNode("orders").withSymbol("orderkey", "X"))
                                        .withSymbol("hash", "H1"),
                                projectNode(
                                        node(EnforceSingleRowNode.class,
                                                anyNodeTree(
                                                        tableScanNode("lineitem")
                                                                .withSymbol("orderkey", "Y"))))
                                        .withSymbol("hash", "H2"))));

        assertPlan("SELECT * FROM orders WHERE orderkey IN (SELECT orderkey FROM lineitem WHERE linenumber % 4 = 0)",
                anyNodeTree(
                        filterNode("S",
                                semiJoinNode("X", "Y", "S",
                                        anyNodeTree(
                                                tableScanNode("orders")
                                                        .withSymbol("orderkey", "X")),
                                        anyNodeTree(
                                                tableScanNode("lineitem")
                                                        .withSymbol("orderkey", "Y"))))));

        assertPlan("SELECT * FROM orders WHERE orderkey NOT IN (SELECT orderkey FROM lineitem WHERE linenumber < 0)",
                anyNodeTree(
                        filterNode("NOT S",
                                semiJoinNode("X", "Y", "S",
                                        anyNodeTree(
                                                tableScanNode("orders")
                                                        .withSymbol("orderkey", "X")),
                                        anyNodeTree(
                                                tableScanNode("lineitem")
                                                        .withSymbol("orderkey", "Y"))))));
    }

    private void assertPlan(String sql, PlanMatcher expectedPlan)
    {
        Plan actualPlan = queryRunner.inTransaction(transactionSession -> queryRunner.createPlan(transactionSession, sql));
        queryRunner.inTransaction(transactionSession -> {
            PlanAssert.assertPlan(transactionSession, queryRunner.getMetadata(), actualPlan.getRoot(), expectedPlan);
            return null;
        });
    }
}
