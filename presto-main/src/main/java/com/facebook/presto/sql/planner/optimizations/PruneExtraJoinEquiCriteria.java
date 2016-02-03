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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;

public class PruneExtraJoinEquiCriteria
        extends PlanOptimizer
{
    private static final Comparator<EquiJoinClause> EQUI_JOIN_CLAUSE_COMPARATOR = Comparator.comparing(EquiJoinClause::getLeft).thenComparing(EquiJoinClause::getRight);

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Optimizer(), plan, null);
    }

    private class Optimizer
            extends SimplePlanRewriter<Object>
    {
        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Object> context)
        {
            List<JoinNode.EquiJoinClause> criteria = node.getCriteria().stream()
                    .distinct()
                    .sorted(EQUI_JOIN_CLAUSE_COMPARATOR)
                    .collect(toImmutableList());

            return new JoinNode(
                    node.getId(),
                    node.getType(),
                    node.getLeft(),
                    node.getRight(),
                    criteria,
                    node.getLeftHashSymbol(),
                    node.getRightHashSymbol());
        }
    }
}
