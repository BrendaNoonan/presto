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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertTrue;

public class PlanAssert
{
    public static void assertPlan(Session transactionSesssion, Metadata metadata, PlanNode actualPlan, PlanMatcher planMatcher)
    {
        requireNonNull(actualPlan, "root is null");

        assertTrue(actualPlan.accept(new PlanMatchingVisitor(transactionSesssion, metadata), new PlanMatchingContext(planMatcher)),
                "Plan does not match, because one of the following:");
    }

    public static PlanMatcher node(Class<? extends PlanNode> nodeClass, PlanMatcher... sources)
    {
        return new NodeClassPlanMatcher(nodeClass, ImmutableList.copyOf(sources));
    }

    public static PlanMatcher anyNode(PlanMatcher... sources)
    {
        return new PlanMatcher(ImmutableList.copyOf(sources));
    }

    /**
     * Matches to any tree of nodes with children matching to given source matchers.
     * anyNodeTree(tableScanNode("nation")) - will match to any plan which all leafs contain
     * any node containing table scan from nation table.
     */
    public static PlanMatcher anyNodeTree(PlanMatcher... sources)
    {
        return new AnyNodesTreePlanNodeMatcher(ImmutableList.copyOf(sources));
    }

    public static PlanMatcher tableScanNode(String expectedTableName)
    {
        return new PlanMatcher(ImmutableList.of())
        {
            @Override
            public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                if (node instanceof TableScanNode) {
                    TableScanNode tableScanNode = (TableScanNode) node;
                    TableMetadata tableMetadata = metadata.getTableMetadata(session, tableScanNode.getTable());
                    String actualTableName = tableMetadata.getTable().getTableName();
                    if (expectedTableName.equalsIgnoreCase(actualTableName)) {
                        return super.matches(node, session, metadata, symbolAliases);
                    }
                }
                return ImmutableList.of();
            }
        };
    }

    public static PlanMatcher projectNode(PlanMatcher... sources)
    {
        return new NodeClassPlanMatcher(ProjectNode.class, ImmutableList.copyOf(sources));
    }

    public static PlanMatcher semiJoinNode(String sourceSymbolAlias, String filteringSymbolAlias, String outputAlias, PlanMatcher... sources)
    {
        return new PlanMatcher(ImmutableList.copyOf(sources))
        {
            @Override
            public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                if (node instanceof SemiJoinNode) {
                    SemiJoinNode semiJoinNode = (SemiJoinNode) node;
                    symbolAliases.put(sourceSymbolAlias, semiJoinNode.getSourceJoinSymbol());
                    symbolAliases.put(filteringSymbolAlias, semiJoinNode.getFilteringSourceJoinSymbol());
                    symbolAliases.put(outputAlias, semiJoinNode.getSemiJoinOutput());
                    return super.matches(node, session, metadata, symbolAliases);
                }
                return ImmutableList.of();
            }
        };
    }

    public static PlanMatcher joinNode(List<AliasPair> expectedEquiCriteria, PlanMatcher... sources)
    {
        return new PlanMatcher(ImmutableList.copyOf(sources))
        {
            @Override
            public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                if (node instanceof JoinNode) {
                    JoinNode joinNode = (JoinNode) node;
                    if (joinNode.getCriteria().size() == expectedEquiCriteria.size()) {
                        int i = 0;
                        for (EquiJoinClause equiJoinClause : joinNode.getCriteria()) {
                            AliasPair expectedEquiClause = expectedEquiCriteria.get(i++);
                            symbolAliases.put(expectedEquiClause.left, equiJoinClause.getLeft());
                            symbolAliases.put(expectedEquiClause.right, equiJoinClause.getRight());
                        }
                        return super.matches(node, session, metadata, symbolAliases);
                    }
                }
                return ImmutableList.of();
            }
        };
    }

    public static AliasPair aliasPair(String left, String right)
    {
        return new AliasPair(left, right);
    }

    public static PlanMatcher filterNode(String predicate, PlanMatcher... sources)
    {
        final Expression expectedPredicate = new SqlParser().createExpression(predicate);
        return new PlanMatcher(ImmutableList.copyOf(sources))
        {
            @Override
            public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
            {
                if (node instanceof FilterNode) {
                    FilterNode filterNode = (FilterNode) node;
                    Expression actualPredicate = filterNode.getPredicate();

                    if (new ExpressionVerifier(symbolAliases).process(actualPredicate, expectedPredicate)) {
                        return super.matches(node, session, metadata, symbolAliases);
                    }
                }
                return ImmutableList.of();
            }
        };
    }

    private static class ExpressionVerifier
            extends AstVisitor<Boolean, Expression>
    {
        private final SymbolAliases symbolAliases;

        public ExpressionVerifier(SymbolAliases symbolAliases)
        {
            this.symbolAliases = requireNonNull(symbolAliases, "symbolAliases is null");
        }

        @Override
        protected Boolean visitNode(Node node, Expression context)
        {
            throw new IllegalStateException(format("Node %s is not supported", node));
        }

        @Override
        protected Boolean visitNotExpression(NotExpression actual, Expression context)
        {
            if (context instanceof NotExpression) {
                NotExpression expected = (NotExpression) context;
                return process(actual.getValue(), expected.getValue());
            }
            return false;
        }

        @Override
        protected Boolean visitQualifiedNameReference(QualifiedNameReference actual, Expression context)
        {
            if (context instanceof QualifiedNameReference) {
                QualifiedNameReference expected = (QualifiedNameReference) context;
                symbolAliases.put(expected.getName().toString(), Symbol.fromQualifiedName(actual.getName()));
                return true;
            }
            return false;
        }
    }

    public static class PlanMatcher
    {
        protected final List<PlanMatcher> sources;
        protected final List<SymbolMatcher> symbolMathcers = new ArrayList<>();

        public PlanMatcher(List<PlanMatcher> sources)
        {
            requireNonNull(sources, "sources are null");

            this.sources = ImmutableList.copyOf(sources);
        }

        public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            if (node.getSources().size() == sources.size() && symbolMathcers.stream().allMatch(it -> it.matches(node, symbolAliases))) {
                return ImmutableList.of(new PlanMatching(sources, symbolAliases));
            }
            return ImmutableList.of();
        }

        public boolean isTerminated()
        {
            return false;
        }

        public PlanMatcher withSymbol(String pattern, String alias)
        {
            symbolMathcers.add(new SymbolMatcher(pattern, alias));
            return this;
        }
    }

    private static class SymbolMatcher
    {
        private final Pattern pattern;
        private final String alias;

        public SymbolMatcher(String pattern, String alias)
        {
            this.pattern = Pattern.compile(pattern);
            this.alias = alias;
        }

        public boolean matches(PlanNode node, SymbolAliases symbolAliases)
        {
            Symbol symbol = null;
            for (Symbol outputSymbol : node.getOutputSymbols()) {
                if (pattern.matcher(outputSymbol.getName()).find()) {
                    checkState(symbol == null, "%s symbol was found multiple times in %s", pattern, node.getOutputSymbols());
                    symbol = outputSymbol;
                }
            }
            if (symbol != null) {
                symbolAliases.put(alias, symbol);
                return true;
            }
            return false;
        }
    }

    private static class NodeClassPlanMatcher
            extends PlanMatcher
    {
        private final Class<? extends PlanNode> nodeClass;

        public NodeClassPlanMatcher(Class<? extends PlanNode> nodeClass, List<PlanMatcher> planMatchers)
        {
            super(planMatchers);
            this.nodeClass = requireNonNull(nodeClass, "nodeClass is null");
        }

        public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            if (node.getClass().equals(nodeClass)) {
                return super.matches(node, session, metadata, symbolAliases);
            }
            return ImmutableList.of();
        }
    }

    private static class AnyNodesTreePlanNodeMatcher
            extends PlanMatcher
    {
        public AnyNodesTreePlanNodeMatcher(List<PlanMatcher> sources)
        {
            super(sources);
        }

        @Override
        public List<PlanMatching> matches(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            ImmutableList.Builder<PlanMatching> states = ImmutableList.builder();
            int sourcesCount = node.getSources().size();
            if (sourcesCount > 1) {
                states.add(new PlanMatching(nCopies(sourcesCount, this), symbolAliases));
            }
            else {
                states.add(new PlanMatching(ImmutableList.of(this), symbolAliases));
            }
            if (node.getSources().size() == sources.size() && symbolMathcers.stream().allMatch(it -> it.matches(node, symbolAliases))) {
                states.add(new PlanMatching(sources, symbolAliases));
            }
            return states.build();
        }

        @Override
        public boolean isTerminated()
        {
            return sources.isEmpty();
        }
    }

    private static class PlanMatching
    {
        private final List<PlanMatcher> planMatchers;
        private final SymbolAliases symbolAliases;

        private PlanMatching(List<PlanMatcher> planMatchers, SymbolAliases symbolAliases)
        {
            requireNonNull(symbolAliases, "symbolAliases is null");
            requireNonNull(planMatchers, "matchers is null");
            this.symbolAliases = new SymbolAliases(symbolAliases);
            this.planMatchers = ImmutableList.copyOf(planMatchers);
        }

        public boolean isTerminated()
        {
            return planMatchers.isEmpty() || planMatchers.stream().allMatch(PlanMatcher::isTerminated);
        }

        public PlanMatchingContext createContext(int matcherId)
        {
            checkArgument(matcherId < planMatchers.size(), "mactcherId out of scope");
            return new PlanMatchingContext(symbolAliases, planMatchers.get(matcherId));
        }
    }

    private static class PlanMatchingVisitor
            extends PlanVisitor<PlanMatchingContext, Boolean>
    {
        private final Metadata metadata;
        private final Session session;

        public PlanMatchingVisitor(Session session, Metadata metadata)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
        }

        @Override
        protected Boolean visitPlan(PlanNode node, PlanMatchingContext context)
        {
            List<PlanMatching> states = context.planMatcher.matches(node, session, metadata, context.symbolAliases);

            if (states.isEmpty()) {
                return false;
            }

            if (node.getSources().isEmpty()) {
                return !filterTerminated(states).isEmpty();
            }

            for (PlanMatching state : states) {
                checkState(node.getSources().size() == state.planMatchers.size(), "Matchers count does not match count of sources");
                int i = 0;
                boolean sourcesMatch = true;
                for (PlanNode source : node.getSources()) {
                    sourcesMatch = sourcesMatch && source.accept(this, state.createContext(i++));
                }
                if (sourcesMatch) {
                    return true;
                }
            }
            return false;
        }

        private List<PlanMatching> filterTerminated(List<PlanMatching> states)
        {
            return states.stream()
                    .filter(PlanMatching::isTerminated)
                    .collect(toImmutableList());
        }
    }

    private static class PlanMatchingContext
    {
        private final SymbolAliases symbolAliases;
        private final PlanMatcher planMatcher;

        private PlanMatchingContext(PlanMatcher planMatcher)
        {
            this(new SymbolAliases(), planMatcher);
        }

        private PlanMatchingContext(SymbolAliases symbolAliases, PlanMatcher planMatcher)
        {
            requireNonNull(symbolAliases, "symbolAliases is null");
            requireNonNull(planMatcher, "planMatcher is null");
            this.symbolAliases = new SymbolAliases(symbolAliases);
            this.planMatcher = planMatcher;
        }
    }

    private static class SymbolAliases
    {
        private final Multimap<String, Symbol> map;

        private SymbolAliases()
        {
            this.map = ArrayListMultimap.create();
        }

        private SymbolAliases(SymbolAliases symbolAliases)
        {
            requireNonNull(symbolAliases, "symbolAliases are null");
            this.map = ArrayListMultimap.create(symbolAliases.map);
        }

        public void put(String alias, Symbol symbol)
        {
            alias = alias.toLowerCase();
            if (map.containsKey(alias)) {
                checkState(map.get(alias).contains(symbol), "Alias %s points to different symbols %s and %s", alias, symbol, map.get(alias));
            }
            else {
                checkState(!map.values().contains(symbol), "Symbol %s is already pointed by different alias than %s, check mapping %s", symbol, alias, map);
                map.put(alias, symbol);
            }
        }
    }

    private PlanAssert() {}

    private static class AliasPair
    {
        private final String left;
        private final String right;

        public AliasPair(String left, String right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
        }
    }
}
