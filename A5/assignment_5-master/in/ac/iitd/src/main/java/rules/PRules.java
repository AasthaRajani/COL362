package rules;

import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;

import convention.PConvention;
import org.apache.calcite.rex.RexNode;
import rel.PProjectFilter;
import rel.PTableScan;

import org.checkerframework.checker.nullness.qual.Nullable;

import static org.apache.calcite.plan.RelOptRule.operand;


public class PRules {

    private PRules(){
    }

    public static final RelOptRule P_TABLESCAN_RULE = new PTableScanRule(PTableScanRule.DEFAULT_CONFIG);
//    public static final PProjectFilterRule INSTANCE = new PProjectFilterRule(PProjectFilterRule.DEFAULT_CONFIG);

    private static class PTableScanRule extends ConverterRule {

        public static final Config DEFAULT_CONFIG = Config.INSTANCE
                .withConversion(LogicalTableScan.class,
                        Convention.NONE, PConvention.INSTANCE,
                        "PTableScanRule")
                .withRuleFactory(PTableScanRule::new);

        protected PTableScanRule(Config config) {
            super(config);
        }

        @Override
        public @Nullable RelNode convert(RelNode relNode) {

            TableScan scan = (TableScan) relNode;
            final RelOptTable relOptTable = scan.getTable();

            if(relOptTable.getRowType() == scan.getRowType()) {
                return PTableScan.create(scan.getCluster(), relOptTable);
            }

            return null;
        }
    }

    // Write a class PProjectFilterRule that converts a LogicalProject followed by a LogicalFilter to a single PProjectFilter node.
    
    // You can make any changes starting here.
    public static class PProjectFilterRule extends RelOptRule{
        public static final PProjectFilterRule INSTANCE = new PProjectFilterRule();

        public PProjectFilterRule() {

            super(operand(LogicalProject.class, operand(LogicalFilter.class, any())), "PProjectFilterRule");
            System.out.println("In Pprojectfilter type");
        }


//        private PProjectFilter construct_r(LogicalProject project, LogicalFilter filter) {
//            HepRelVertex input = (HepRelVertex) filter.getInput();
//
//            while (!(input.getCurrentRel() instanceof TableScan)) {
//                if (input.getCurrentRel() instanceof LogicalProject) {
//                    project = (LogicalProject) input.getCurrentRel();
//                    input = (HepRelVertex) project.getInput();
//                } else if (input.getCurrentRel() instanceof LogicalFilter) {
//                    filter = (LogicalFilter) input.getCurrentRel();
//                    input = (HepRelVertex) filter.getInput();
//                } else {
//                    throw new IllegalArgumentException("Unsupported operation: " + input.getClass().getSimpleName());
//                }
//            }
//            TableScan scan = (TableScan) input.getCurrentRel();
//            return new PProjectFilter(project.getCluster(), project.getTraitSet(), scan, filter.getCondition(), project.getProjects(), project.getRowType());
//        }
private PProjectFilter construct(LogicalProject project, LogicalFilter filter){
    HepRelVertex temp = (HepRelVertex) filter.getInput();
    PProjectFilter rel = null;
    if (!(temp.getCurrentRel() instanceof TableScan)) {
        LogicalProject p = (LogicalProject) temp.getCurrentRel();
        temp = (HepRelVertex) p.getInput();
        LogicalFilter f = (LogicalFilter) temp.getCurrentRel();
        RelNode input = construct(p, f);
        rel = new PProjectFilter(project.getCluster(),project.getTraitSet(),input, filter.getCondition(),project.getProjects(), project.getRowType());
    }
    else if (temp.getCurrentRel() instanceof TableScan) {
        TableScan scan = (TableScan) temp.getCurrentRel();
        rel = new PProjectFilter(project.getCluster(), project.getTraitSet(), scan, filter.getCondition(), project.getProjects(), project.getRowType());
    }
    return rel;
}

@Override
public void onMatch(RelOptRuleCall call) {
    final LogicalFilter filter = call.rel(1);
    final LogicalProject project = call.rel(0);
//            HepRelVertex table_scan = (HepRelVertex) filter.getInput();
//            TableScan scan = (TableScan) table_scan.getCurrentRel();
//            RelNode input = filter.getInput();
//            RexNode condition = filter.getCondition();
//            RelOptCluster cluster = project.getCluster();
//            RelTraitSet traits = cluster.traitSet().replace(PConvention.INSTANCE);
    PProjectFilter pProjectFilter = construct(project, filter);
    call.transformTo(pProjectFilter);


}
    }

}
