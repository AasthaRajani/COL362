package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

import java.util.List;

// Hint: Think about alias and arithmetic operations
public class PProject extends Project implements PRel {

    public PProject(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PProject copy(RelTraitSet traitSet, RelNode input,
                         List<RexNode> projects, RelDataType rowType) {
        return new PProject(getCluster(), traitSet, input, projects, rowType);
    }

    @Override
    public String toString() {
        return "PProject";
    }

    // returns true if successfully opened, false otherwise
    private Object[] row_reused;
    private int check = 0;
    @Override
    public boolean open(){
        logger.trace("Opening PProject");
        /* Write your code here */
        List <RelNode> children = getInputs();
        for(RelNode child:children){
            PRel c = (PRel) child;
            return c.open();
        }
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProject");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProject has next");
        /* Write your code here */
        PRel child = (PRel) getInput();
        while (child.hasNext()) {
            Object[] row = child.next();
            if (row != null) {
                // Set the reusable row and flag for reuse
                row_reused = row;
                check = 1;
                return true;
            }
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProject");
        /* Write your code here */
//        PRel child = (PRel) getInput();
//        Object[] row = child.next();
        Object[] row;
        if(check==0){
            PRel child = (PRel) getInput();
            row = child.next();
        }
        else{
            row = row_reused;
            check = 0;
        }
        if(row != null){
            List<RexNode> projected_columns = getProjects();
            Object[] rows = new Object[projected_columns.size()];
            for(int i=0;i<projected_columns.size();i++){
                RexNode index = projected_columns.get(i);
                Object val = eval_operandp(index,row);
                rows[i] = val;
            }
            return rows;
        }
        return null;
    }
    private Object eval_operandp(RexNode operand, Object[] row){
        if(operand instanceof RexInputRef){
            return evaluateInputRef((RexInputRef) operand, row);
        }
        else if(operand instanceof RexLiteral){
            return evaluateLiteral((RexLiteral) operand);
        }
        return null;
    }
    private Object evaluateInputRef(RexInputRef inputRef, Object[] row){
        int index = inputRef.getIndex();
        if (index >= 0 && index < row.length) {
            return row[index];
        } else {
            throw new IllegalArgumentException("Invalid column index: " + index);
        }
    }
    private Object evaluateLiteral(RexLiteral literal) {
        return literal.getValue();
    }
}