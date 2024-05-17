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
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;

/*
    * PProjectFilter is a relational operator that represents a Project followed by a Filter.
    * You need to write the entire code in this file.
    * To implement PProjectFilter, you can extend either Project or Filter class.
    * Define the constructor accordinly and override the methods as required.
*/
public class PProjectFilter extends Project implements PRel {
    private final RexNode condition;
    public PProjectFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode input,
            RexNode condition,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traits, ImmutableList.of(), input, projects, rowType);
        this.condition = condition;
        System.out.println(("here asserting"));
//        assert getConvention() instanceof PConvention;
    }

    @Override
    public Project copy(RelTraitSet traitSet, RelNode input,
                         List<RexNode> projects, RelDataType rowType) {
        System.out.println("PProjectFilter copy");
        return new PProjectFilter(getCluster(), traitSet, input,condition, projects, rowType);
//        return null;
    }

    public String toString() {
        return "PProjectFilter";
    }
    private Object[] row_reused;
    private int check = 0;
    // returns true if successfully opened, false otherwise
    @Override
    public boolean open(){
        logger.trace("Opening PProjectFilter");
        /* Write your code here */
        RelNode input = getInput();
        PRel p = (PRel) input;
        return p.open();
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PProjectFilter");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PProjectFilter has next");
        /* Write your code here */
        PRel child = (PRel) getInput();
        boolean ans = false;
        boolean res = false;
        while (child.hasNext() && res==false) {
            Object[] row = child.next();
            SqlOperator operator = ((RexCall) condition).getOperator();
            List<RexNode> operands = ((RexCall) condition).getOperands();
            res = simplify(operator,operands,row);

            if (res == true) {
                // Set the reusable row and flag for reuse
                List<RexNode> projected_columns = getProjects();
                Object[] rows = new Object[projected_columns.size()];
                for(int i=0;i<projected_columns.size();i++){
                    RexNode index = projected_columns.get(i);
                    Object val = eval_operandp(index,row);
                    rows[i] = val;
                }
                row_reused = rows;
//                row_reused = row;
                check = 1;
//                return true;
                ans = true;
            }
        }
        return ans;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PProjectFilter");
        /* Write your code here */
        Object[] row;
        if(check==0){
            PRel child = (PRel) getInput();
            row = child.next();
        }
        else{
            row = row_reused;
            check = 0;
        }

//        if(row != null){
//            List<RexNode> projected_columns = getProjects();
//            Object[] rows = new Object[projected_columns.size()];
//            for(int i=0;i<projected_columns.size();i++){
//                RexNode index = projected_columns.get(i);
//                Object val = eval_operandp(index,row);
//                rows[i] = val;
//            }
//            return rows;
//        }
//        return null;
//        SqlOperator operator = ((RexCall) getCondition()).getOperator();
//        List<RexNode> operands = ((RexCall) getCondition()).getOperands();
//        boolean res = simplify(operator,operands,row);
//        if(!res) return null;
        return row;

//        return null;
    }

    @Override
    public PProjectFilter copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType, RexNode condition) {
        return null;
    }

    private Object eval_operandp(RexNode operand, Object[] row){
        if(operand instanceof RexInputRef){
            return evaluateInputRef((RexInputRef) operand, row);
        }
        else if(operand instanceof RexLiteral){
            return evaluateLiteral((RexLiteral) operand);
        }
        else{
            return evaluateOperation((RexCall) operand, row);
        }
//        return null;
    }

    private Object eval_operand(RexNode operand, Object[] row){
        if(operand instanceof RexInputRef){
            return evaluateInputRef((RexInputRef) operand, row);
        }
        else if(operand instanceof RexLiteral){
            return evaluateLiteral((RexLiteral) operand);
        }
        else{
            return evaluateOperation((RexCall) operand, row);
        }
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
    private Object evaluateOperation(RexCall call, Object[] row){
        SqlOperator operator = call.getOperator();
        List<RexNode> operands = call.getOperands();

        Object left = eval_operand(operands.get(0), row);
        Object right = eval_operand(operands.get(1), row);

        BigDecimal left_bd = toBigDecimal(left);
        BigDecimal right_bd = toBigDecimal(right);

        switch(operator.getKind()){
            case PLUS:
                return left_bd.add(right_bd);
            case MINUS:
                return left_bd.subtract(right_bd);
            case TIMES:
                return left_bd.multiply(right_bd);
            case DIVIDE:
                return left_bd.divide(right_bd);
            default:
                throw new IllegalArgumentException("Unsupported operator: " + operator);

        }
    }

    private BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        } else if (value instanceof Integer || value instanceof Double || value instanceof Float) {
            return new BigDecimal(value.toString());
        } else {
            throw new IllegalArgumentException("Unsupported operand type: " + value.getClass().getSimpleName());
        }
    }

    private boolean simplify(SqlOperator operator,List<RexNode> operands, Object[] row){
        switch(operator.getKind()){
            case AND:
                return simplify_and(operands,row);
            case OR:
                return simplify_or(operands,row);
            case NOT:
                return simplify_not(operands,row);
            case EQUALS:
                return simplify_eq(operands,row);
            case GREATER_THAN:
                return simplify_gt(operands,row);
            case GREATER_THAN_OR_EQUAL:
                return simplify_gte(operands,row);
            case LESS_THAN:
                return simplify_lt(operands,row);
            case LESS_THAN_OR_EQUAL:
                return simplify_lte(operands,row);
        }
        return false;
    }
    private boolean simplify_and(List<RexNode>operands, Object[] row){
        return simplify(((RexCall) operands.get(0)).getOperator(), ((RexCall) operands.get(0)).getOperands(), row) &&
                simplify(((RexCall) operands.get(1)).getOperator(), ((RexCall) operands.get(1)).getOperands(),row) ;
    }
    private boolean simplify_or(List<RexNode>operands, Object[] row){
        return simplify(((RexCall) operands.get(0)).getOperator(), ((RexCall) operands.get(0)).getOperands(), row) ||
                simplify(((RexCall) operands.get(1)).getOperator(), ((RexCall) operands.get(1)).getOperands(),row) ;

    }
    private boolean simplify_not(List<RexNode>operands, Object[] row){
        return !simplify(((RexCall) operands.get(0)).getOperator(), ((RexCall) operands.get(0)).getOperands(), row);
    }
    private boolean simplify_eq(List<RexNode>operands, Object[] row){
        Object left_o = eval_operand(operands.get(0),row);
        Object right_o = eval_operand(operands.get(1),row);
        if(left_o instanceof Integer ){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))==0;
        }
        else if(left_o instanceof Double){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))==0;
        }
        else if(left_o instanceof BigDecimal){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))==0;
        }
        else if(left_o instanceof Boolean){
            return (Boolean.compare((Boolean)left_o, (Boolean)right_o) == 0);
        }
        else if(left_o instanceof Float){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))==0;
        }
        else if(left_o instanceof String){
            String r = ((NlsString) right_o).getValue();
            return ((Comparable) left_o).compareTo(r)==0;
        }
        return false;
    }
    private boolean simplify_gt(List<RexNode>operands, Object[] row){
        Object left_o = eval_operand(operands.get(0),row);
        Object right_o = eval_operand(operands.get(1),row);
        if(left_o instanceof Integer ){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>0;
        }
        else if(left_o instanceof BigDecimal){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>0;
        }
        else if(left_o instanceof Double){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>0;
        }
        else if(left_o instanceof Boolean){
            return (Boolean.compare((Boolean)left_o, (Boolean)right_o) > 0);
        }
        else if(left_o instanceof Float){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>0;
        }
        else if(left_o instanceof String){
            String r = ((NlsString) right_o).getValue();
            return ((Comparable) left_o).compareTo(r)>0;
        }
        return false;
    }
    private boolean simplify_gte(List<RexNode>operands, Object[] row){
        Object left_o = eval_operand(operands.get(0),row);
        Object right_o = eval_operand(operands.get(1),row);
        if(left_o instanceof Integer ){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>=0;
        }
        else if(left_o instanceof Double){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>=0;
        }
        else if(left_o instanceof BigDecimal){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>=0;
        }
        else if(left_o instanceof Boolean){
            return (Boolean.compare((Boolean)left_o, (Boolean)right_o) >= 0);
        }
        else if(left_o instanceof Float){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))>=0;
        }
        else if(left_o instanceof String){
            String r = ((NlsString) right_o).getValue();
            return ((Comparable) left_o).compareTo(r)>=0;
        }
        return false;
    }
    private boolean simplify_lt(List<RexNode>operands, Object[] row){
        Object left_o = eval_operand(operands.get(0),row);
        Object right_o = eval_operand(operands.get(1),row);
        if(left_o instanceof Integer ){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<0;
        }
        else if(left_o instanceof Double){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<0;
        }
        else if(left_o instanceof BigDecimal){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<0;
        }
        else if(left_o instanceof Boolean){
            return (Boolean.compare((Boolean)left_o, (Boolean)right_o) < 0);
        }
        else if(left_o instanceof Float){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<0;
        }
        else if(left_o instanceof String){
            String r = ((NlsString) right_o).getValue();
            return ((Comparable) left_o).compareTo(r)<0;
        }
        return false;
    }
    private boolean simplify_lte(List<RexNode>operands, Object[] row){
        Object left_o = eval_operand(operands.get(0),row);
        Object right_o = eval_operand(operands.get(1),row);
        if(left_o instanceof Integer ){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<=0;
        }
        else if(left_o instanceof Double){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<=0;
        }
        else if(left_o instanceof BigDecimal){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<=0;
        }
        else if(left_o instanceof Boolean){
            return (Boolean.compare((Boolean)left_o, (Boolean)right_o) <= 0);
        }
        else if(left_o instanceof Float){
            return ((Comparable)toBigDecimal(left_o)).compareTo(toBigDecimal(right_o))<=0;
        }
        else if(left_o instanceof String){
            String r = ((NlsString) right_o).getValue();
            return ((Comparable) left_o).compareTo(r)<=0;
        }
        return false;
    }
//    @Override
//    public Project copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
//        return null;
//    }
}
