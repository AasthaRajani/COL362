package rel;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.NlsString;

import java.math.BigDecimal;
import java.util.List;


public class PFilter extends Filter implements PRel {

    public PFilter(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode child,
            RexNode condition) {
        super(cluster, traits, child, condition);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new PFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public String toString() {
        return "PFilter";
    }

    // returns true if successfully opened, false otherwise

    private Object[] row_reused;
    private int check = 0;
    @Override
    public boolean open(){
        logger.trace("Opening PFilter");
        /* Write your code here */
//        System.out.println("PFilter is opened");
        List<RelNode> children = getInputs();
        for(RelNode child:children){
            PRel c = (PRel) child;
            return c.open();
        }
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PFilter");
        /* Write your code here */
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PFilter has next");
        /* Write your code here */
        PRel child = (PRel) getInput();



        boolean res = false;
        while (child.hasNext() && res==false) {
            Object[] row = child.next();
            SqlOperator operator = ((RexCall) getCondition()).getOperator();
            List<RexNode> operands = ((RexCall) getCondition()).getOperands();
            res = simplify(operator,operands,row);

            if (res == true) {
                // Set the reusable row and flag for reuse
                row_reused = row;
                check = 1;
                return true;
            }
        }
        return false;
    }
    // returns the next row
    // Hint: Try looking at different possible filter conditions
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PFilter");
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
//        SqlOperator operator = ((RexCall) getCondition()).getOperator();
//        List<RexNode> operands = ((RexCall) getCondition()).getOperands();
//        boolean res = simplify(operator,operands,row);
//        if(!res) return null;
        return row;
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

}