package rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;
import org.apache.calcite.sql.SqlOperator;

import java.util.*;

/*
 * Implement Hash Join
 * The left child is blocking, the right child is streaming
 */
public class PJoin extends Join implements PRel {

    public PJoin(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public PJoin copy(
            RelTraitSet relTraitSet,
            RexNode condition,
            RelNode left,
            RelNode right,
            JoinRelType joinType,
            boolean semiJoinDone) {
        return new PJoin(getCluster(), relTraitSet, left, right, condition, variablesSet, joinType);
    }

    @Override
    public String toString() {
        return "PJoin";
    }

    // returns true if successfully opened, false otherwise

    private ArrayList<Object[]> result = new ArrayList<>();
    private int counter = 0;
    @Override
    public boolean open() {
        logger.trace("Opening PJoin");
        /* Write your code here */
        PRel left_child  = (PRel) getLeft();
        PRel right_child = (PRel) getRight();
        left_child.open();
        right_child.open();
        if(getJoinType() == JoinRelType.INNER){
            RexCall call = (RexCall) getCondition();
            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();
            PRel left = left_child;
            PRel right = right_child;
            HashMap<Object, ArrayList<Object[]>> hashTable = new HashMap<>();
            int left_length = 0;
            while (left.hasNext()) {
                Object[] row = left.next();
                left_length = row.length;
                Object[] joinKey=eval_left(operator,operands,row);
                Object lkey = concatenateJoinKeys(joinKey);
                if (!hashTable.containsKey(lkey)) hashTable.put(lkey, new ArrayList<>());
                hashTable.get(lkey).add(row);
            }
            while(right.hasNext()){
                Object[] row = right.next();
                Object[] joinKey = eval_right(operator,operands,row,left_length);
                Object lkey = concatenateJoinKeys(joinKey);
                if(hashTable.containsKey(lkey)){
                    ArrayList<Object[]> same_col = hashTable.get(lkey);
                    for (Object[] col_row : same_col) {
                        Object[] combined_row = concatenateArrays(col_row,row);
                        result.add(combined_row);
                    }
                }
            }
        }
        else if(getJoinType() == JoinRelType.LEFT){
            RexCall call = (RexCall) getCondition();
            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();
            PRel left = left_child;
            PRel right = right_child;
            HashMap<Object, ArrayList<Object[]>> hashTable_L = new HashMap<>();
            HashMap<Object, ArrayList<Object[]>> hashTable_R = new HashMap<>();
            ArrayList<Object[]> left_rows = new ArrayList<>();
            int left_length = 0;
            int right_length = 0;
            while(left.hasNext()){
                Object[] row = left.next();
                left_length = row.length;
                left_rows.add(row);
                Object[] joinKey=eval_left(operator,operands,row);
                Object lkey = concatenateJoinKeys(joinKey);
                if(!hashTable_L.containsKey(lkey)) hashTable_L.put(lkey,new ArrayList<>());
                hashTable_L.get(lkey).add(row);
            }
            while(right.hasNext()){
                Object[] row = right.next();
                right_length = row.length;
                Object[] joinKey = eval_right(operator,operands,row,left_length);
                Object lkey = concatenateJoinKeys(joinKey);
                if (!hashTable_R.containsKey(lkey)) hashTable_R.put(lkey, new ArrayList<>());
                hashTable_R.get(lkey).add(row);
                if(hashTable_L.containsKey(lkey)){
                    ArrayList<Object[]> same_col = hashTable_L.get(lkey);
                    for (Object[] col_row : same_col) {
                        Object[] combined_row = concatenateArrays(col_row,row);
                        result.add(combined_row);
                    }
                }
            }
            for(int i=0;i<left_rows.size();i++){
                Object[] joinKey=eval_left(operator,operands,left_rows.get(i));
                Object lkey = concatenateJoinKeys(joinKey);
                if(!hashTable_R.containsKey(lkey)){
                    Object[] null_right_row = new Object[right_length];
                    for(int j=0;j<right_length;j++){
                        null_right_row[j]=null;
                    }
                    Object[] combined_row = concatenateArrays(left_rows.get(i),null_right_row);
                    result.add(combined_row);
                }
            }

        }
        else if(getJoinType() == JoinRelType.RIGHT){
            RexCall call = (RexCall) getCondition();
            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();
            PRel left = left_child;
            PRel right = right_child;
            HashMap<Object, ArrayList<Object[]>> hashTable = new HashMap<>();
            int left_length = 0;
            while (left.hasNext()) {
                Object[] row = left.next();
                left_length = row.length;
                Object[] joinKey=eval_left(operator,operands,row);
                Object lkey = concatenateJoinKeys(joinKey);
                if (!hashTable.containsKey(lkey)) hashTable.put(lkey, new ArrayList<>());
                hashTable.get(lkey).add(row);
            }
            while(right.hasNext()){
                Object[] row = right.next();
                Object[] joinKey = eval_right(operator,operands,row,left_length);
                Object lkey = concatenateJoinKeys(joinKey);
                if(hashTable.containsKey(lkey)){
                    ArrayList<Object[]> same_col = hashTable.get(lkey);
                    for (Object[] col_row : same_col) {
                        Object[] combined_row = concatenateArrays(col_row,row);
                        result.add(combined_row);
                    }
                }
                else{
                    Object[] null_left_row = new Object[left_length];
                    for(int i=0;i<left_length;i++){
                        null_left_row[i]=null;
                    }
                    Object[] combined_row = concatenateArrays(null_left_row,row);
                    result.add(combined_row);
                }
            }
        }
        else if(getJoinType() == JoinRelType.FULL){
            RexCall call = (RexCall) getCondition();
            SqlOperator operator = call.getOperator();
            List<RexNode> operands = call.getOperands();
            PRel left = left_child;
            PRel right = right_child;
            HashMap<Object, ArrayList<Object[]>> hashTable_L = new HashMap<>();
            HashMap<Object, ArrayList<Object[]>> hashTable_R = new HashMap<>();
            ArrayList<Object[]> left_rows = new ArrayList<>();
            int left_length = 0;
            int right_length = 0;
            while(left.hasNext()){
                Object[] row = left.next();
                left_length = row.length;
                left_rows.add(row);
                Object[] joinKey=eval_left(operator,operands,row);
                Object lkey = concatenateJoinKeys(joinKey);
                if(!hashTable_L.containsKey(lkey)) hashTable_L.put(lkey,new ArrayList<>());
                hashTable_L.get(lkey).add(row);
            }
            while(right.hasNext()){
                Object[] row = right.next();
                right_length = row.length;
                Object[] joinKey = eval_right(operator,operands,row,left_length);
                Object lkey = concatenateJoinKeys(joinKey);
                if (!hashTable_R.containsKey(lkey)) hashTable_R.put(lkey, new ArrayList<>());
                hashTable_R.get(lkey).add(row);
                if(hashTable_L.containsKey(lkey)){
                    ArrayList<Object[]> same_col = hashTable_L.get(lkey);
                    for (Object[] col_row : same_col) {
                        Object[] combined_row = concatenateArrays(col_row,row);
                        result.add(combined_row);
                    }
                }
                else{
                    Object[] null_left_row = new Object[left_length];
                    for(int i=0;i<left_length;i++){
                        null_left_row[i]=null;
                    }
                    Object[] combined_row = concatenateArrays(null_left_row,row);
                    result.add(combined_row);
                }
            }
            for(int i=0;i<left_rows.size();i++){
                Object[] joinKey=eval_left(operator,operands,left_rows.get(i));
                Object lkey = concatenateJoinKeys(joinKey);
                if(!hashTable_R.containsKey(lkey)){
                    Object[] null_right_row = new Object[right_length];
                    for(int j=0;j<right_length;j++){
                        null_right_row[j]=null;
                    }
                    Object[] combined_row = concatenateArrays(left_rows.get(i),null_right_row);
                    result.add(combined_row);
                }
            }
        }
        System.out.println("Result size is " + result.size());
        return true;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PJoin");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PJoin has next");
        /* Write your code here */
        if (counter < result.size()) {
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PJoin");
        /* Write your code here */
        if(counter < result.size()){
            counter++;
            return result.get(counter-1);
        }

        return null;
    }


    private Object eval_operand(RexNode operand, Object[] row, int len){
        if(operand instanceof RexInputRef){
            return evaluateInputRef((RexInputRef) operand, row , len);
        }
        return null;
    }
    private Object evaluateInputRef(RexInputRef inputRef, Object[] row , int len){
        int index = inputRef.getIndex();
        index = index - len;
        if (index >= 0 && index < row.length) {
            return row[index];
        } else {
            throw new IllegalArgumentException("Invalid column index: " + index);
        }
    }
    private Object[] concatenateArrays(Object[] a1, Object[] a2) {
        Object[] result = new Object[a1.length + a2.length];
        System.arraycopy(a1, 0, result, 0, a1.length);
        System.arraycopy(a2, 0, result, a1.length, a2.length);
        return result;
    }
    private Object[] eval_left (SqlOperator operator, List<RexNode> operands, Object[] row){
        switch (operator.getKind()) {
            case AND:
                return concatenateArrays(
                        Objects.requireNonNull(eval_left(((RexCall) operands.get(0)).getOperator(),
                                ((RexCall) operands.get(0)).getOperands(), row)),
                        Objects.requireNonNull(eval_left(((RexCall) operands.get(1)).getOperator(),
                                ((RexCall) operands.get(1)).getOperands(), row)));
            case EQUALS:
                Object lval3 = eval_operand(operands.get(0), row, 0);
                return new Object[]{lval3 };
        }
        return null;
    }
    private Object[] eval_right(SqlOperator operator, List<RexNode> operands, Object[] row, int len){
        switch (operator.getKind()) {
            case AND:
                return concatenateArrays(
                        Objects.requireNonNull(eval_right(((RexCall) operands.get(0)).getOperator(),
                                ((RexCall) operands.get(0)).getOperands(), row,len)),
                        Objects.requireNonNull(eval_right(((RexCall) operands.get(1)).getOperator(),
                                ((RexCall) operands.get(1)).getOperands(), row,len)));
            case EQUALS:
                Object rval3 = eval_operand(operands.get(1), row, len);
                return new Object[]{rval3 };
        }
        return null;
    }
    private Object concatenateJoinKeys(Object[] joinKeys) {
        StringBuilder concatenatedKey = new StringBuilder();
        for (Object key : joinKeys) {
            concatenatedKey.append(key.toString()); // Assuming keys are convertible to strings
            concatenatedKey.append("|"); // Add a delimiter between concatenated keys
        }
        return concatenatedKey.toString();
    }
}