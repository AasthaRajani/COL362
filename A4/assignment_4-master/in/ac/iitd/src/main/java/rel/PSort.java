package rel;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rex.RexNode;

import convention.PConvention;

public class PSort extends Sort implements PRel{

    public PSort(
            RelOptCluster cluster,
            RelTraitSet traits,
            List<RelHint> hints,
            RelNode child,
            RelCollation collation,
            RexNode offset,
            RexNode fetch
    ) {
        super(cluster, traits, hints, child, collation, offset, fetch);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode input, RelCollation collation, RexNode offset, RexNode fetch) {
        return new PSort(getCluster(), traitSet, hints, input, collation, offset, fetch);
    }

    @Override
    public String toString() {
        return "PSort";
    }

    // returns true if successfully opened, false otherwise
    private List<Object[]> all_rows = new ArrayList<>();
    private int counter =0;
    @Override
    public boolean open(){
        logger.trace("Opening PSort");
        /* Write your code here */
        PRel child = (PRel) getInput();
        boolean op = child.open();
        if(!op) return false;
        while(child.hasNext()){
            Object[] row = child.next();
            if(row!=null){
                all_rows.add(row);
            }
        }

        Comparator<Object[]> hierarchicalComparator = (row1, row2) -> {
            for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                int columnIndex = fieldCollation.getFieldIndex();
                Comparable value1 = (Comparable) row1[columnIndex];
                Comparable value2 = (Comparable) row2[columnIndex];

                int result;
                if (fieldCollation.getDirection().isDescending()) {
                    result = value2.compareTo(value1); // Compare in reverse for descending
                } else {
                    result = value1.compareTo(value2); // Compare normally for ascending
                }

                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };

        all_rows.sort(hierarchicalComparator);
        sort_all_rows(all_rows);
        return true;
    }
    private void sort_all_rows(List<Object[]> rows){
        Comparator<Object[]> hierarchicalComparator = (row1, row2) -> {
            for (RelFieldCollation fieldCollation : collation.getFieldCollations()) {
                int columnIndex = fieldCollation.getFieldIndex();
                Comparable value1 = (Comparable) row1[columnIndex];
                Comparable value2 = (Comparable) row2[columnIndex];
                int result;
//                int result = compareValues(value1, value2, fieldCollation.getDirection());
                if (fieldCollation.getDirection().isDescending()) {
                    result = value2.compareTo(value1); // Compare in reverse for descending
                } else {
                    result = value1.compareTo(value2); // Compare normally for ascending
                }
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };
        all_rows.sort(hierarchicalComparator);
    }
    // any postprocessing, if needed
    @Override
    public void close(){
        logger.trace("Closing PSort");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext(){
        logger.trace("Checking if PSort has next");
        /* Write your code here */
        if(counter<all_rows.size()){
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next(){
        logger.trace("Getting next row from PSort");
        /* Write your code here */
        if(counter<all_rows.size()){
            Object[] row = all_rows.get(counter);
            counter++;
            return row;
        }
        return null;
    }

}