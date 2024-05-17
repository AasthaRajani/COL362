package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.*;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            List<RelHint> hints,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    // returns true if successfully opened, false otherwise
    private List<Object[]> all_rows = new ArrayList<>();
    HashMap<String, List<Integer>> columnIndexMap = new HashMap<>();
    HashMap<String, Boolean> find_dis = new HashMap<>();
    List<Object[]> result = new ArrayList<>();
    private int counter = 0;
    private List<SqlTypeName> extract_dt(List<RelDataTypeField> fields){
        List<SqlTypeName> dataTypesList = new ArrayList<>();
        for (RelDataTypeField field : fields) {
            SqlTypeName dataType = field.getType().getSqlTypeName();
            dataTypesList.add(dataType);
        }
        return dataTypesList;
    }
    private void create_map(){
        for (AggregateCall aggregateCall : aggCalls) {
            String functionName = aggregateCall.getAggregation().getName();
            List<Integer> columnIndex = new ArrayList<>(aggregateCall.getArgList());
            Boolean is_dis = aggregateCall.isDistinct();
            columnIndexMap.put(functionName, columnIndex);
            find_dis.put(functionName, is_dis);
        }
    }
    @Override
    public boolean open() {
        logger.trace("Opening PAggregate");
        /* Write your code here */
        final RelDataType type_of_row = getRowType() ;
        List<RelDataTypeField> fields = type_of_row.getFieldList();
        List<SqlTypeName> datatypes_list = extract_dt(fields);
        PRel child = (PRel) getInput();
        if(!child.open()){
            return false;
        }
        while(child.hasNext()){
            Object[] row = child.next();
            if(row!=null){
                all_rows.add(row);
            }
        }
        System.out.println("Rows accumulated " + all_rows.size());
        create_map();
        if (groupSet.isEmpty()) {
//            System.out.println("Without grouping called");
            aggregateWithoutGrouping(datatypes_list, all_rows, result);
        } else {
//            System.out.println("With grouping called");
            aggregateWithGrouping(datatypes_list, all_rows, result);
        }

//        List<Object[]> result = new ArrayList<>();
//        for (Object[] array : result) {
//            for (Object element : array) {
//                System.out.print(element + " ");
//            }
//            System.out.println(); // Move to the next line after printing each array
//        }

        return true;
    }
    private void aggregateWithoutGrouping(List<SqlTypeName> dataTypesList, List<Object[]> all_rows, List<Object[]> result){
        Object[] newRow = new Object[dataTypesList.size()];
        for (int i = 0; i < aggCalls.size(); i++) {
            AggregateCall aggregateCall = aggCalls.get(i);
            List<Integer> columnIndex = columnIndexMap.get(aggregateCall.getAggregation().getName());
            boolean is_dis = find_dis.get(aggregateCall.getAggregation().getName());
            SqlTypeName dataType = dataTypesList.get(i);

            switch (aggregateCall.getAggregation().getName()) {
                case "COUNT":
                    newRow[i] = calculateCount(all_rows, columnIndex,is_dis);
                    break;
                case "SUM":
                    if(!columnIndex.isEmpty()) newRow[i] = calculateSum(all_rows, columnIndex, dataType, is_dis);
                    break;
                case "MAX":
                    if(!columnIndex.isEmpty()) newRow[i] = calculateMax(all_rows, columnIndex, dataType);
                    break;
                case "MIN":
                    if(!columnIndex.isEmpty()) newRow[i] = calculateMin(all_rows, columnIndex, dataType);
                    break;
                case "AVG":
                    if(!columnIndex.isEmpty()) newRow[i] = calculateAverage(all_rows,columnIndex,dataType, is_dis);
            }
        }
        result.add(newRow);
    }
    private void aggregateWithGrouping(List<SqlTypeName> dataTypesList, List<Object[]> all_rows, List<Object[]> result){
        HashMap<List<Object>, List<Object>> groupAggregates = new HashMap<>();
        List<HashMap<List<Object>, Integer>> dist_list = new ArrayList<>(Collections.nCopies(aggCalls.size(), new HashMap<>()));

        for(Object[] row:all_rows){
            List<Object> groupKey = new ArrayList<>();
            for (Integer columnIndex : groupSet) {
                groupKey.add(row[columnIndex]);
            }
//            System.out.println("Calculate aggregates called");
            if (aggCalls.isEmpty()) {
                groupAggregates.putIfAbsent(groupKey, new ArrayList<>(Collections.singletonList(1)));
            }

            calculateAggregates(aggCalls, row, groupKey, groupAggregates,dataTypesList,dist_list );
//            System.out.println("Calculate aggregates returned");
        }
        for (HashMap.Entry<List<Object>, List<Object>> entry : groupAggregates.entrySet()) {
            List<Object> key = entry.getKey();
            List<Object> value = entry.getValue();
            Object[] row = new Object[key.size() + value.size()];
            for (int j = 0; j < key.size(); j++) {
                row[j] = key.get(j);
            }
            for (int j = 0; j < value.size(); j++) {
                row[key.size() + j] = value.get(j);
            }
            result.add(row);
        }

    }
    // any postprocessing, if needed
    private void calculateAggregates(List<AggregateCall>aggCalls, Object[] row, List<Object> groupKey, HashMap<List<Object>, List<Object>> groupAggregates, List<SqlTypeName> dataTypesList, List<HashMap<List<Object>, Integer>> dist_list){
        for (int j=0;j<aggCalls.size();j++) {
            String functionName = aggCalls.get(j).getAggregation().getName();
//            System.out.println("function name "+ functionName);
            List<Integer> columnIndex = columnIndexMap.get(functionName);
//            System.out.println("Column index calculated");
//            List<Object> aggregateValues = groupAggregates.getOrDefault(groupKey, new ArrayList<>());
            SqlTypeName dtype = dataTypesList.get(j+groupKey.size());
//            System.out.println("Datatypes calculated");
//            SqlTypeName dtype = data.get(start);
            boolean is_dis = find_dis.get(functionName);
            HashMap<List<Object>,Integer> h_p = dist_list.get(j);
            switch (functionName) {
                case "COUNT":
//                    System.out.println("Calculate count called");
                    updateCountAggregate(functionName,columnIndex,groupKey,j,groupAggregates,row,is_dis,h_p);
                    break;
                case "SUM":
//                    System.out.println("Calculate sum called");
                    updateSumAggregate(functionName,columnIndex,dtype,groupKey,row,groupAggregates,j,is_dis,h_p);
                    break;
                case "MAX":
                    updateMaxAggregate(functionName,columnIndex,dtype,groupKey,row,groupAggregates,j);
                    break;
                case "MIN":
                    updateMinAggregate(functionName,columnIndex,dtype,groupKey,row,groupAggregates,j);
                    break;
                case "AVG":
                    updateAvgAggregate(functionName,columnIndex,dtype,groupKey,row,groupAggregates,j);
                    break;
                // Add cases for other aggregate functions if needed
            }


        }

    }
    private void updateCountAggregate(String fun_name, List<Integer> ind, List<Object> list, int j,HashMap<List<Object>, List<Object>> h, Object[] row, boolean isdis,HashMap<List<Object>, Integer> dist_list) {
        int count = 0;
        if (!ind.isEmpty()) {
            boolean b = false;
            for(int i:ind){
                if(row[i]==null) {
                    b = true;
                    break;
                }
            }
            count = !b ? 1 : 0;
        } else if (ind.isEmpty()) {
            count = 1;
        }
        if (h.containsKey(list)) {
            List<Object> l = h.get(list);
            if(l.size() == j) l.add(0);
            Object cnt = l.get(j);
            if(!isdis)cnt = (int) cnt + count;
            else{
                List<Object> tt = new ArrayList<>();
                boolean b = false;
                for(int i:ind){
                    if(row[i]==null) {
                        b = true;
                        break;
                    }
                    else{
                        tt.add(row[i]);
                    }
                }
                if(!b && !dist_list.containsKey(tt)){
                    dist_list.put(tt,1);
                    cnt = (Integer) cnt + 1;
                };
            }
            l.set(j, cnt);
//            System.out.println("j is "+j+" cnt is "+(int) cnt);
            h.put(list,l);
        } else {
            List<Object> l = new ArrayList<>();
            l.add(count);
            h.put(list, l);
        }
    }

    // Helper function to update the sum aggregate
    private void updateSumAggregate(String functionName, List<Integer> index, SqlTypeName dtype, List<Object> list, Object[] row, HashMap<List<Object>, List<Object>> hashMap, int j, boolean isdis, HashMap<List<Object>, Integer> dist_list) {
        if (!index.isEmpty()) {
            int ind = index.get(0);
//            System.out.println("Ind is "+ind);
            List<Object> l = hashMap.getOrDefault(list, new ArrayList<>());
            if (l.size() == j && hashMap.containsKey(list)) {
                if(dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) l.add(0);
                if(dtype == SqlTypeName.FLOAT) l.add(0.0f);
                if(dtype == SqlTypeName.DOUBLE) l.add(0.0);

            }
            Object cnt = null;
            if(hashMap.containsKey(list)){
                cnt = l.get(j);
            }
            else {
//                if((Integer)list.get(0)==341) System.out.println("Setting 0 entered");
                if(dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) cnt = 0;
                if(dtype == SqlTypeName.FLOAT) cnt = 0.0f;
                if(dtype == SqlTypeName.DOUBLE) cnt = 0.0;
            }
            if (row[ind] != null) {
                if(!isdis) {
                    switch (dtype) {
                        case INTEGER:
                        case BIGINT:
                            cnt = (Integer) cnt + (Integer) row[ind];
                            break;
                        case FLOAT:
                            cnt = (Float) cnt + (Float) row[ind];
                            break;
                        case DOUBLE:
//                        for(Object val:row){
//                            System.out.print(val + " ");
//                        }
//                        System.out.println("\n-------------------------------------------------------------");
//                        if((Integer)list.get(0)==341) {
//                            System.out.print("Old sum is " + cnt + " Value added is " + row[ind]);
                            cnt = (Double) cnt + (Double) row[ind];
//                            System.out.print(" New sum is " + cnt + "\n");
//                        }
                            break;
                    }
                }
                else{
                    List<Object> tt = new ArrayList<>();
                    tt.add(row[ind]);
                    if(!dist_list.containsKey(tt)){
                        dist_list.put(tt,1);
                        switch (dtype) {
                            case INTEGER:
                            case BIGINT:
                                cnt = (Integer) cnt + (Integer) row[ind];
                                break;
                            case FLOAT:
                                cnt = (Float) cnt + (Float) row[ind];
                                break;
                            case DOUBLE:
//                        for(Object val:row){
//                            System.out.print(val + " ");
//                        }
//                        System.out.println("\n-------------------------------------------------------------");
//                        if((Integer)list.get(0)==341) {
//                            System.out.print("Old sum is " + cnt + " Value added is " + row[ind]);
                                cnt = (Double) cnt + (Double) row[ind];
//                            System.out.print(" New sum is " + cnt + "\n");
//                        }
                                break;
                        }
                    }
                }
            }
            if(!hashMap.containsKey(list)){
//                for(Object val:row){
//                    System.out.print(val + " ");
//                }
//                System.out.println("\n-------------------------------------------------------------");
//                if((Integer)list.get(0)==341)System.out.println("I am getting added to hashmap "+ cnt);
                l.add(cnt);
            }
            else {
                l.set(j, cnt);
            }

            hashMap.put(list, l);
        }
    }

    // Helper function to update the max aggregate
    private void updateMaxAggregate(String functionName, List<Integer> index, SqlTypeName dtype, List<Object> list, Object[] row, HashMap<List<Object>, List<Object>> hashMap, int j) {
        if (!index.isEmpty()) {
            int ind = index.get(0);
            Object max = null;
            List<Object> l = hashMap.getOrDefault(list, new ArrayList<>());
            if (l.size() == j && hashMap.containsKey(list)) {
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        l.add(Integer.MIN_VALUE);
                        break;
                    case FLOAT:
                        l.add(Float.MIN_VALUE);
                        break;
                    case DOUBLE:
                        l.add(Double.MIN_VALUE);
                        break;
                }
            }

            if(hashMap.containsKey(list)) max = l.get(j);
            else{
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        max = (Integer.MIN_VALUE);
                        break;
                    case FLOAT:
                        max = (Float.MIN_VALUE);
                        break;
                    case DOUBLE:
                        max = (Double.MIN_VALUE);
                        break;
                }
            }
            if (row[ind] != null) {
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        if (((Integer) max).compareTo((Integer) row[ind]) < 0) {
                            max = row[ind];
                        }
                        break;
                    case FLOAT:
                        if (((Float) max).compareTo((Float) row[ind]) < 0) {
                            max = row[ind];
                        }
                        break;
                    case DOUBLE:
                        if (((Double) max).compareTo((Double) row[ind]) < 0) {
                            max = row[ind];
                        }
                        break;
                }
            }
            if(hashMap.containsKey(list)) {l.set(j, max);}
            else { l.add(max);}
            hashMap.put(list, l);
        }
//        else {
//            List<Object> l = new ArrayList<>();
//            switch (dtype) {
//                case INTEGER:
//                case BIGINT:
//                    l.add(Integer.MIN_VALUE);
//                    break;
//                case FLOAT:
//                    l.add(Float.MIN_VALUE);
//                    break;
//                case DOUBLE:
//                    l.add(Double.MIN_VALUE);
//                    break;
//            }
//            hashMap.put(list, l);
//            c++;
//        }
    }


    // Helper function to update the min aggregate
    private void updateMinAggregate(String functionName,List<Integer> index, SqlTypeName dtype, List<Object> list, Object[] row, HashMap<List<Object>, List<Object>> hashMap, int j) {
        if (!index.isEmpty()) {
            int ind = index.get(0);
            Object max = null;
            List<Object> l = hashMap.getOrDefault(list, new ArrayList<>());
            if (l.size() == j && hashMap.containsKey(list)) {
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        l.add(Integer.MAX_VALUE);
                        break;
                    case FLOAT:
                        l.add(Float.MAX_VALUE);
                        break;
                    case DOUBLE:
                        l.add(Double.MAX_VALUE);
                        break;
                }
            }

            if(hashMap.containsKey(list)) max = l.get(j);
            else{
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        max = (Integer.MAX_VALUE);
                        break;
                    case FLOAT:
                        max = (Float.MAX_VALUE);
                        break;
                    case DOUBLE:
                        max = (Double.MAX_VALUE);
                        break;
                }
            }
            if (row[ind] != null) {
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        if (((Integer) max).compareTo((Integer) row[ind]) > 0) {
                            max = row[ind];
                        }
                        break;
                    case FLOAT:
                        if (((Float) max).compareTo((Float) row[ind]) > 0) {
                            max = row[ind];
                        }
                        break;
                    case DOUBLE:
                        if (((Double) max).compareTo((Double) row[ind]) > 0) {
                            max = row[ind];
                        }
                        break;
                }
            }
            if(hashMap.containsKey(list)) {l.set(j, max);}
            else { l.add(max);}
            hashMap.put(list, l);
        }
//        else {
//            List<Object> l = new ArrayList<>();
//            switch (dtype) {
//                case INTEGER:
//                case BIGINT:
//                    l.add(Integer.MIN_VALUE);
//                    break;
//                case FLOAT:
//                    l.add(Float.MIN_VALUE);
//                    break;
//                case DOUBLE:
//                    l.add(Double.MIN_VALUE);
//                    break;
//            }
//            hashMap.put(list, l);
//            c++;
//        }
    }
    private int count =0;
    HashMap<List<Object>,Integer> count_map_for_avg = new HashMap<>();
    private void updateAvgAggregate(String functionName,List<Integer> index, SqlTypeName dtype, List<Object> list, Object[] row, HashMap<List<Object>, List<Object>> hashMap, int j){
        if (!index.isEmpty()) {
            int ind = index.get(0);
            List<Object> l = hashMap.getOrDefault(list, new ArrayList<>());
            if (l.size() == j && hashMap.containsKey(list)) {
                if(dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) l.add(0);
                if(dtype == SqlTypeName.FLOAT) l.add(0.0f);
                if(dtype == SqlTypeName.DOUBLE) l.add(0.0);
            }
            Object cnt = null;
            if(hashMap.containsKey(list)){
                cnt = l.get(j);
            }
            else {
                if(dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) cnt = 0;
                if(dtype == SqlTypeName.FLOAT) cnt = 0.0f;
                if(dtype == SqlTypeName.DOUBLE) cnt = 0.0;
            }

            count = count_map_for_avg.getOrDefault(list, 0);
            if (row[ind] != null) {
                switch (dtype) {
                    case INTEGER:
                    case BIGINT:
                        cnt = ((Integer) cnt * count) + (Integer) row[ind];
                        count ++;
                        cnt = (Integer) cnt / count;
                        break;
                    case FLOAT:
                        cnt = ((Float) cnt * count) + (Float) row[ind];
                        count ++;
                        cnt = (Float) cnt / count;
                        break;
                    case DOUBLE:
                        if((Integer) list.get(0) == 134)System.out.print("OldCnt is "+cnt + " Row[ind] is "+row[ind]+" ");
                        cnt = ((Double) cnt * count) + (Double) row[ind];

                        count ++;
                        cnt = (Double) cnt/count;
                        if((Integer) list.get(0) == 134)System.out.println("New cnt is " + cnt + " Count is " + count);
                        break;
                }
            }
//            else if(row[ind])
            if(!hashMap.containsKey(list)){
                l.add(cnt);
            }
            else {
                l.set(j, cnt);
            }
            if(!count_map_for_avg.containsKey(list)){
                count_map_for_avg.put(list,1);
            }
            else {
                count_map_for_avg.put(list,count);
            }
            hashMap.put(list, l);
        }
    }
    private int calculateCount(List<Object[]> accumulatedRows, List<Integer> columnIndex, boolean isdis) {
        if (!columnIndex.isEmpty()) {

            int count = 0;
            if(!isdis) {
                for (Object[] row : accumulatedRows) {
                    int flag = 0;
                    for (int index : columnIndex) {
                        if (row[index] == null) {
                            flag = 1;
                            break;
                        }
                    }
                    if (flag == 0) count++;
                }
            }
            else{
                HashMap<List<Object>, Integer> hashMap = new HashMap<>();
                for (Object[] row : accumulatedRows) {
                    boolean isValid = true;
                    List<Object> list = new ArrayList<>();
                    for (int ind : columnIndex) {
                        if (row[ind] == null) {
                            isValid = false;
                            break;
                        } else {
                            list.add(row[ind]);
                        }
                    }
                    if (isValid) {
                        if (!hashMap.containsKey(list)) {
                            hashMap.put(list, 1);
                            count++;
                        }
                    }
                }
            }
            return count;
        } else {
            return accumulatedRows.size();
        }
    }

    private double calculateSum(List<Object[]> accumulatedRows, List<Integer>  columns, SqlTypeName dataType, boolean isdis) {
        double sum = 0;
        int columnIndex = columns.get(0);
        if(!isdis) {
            for (Object[] row : accumulatedRows) {
                if (row[columnIndex] != null) {
                    if (dataType == SqlTypeName.INTEGER || dataType == SqlTypeName.BIGINT) {
                        sum += (Integer) row[columnIndex];
                    } else if (dataType == SqlTypeName.FLOAT) {
                        sum += (Float) row[columnIndex];
                    } else if (dataType == SqlTypeName.DOUBLE) {
                        sum += (Double) row[columnIndex];
                    }
                }
            }
        }
        else{
            HashMap<Object, Integer> hashMap = new HashMap<>();
            for (Object[] row : accumulatedRows) {
                if(row[columnIndex] != null && !hashMap.containsKey(row[columnIndex])) {
                    hashMap.put(row[columnIndex], 1);
                    if (dataType == SqlTypeName.INTEGER || dataType == SqlTypeName.BIGINT) {
                        sum += (Integer) row[columnIndex];
                    } else if (dataType == SqlTypeName.FLOAT) {
                        sum += (Float) row[columnIndex];
                    } else if (dataType == SqlTypeName.DOUBLE) {
                        sum += (Double) row[columnIndex];
                    }
                }

            }
        }
        return sum;
    }

    private Object calculateMax(List<Object[]> accumulatedRows, List<Integer>  column, SqlTypeName dataType) {
        Object max = null;
        int columnIndex = column.get(0);
        if (dataType == SqlTypeName.INTEGER) {
            max = Integer.MIN_VALUE;
        } else if (dataType == SqlTypeName.FLOAT) {
            max = Float.MIN_VALUE;
        } else if (dataType == SqlTypeName.DOUBLE) {
            max = Double.MIN_VALUE;
        }
        for (Object[] row : accumulatedRows) {
            if (row[columnIndex] != null) {
                if (dataType == SqlTypeName.INTEGER && (Integer) row[columnIndex] > (Integer) max) {
                    max = row[columnIndex];
                } else if (dataType == SqlTypeName.FLOAT && (Float) row[columnIndex] > (Float) max) {
                    max = row[columnIndex];
                } else if (dataType == SqlTypeName.DOUBLE && (Double) row[columnIndex] > (Double) max) {
                    max = row[columnIndex];
                }
            }
        }
        return max;
    }

    private Object calculateMin(List<Object[]> accumulatedRows, List<Integer>  column, SqlTypeName dataType) {
        Object min = null;
        int columnIndex = column.get(0);
        if (dataType == SqlTypeName.INTEGER) {
            min = Integer.MAX_VALUE;
        } else if (dataType == SqlTypeName.FLOAT) {
            min = Float.MAX_VALUE;
        } else if (dataType == SqlTypeName.DOUBLE) {
            min = Double.MAX_VALUE;
        }
        for (Object[] row : accumulatedRows) {
            if (row[columnIndex] != null) {
                if (dataType == SqlTypeName.INTEGER && (Integer) row[columnIndex] < (Integer) min) {
                    min = row[columnIndex];
                } else if (dataType == SqlTypeName.FLOAT && (Float) row[columnIndex] < (Float) min) {
                    min = row[columnIndex];
                } else if (dataType == SqlTypeName.DOUBLE && (Double) row[columnIndex] < (Double) min) {
                    min = row[columnIndex];
                }
            }
        }
        return min;
    }
    private Object calculateAverage(List<Object[]> accumulatedRows, List<Integer> l_ind, SqlTypeName dtype, boolean isdis) {
        int ind = l_ind.get(0);
        Object sum = null;
        int count = 0;

        if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
            sum = 0;
        } else if (dtype == SqlTypeName.FLOAT) {
            sum = 0.0f;
        } else if (dtype == SqlTypeName.DOUBLE) {
            sum = 0.0;
        }
        if(!isdis) {
            for (Object[] row : accumulatedRows) {
                if (row[ind] != null) {
                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                        sum = (Integer) sum + (Integer) row[ind];
                    } else if (dtype == SqlTypeName.FLOAT) {
                        sum = (Float) sum + (Float) row[ind];
                    } else if (dtype == SqlTypeName.DOUBLE) {
                        sum = (Double) sum + (Double) row[ind];
                    }
                    count++;
                }
            }
        }
        else{
            HashMap<Object, Integer> hashMap = new HashMap<>();
            for (Object[] row : accumulatedRows) {
                if(row[ind] != null && !hashMap.containsKey(row[ind])) {
                    hashMap.put(row[ind], 1);
                    if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                        sum = (Integer) sum + (Integer) row[ind];
                    } else if (dtype == SqlTypeName.FLOAT) {
                        sum = (Float) sum + (Float) row[ind];
                    } else if (dtype == SqlTypeName.DOUBLE) {
                        sum = (Double) sum + (Double) row[ind];
                    }
                    count++;
                }
            }
        }
        if (count > 0) {
            // Calculate average
            if (dtype == SqlTypeName.INTEGER || dtype == SqlTypeName.BIGINT) {
                sum = (Integer) sum / count;
            } else if (dtype == SqlTypeName.FLOAT) {
                sum = (Float) sum / count;
            } else if (dtype == SqlTypeName.DOUBLE) {
                sum = (Double) sum / count;
            }
        }

        return sum;
 // Handle case when l_ind is empty
    }

    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        /* Write your code here */
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        /* Write your code here */
        if(counter < result.size()) return true;
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if(counter<result.size()) {
            counter++;
            return result.get(counter - 1);
        }
        return null;
    }

}