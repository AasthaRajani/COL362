package optimizer.rel;

import com.sun.org.apache.xpath.internal.operations.Bool;
import index.bplusTree.LeafNode;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import manager.StorageManager;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import storage.Block;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// Operator trigged when doing indexed scan
// Matches SFW queries with indexed columns in the WHERE clause
public class PIndexScan extends TableScan implements PRel {

    private final List<RexNode> projects;
    private final RelDataType rowType;
    private final RelOptTable table;
    private final RexNode filter;

    public PIndexScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, RexNode filter, List<RexNode> projects) {
        super(cluster, traitSet, table);
        this.table = table;
        this.rowType = deriveRowType();
        this.filter = filter;
        this.projects = projects;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new PIndexScan(getCluster(), traitSet, table, filter, projects);
    }

    @Override
    public RelOptTable getTable() {
        return table;
    }

    @Override
    public String toString() {
        return "PIndexScan";
    }

    public String getTableName() {
        return table.getQualifiedName().get(1);
    }
    private byte[] convertToByteArray(Object[] row, List<RelDataType> typeList) {

        List<Byte> fixed_length_Bytes = new ArrayList<>();
        List<Byte> variable_length_Bytes = new ArrayList<>();
        List<Integer> variable_length = new ArrayList<>();
        List<Boolean> fixed_length_nullBitmap = new ArrayList<>();
        List<Boolean> variable_length_nullBitmap = new ArrayList<>();

        for(int i = 0; i < row.length; i++) {

            if(typeList.get(i).getSqlTypeName().getName().equals("INTEGER")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    int val = (int) row[i];
                    byte[] intBytes = new byte[4];
                    intBytes[0] = (byte) (val & 0xFF);
                    intBytes[1] = (byte) ((val >> 8) & 0xFF);
                    intBytes[2] = (byte) ((val >> 16) & 0xFF);
                    intBytes[3] = (byte) ((val >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(intBytes[j]);
                    }
                }
            } else if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                if(row[i] == null){
                    variable_length_nullBitmap.add(true);
                    for(int j = 0; j < 1; j++) {
                        variable_length_Bytes.add((byte) 0);
                    }
                } else {
                    variable_length_nullBitmap.add(false);
                    String val = (String) row[i];
                    byte[] strBytes = val.getBytes();
                    for(int j = 0; j < strBytes.length; j++) {
                        variable_length_Bytes.add(strBytes[j]);
                    }
                    variable_length.add(strBytes.length);
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("BOOLEAN")) {
                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    fixed_length_Bytes.add((byte) 0);
                } else {
                    fixed_length_nullBitmap.add(false);
                    boolean val = (boolean) row[i];
                    fixed_length_Bytes.add((byte) (val ? 1 : 0));
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("FLOAT")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    float val = (float) row[i];
                    byte[] floatBytes = new byte[4];
                    int intBits = Float.floatToIntBits(val);
                    floatBytes[0] = (byte) (intBits & 0xFF);
                    floatBytes[1] = (byte) ((intBits >> 8) & 0xFF);
                    floatBytes[2] = (byte) ((intBits >> 16) & 0xFF);
                    floatBytes[3] = (byte) ((intBits >> 24) & 0xFF);
                    for(int j = 0; j < 4; j++) {
                        fixed_length_Bytes.add(floatBytes[j]);
                    }
                }
            } else if (typeList.get(i).getSqlTypeName().getName().equals("DOUBLE")) {

                if(row[i] == null){
                    fixed_length_nullBitmap.add(true);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add((byte) 0);
                    }
                } else {
                    fixed_length_nullBitmap.add(false);
                    double val = (double) row[i];
                    byte[] doubleBytes = new byte[8];
                    long longBits = Double.doubleToLongBits(val);
                    doubleBytes[0] = (byte) (longBits & 0xFF);
                    doubleBytes[1] = (byte) ((longBits >> 8) & 0xFF);
                    doubleBytes[2] = (byte) ((longBits >> 16) & 0xFF);
                    doubleBytes[3] = (byte) ((longBits >> 24) & 0xFF);
                    doubleBytes[4] = (byte) ((longBits >> 32) & 0xFF);
                    doubleBytes[5] = (byte) ((longBits >> 40) & 0xFF);
                    doubleBytes[6] = (byte) ((longBits >> 48) & 0xFF);
                    doubleBytes[7] = (byte) ((longBits >> 56) & 0xFF);
                    for(int j = 0; j < 8; j++) {
                        fixed_length_Bytes.add(doubleBytes[j]);
                    }
                }
            } else {
                System.out.println("Unsupported type");
                throw new RuntimeException("Unsupported type");
            }
        }

        short num_bytes_for_bitmap = (short) ((fixed_length_nullBitmap.size() + variable_length_nullBitmap.size() + 7) / 8); // should be in multiples of bytes

        //                       bytes for fixed length and variable length fields          offset & length of var fields
        byte[] result = new byte[fixed_length_Bytes.size() + variable_length_Bytes.size() + 4 * variable_length.size() + num_bytes_for_bitmap];
        int variable_length_offset = 4 * variable_length.size() + fixed_length_Bytes.size() + num_bytes_for_bitmap;

        int idx = 0;
        for(; idx < variable_length.size() ; idx ++){
            // first 2 bytes should be offset
            result[idx * 4] = (byte) (variable_length_offset & 0xFF);
            result[idx * 4 + 1] = (byte) ((variable_length_offset >> 8) & 0xFF);

            // next 2 bytes should be length
            result[idx * 4 + 2] = (byte) (variable_length.get(idx) & 0xFF);
            result[idx * 4 + 3] = (byte) ((variable_length.get(idx) >> 8) & 0xFF);

            variable_length_offset += variable_length.get(idx);
        }

        idx = idx * 4;

        // write fixed length fields
        for(int i = 0; i < fixed_length_Bytes.size(); i++, idx++) {
            result[idx] = fixed_length_Bytes.get(i);
        }

        // write null bitmap
        int bitmap_idx = 0;
        for(int i = 0; i < fixed_length_nullBitmap.size(); i++) {
            if(fixed_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }
        for(int i = 0; i < variable_length_nullBitmap.size(); i++) {
            if(variable_length_nullBitmap.get(i)) {
                result[idx] |= (1 << (7 - bitmap_idx));
            }
            bitmap_idx++;
            if(bitmap_idx == 8) {
                bitmap_idx = 0;
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }
    private boolean compare(byte[] row,RexLiteral value){
        SqlTypeName val = value.getType().getSqlTypeName();

        if(val== SqlTypeName.INTEGER){
            ByteBuffer buffer =ByteBuffer.wrap(row);
            buffer.order(ByteOrder.BIG_ENDIAN);
            int row_val = buffer.getInt();
            BigDecimal decimalValue = (BigDecimal) value.getValue();
            int rexValue = decimalValue.intValue();

            return(rexValue==row_val);
        }
        else if(val== SqlTypeName.VARCHAR){

            String rexValue = (String) value.getValue();
            String row_val = new String(row);
            return (rexValue == row_val);
        }
        else if(val == SqlTypeName.FLOAT){
            if(row.length!=4){
                return false;
            }
            int row_val = ((row[3] & 0xFF) << 24) | ((row[2] & 0xFF) << 16) | ((row[1] & 0xFF) << 8) | (row[0] & 0xFF);
            float val_1 = Float.intBitsToFloat(row_val);
            float rexValue = ((Float) value.getValue()).floatValue();
            return(rexValue==val_1);
        }
        else if(val == SqlTypeName.DOUBLE || val == SqlTypeName.DECIMAL ){
            if(row.length!=8){
                return false;
            }
            long longBits;
            longBits = ((row[7] & 0xFFL) << 56) | ((row[ 6] & 0xFFL) << 48) | ((row[5] & 0xFFL) << 40) |
                    ((row[ 4] & 0xFFL) << 32) | ((row[3] & 0xFFL) << 24) | ((row[2] & 0xFFL) << 16) |
                    ((row[1] & 0xFFL) << 8) | (row[0] & 0xFFL);
            double row_val = Double.longBitsToDouble(longBits);
            double rexValue = ((Double) value.getValue()).doubleValue();
            return (rexValue==row_val);
        }
        else if(val == SqlTypeName.BOOLEAN){
            if(row.length!=1){
                return false;
            }
            boolean row_val=row[0]!=0;
            boolean rexValue = ((Boolean) value.getValue()).booleanValue();
            return(rexValue == row_val);
        }
        else{
            return false;
        }
    }
    private List<RelDataType> convertInttoTypeList(List<Integer> type_list) {
        List<RelDataType> relTypeList = new ArrayList<>();
        RelDataTypeFactory type_factory = new JavaTypeFactoryImpl();

        for (int i=0; i<type_list.size(); i++) {
            int number = type_list.get(i);
            if (number == 0) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.VARCHAR));
            } else if (number == 1) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.INTEGER));
            } else if (number == 2) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.BOOLEAN));
            } else if (number == 3) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.FLOAT));
            } else if (number == 4) {
                relTypeList.add(type_factory.createSqlType(SqlTypeName.DOUBLE));
            } else {
                System.out.println("Invalid data type");
                throw new RuntimeException("Unsupported type");
            }
        }

        return relTypeList;

    }
    @Override
    public List<Object[]> evaluate(StorageManager storage_manager) {
        String tableName = getTableName();
        List<Object[]> return_records = new ArrayList<>();

        /* Write your code here */
        RexLiteral value=null;
        String column_name ="";
        if(filter!=null & filter.isA(SqlKind.EQUALS)){
//                System.out
            RexNode left = ((RexCall) filter).operands.get(0);
            RexNode right = ((RexCall) filter).operands.get(1);
            int index = ((RexInputRef) left).getIndex();
            column_name = table.getRowType().getFieldNames().get(index);
            value = (RexLiteral) right;
            int id = storage_manager.search(tableName,column_name,value);
            List<Integer> block_ids = new ArrayList<>();
            boolean flag=true;

            while(flag==true) {
                int old_length = block_ids.size();
                if(storage_manager.get_data_block(tableName, id) ==null){
                    break;
                }
                byte[] leaf = storage_manager.get_data_block(tableName, id);
                int num_keys = ((leaf[0] & 0xFF) << 8) | (leaf[1] & 0xFF);
                int offset = 6;
                int id_next_node = ((leaf[offset] & 0xFF) << 8) | (leaf[offset + 1] & 0xFF);
                offset += 2;
                for (int i = 0; i < num_keys; i++) {
                    int b_id = ((leaf[offset] & 0xFF) << 8) | (leaf[offset + 1] & 0xFF);
                    offset += 2;
                    int l_key = ((leaf[offset] & 0xFF) << 8) | (leaf[offset + 1] & 0xFF);
                    offset += 2;
                    byte[] key = Arrays.copyOfRange(leaf, offset, offset + l_key-1);


                    if (compare(key, value)) {
                        block_ids.add(b_id);
                    }
                }
                if(old_length==block_ids.size()){
                    flag=false;
                }
                id = id_next_node;
            }

            for(int ii=0;ii<block_ids.size();ii++){
                List<Object[]> all_records = storage_manager.get_records_from_block(tableName,block_ids.get(ii));

//                        ==========================================================================================
                byte[] schema_Block = storage_manager.get_data_block(tableName, 0);
                int v_start = 0;
                int match = -1;
                int match_type = -1;
                int s_off = 0;
                int c_v = 0;
                int c_f = 0;
                int flag_set = 0;
                List<String> columnNames = new ArrayList<>();
                int num_cols = ( schema_Block[0] & 0xFF) | ((schema_Block[1] & 0xFF) <<8);
                int j = 2;
                Block schema_block = new Block(schema_Block);
                int last_off = schema_block.get_block_capacity()-1;
                List<Integer> typeList = new ArrayList<>();
                for(int i=0;i<num_cols;i++){
                    int col_off = ((schema_Block[j+1] & 0xFF) <<8) | (schema_Block[j] & 0xFF);
                    j+=2;
                    byte[] col = schema_block.get_data(col_off,last_off-col_off);
                    int datatype = col[0]& 0xFF;
                    int len_name = col[1]& 0xFF;
                    byte[] col_name = schema_block.get_data(col_off+2,len_name);
                    columnNames.add(new String(col_name));
                    switch(datatype){
                        case 0:
                            typeList.add(0);
                            break;
                        case 1:
                            typeList.add(1);
                            break;
                        case 2:
                            typeList.add(2);
                            break;
                        case 3:
                            typeList.add(3);
                            break;
                        case 4:
                            typeList.add(4);
                            break;
                    }
                    last_off = col_off;
                }
                for(int i=0;i<columnNames.size();i++){
                    String columnName = columnNames.get(i);
                    if(columnName.equals(column_name)){
                        match = i;
                        flag_set = 1;
                    }
                    int dataType = typeList.get(i);
                    switch(dataType){
                        case 1:
                            c_f ++;
                            if (flag_set == 0) {
                                s_off +=4;
                            }
                            if(columnName.equals(column_name)){
                                match_type = 1;
                            }
                            break;
                        case 3:
                            c_f ++;
                            if (flag_set == 0) {
                                s_off +=4;
                            }
                            if(columnName.equals(column_name)){
                                match_type = 3;
                            }
                            break;
                        case 4:
                            c_f ++;
                            if (flag_set == 0) {
                                s_off +=8;
                            }
                            if(columnName.equals(column_name)){
                                match_type = 4;
                            }
                            break;
                        case 2:
                            c_f ++;
                            if (flag_set == 0) {
                                s_off +=1;
                            }
                            if(columnName.equals(column_name)){
                                match_type = 2;
                            }
                            break;
                        case 0:
                            c_v ++;
                            if(columnName.equals(column_name)){
                                match_type = 0;
                            }
                            break;
                    }
                }
//                        ==========================================================================================

                for(int jj=0;jj<all_records.size();jj++){
                    byte[] row = convertToByteArray(all_records.get(jj),convertInttoTypeList(typeList));
                    if(match<c_f){
                        int idx=4*c_v+s_off;
                        byte[] arg=null;
                        if(match_type==1){
                            arg = Arrays.copyOfRange(row,idx,idx+4);
                        }
                        else if(match_type==2){
                            arg = Arrays.copyOfRange(row,idx,idx+1);
                        }
                        if(match_type==3){
                            arg = Arrays.copyOfRange(row,idx,idx+4);
                        }
                        if(match_type==4){
                            arg = Arrays.copyOfRange(row,idx,idx+8);
                        }

                        if(compare(arg, value)){
                            return_records.add(all_records.get(jj));
                        }
                    }
                    else{
                        match = match - c_f;
                        int idx = 4*match;
                        int key = 0;
                        int offi = (row[idx] & 0xFF) | ((row[idx+1] & 0xFF) << 8);
                        int leni = (row[idx] & 0xFF) | ((row[idx+1] & 0xFF) << 8);
                        byte[] arg = Arrays.copyOfRange(row,offi,offi+leni);

                        if(compare(arg, value)){
                            return_records.add(all_records.get(jj));
                        }
                    }
                }
            }



        }
        else if(filter!=null & filter.isA(SqlKind.GREATER_THAN)){
            RexNode left = ((RexCall) filter).operands.get(0);
            RexNode right = ((RexCall) filter).operands.get(1);
            int index = ((RexInputRef) left).getIndex();
            column_name = table.getRowType().getFieldNames().get(index);
            value = (RexLiteral) right;
            int id = storage_manager.search(tableName,column_name,value);
            List<Integer> block_ids = new ArrayList<>();

        }
        else if(filter!=null & filter.isA(SqlKind.GREATER_THAN_OR_EQUAL)){
            RexNode left = ((RexCall) filter).operands.get(0);
            RexNode right = ((RexCall) filter).operands.get(1);
            int index = ((RexInputRef) left).getIndex();
            column_name = table.getRowType().getFieldNames().get(index);
            value = (RexLiteral) right;
        }
        else if(filter!=null & filter.isA(SqlKind.LESS_THAN)){
            RexNode left = ((RexCall) filter).operands.get(0);
            RexNode right = ((RexCall) filter).operands.get(1);
            int index = ((RexInputRef) left).getIndex();
            column_name = table.getRowType().getFieldNames().get(index);
            value = (RexLiteral) right;
        }
        else if(filter!=null & filter.isA(SqlKind.LESS_THAN_OR_EQUAL)){
            RexNode left = ((RexCall) filter).operands.get(0);
            RexNode right = ((RexCall) filter).operands.get(1);
            int index = ((RexInputRef) left).getIndex();
            column_name = table.getRowType().getFieldNames().get(index);
            value = (RexLiteral) right;
        }



        return return_records;
//            return null;
    }
}