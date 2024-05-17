package manager;

import index.bplusTree.BPlusTreeIndexFile;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import storage.DB;
import storage.File;
import storage.Block;
import Utils.CsvRowConverter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.util.Sources;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.util.Iterator;

public class StorageManager {

    private HashMap<String, Integer> file_to_fileid;
    private DB db;

    enum ColumnType {
        VARCHAR, INTEGER, BOOLEAN, FLOAT, DOUBLE
    };

    public StorageManager() {
        file_to_fileid = new HashMap<>();
        db = new DB();
    }

    // loads CSV files into DB362
    public void loadFile(String csvFile, List<RelDataType> typeList) {

        System.out.println("Loading file: " + csvFile);

        String table_name = csvFile;

        if(csvFile.endsWith(".csv")) {
            table_name = table_name.substring(0, table_name.length() - 4);
        }

        // check if file already exists
        assert(file_to_fileid.get(table_name) == null);

        File f = new File();
        try{
            csvFile = getFsPath() + "/" + csvFile;
            BufferedReader br = new BufferedReader(new FileReader(csvFile));
            String line = "";
            int lineNum = 0;

            while ((line = br.readLine()) != null) {

                // csv header line
                if(lineNum == 0){

                    String[] columnNames = CsvRowConverter.parseLine(line);
                    List<String> columnNamesList = new ArrayList<>();

                    for(String columnName : columnNames) {
                        // if columnName contains ":", then take part before ":"
                        String c = columnName;
                        if(c.contains(":")) {
                            c = c.split(":")[0];
                        }
                        columnNamesList.add(c);
                    }

                    Block schemaBlock = createSchemaBlock(columnNamesList, typeList);
                    f.add_block(schemaBlock);
                    lineNum++;
                    continue;
                }

                String[] parsedLine = CsvRowConverter.parseLine(line);
                Object[] row = new Object[parsedLine.length];

                for(int i = 0; i < parsedLine.length; i++) {
                    row[i] = CsvRowConverter.convert(typeList.get(i), parsedLine[i]);
                }

                // convert row to byte array
                byte[] record = convertToByteArray(row, typeList);

                boolean added = f.add_record_to_last_block(record);
                if(!added) {
                    f.add_record_to_new_block(record);
                }
                lineNum++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }

        System.out.println("Done writing file\n");
        int counter = db.addFile(f);
        file_to_fileid.put(table_name, counter);
        return;
    }

    // converts a row to byte array to write to relational file
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

        if(bitmap_idx != 0) {
            idx++;
        }

        // write variable length fields
        for(int i = 0; i < variable_length_Bytes.size(); i++, idx++) {
            result[idx] = variable_length_Bytes.get(i);
        }

        return result;
    }

    // helper function for loadFile

    private Object[] convertToObjectArray(byte[] row, String table_name) {
        Block row_b = new Block(row);
        List<Object> object_list = new ArrayList<>();

        int file_id = file_to_fileid.get(table_name);
        byte[] schema = db.get_data(file_id,0);
        int num_cols = ( schema[0] & 0xFF) | ((schema[1] & 0xFF) <<8);

        int c_f =0;
        int c_v=0;
        int null_off=0;
        int j = 2;
        Block schema_block = new Block(schema);
        int last_off = schema_block.get_block_capacity()-1;

        List<Integer> typeList = new ArrayList<>();
        for(int i=0;i<num_cols;i++){
            int col_off = ((schema[j+1] & 0xFF) <<8) | (schema[j] & 0xFF);

            j+=2;
            byte[] col = schema_block.get_data(col_off,last_off-col_off);

            int datatype = col[0]& 0xFF;

            switch(datatype){
                case 0:
                    c_v++;
                    typeList.add(0);
                    break;
                case 1:
                    c_f++;
                    null_off+=4;
                    typeList.add(1);
                    break;
                case 2:
                    c_f++;
                    null_off+=1;
                    typeList.add(2);
                    break;
                case 3:
                    c_f++;
                    null_off+=4;
                    typeList.add(3);
                    break;
                case 4:
                    c_f++;
                    null_off+=8;
                    typeList.add(4);
                    break;
            }
            last_off = col_off;
        }
        int idx = 4*c_v;
        int null_o = idx + null_off;
        int len = (c_f+c_v+7)/8;

        byte[] null_bitmap = schema_block.get_data(null_o,len);

        for(int i=0;i< c_f;i++) {
            int type = typeList.get(i);

            int byteIndex = i / 8;
            int bitIndex = i % 8;
            byte bitMask = (byte) (1 << bitIndex);

            // Read the ith bit from the byte array
            boolean bitValue = (null_bitmap[byteIndex] & bitMask) != 0;

            switch (type) {
                case 1:
                    if (bitValue) {
                        object_list.add(null);
                    } else {
                        int val = 0;
                        val = ((row[idx+3] & 0xFF) << 24) | ((row[idx + 2] & 0xFF) << 16) | ((row[idx + 1] & 0xFF) << 8) | (row[idx ] & 0xFF);

                        object_list.add(val);
                    }
                    idx += 4;
                    break;
                case 2:
                    if (bitValue) {
                        object_list.add(null);
                    } else {
                        boolean val = row[idx] != 0;
                        object_list.add(val);
                    }
                    idx++;
                    break;
                case 3:
                    if (bitValue) {
                        object_list.add(null);
                    } else {
                        int intBits = 0;
                        intBits = ((row[idx+3] & 0xFF) << 24) | ((row[idx + 2] & 0xFF) << 16) | ((row[idx + 1] & 0xFF) << 8) | (row[idx] & 0xFF);
                        float val = Float.intBitsToFloat(intBits);
                        object_list.add(val);
                    }
                    idx += 4;
                    break;
                case 4:
                    if (bitValue) {
                        object_list.add(null);
                    } else {
                        long longBits;
                        longBits = ((row[idx + 7] & 0xFFL) << 56) | ((row[idx + 6] & 0xFFL) << 48) | ((row[idx + 5] & 0xFFL) << 40) |
                                ((row[idx + 4] & 0xFFL) << 32) | ((row[idx + 3] & 0xFFL) << 24) | ((row[idx + 2] & 0xFFL) << 16) |
                                ((row[idx + 1] & 0xFFL) << 8) | (row[idx] & 0xFFL);
                        double val = Double.longBitsToDouble(longBits);
                        object_list.add(val);
                    }
                    idx += 8;
                    break;
            }
        }
        idx =0;

        for(int k=0;k<c_v;k++){
//            int type = typeList.get(k+c_f);
            int byteIndex = (k+c_f) / 8;
            int bitIndex = (k+c_f) % 8;
            byte bitMask = (byte) (1 << bitIndex);
            boolean bitValue = (null_bitmap[byteIndex] & bitMask) != 0;
            if(bitValue){
                object_list.add(null);
                idx += 4;
            }
            else {
                int v_off = (row[idx] & 0xFF) | ((row[idx+1] & 0xFF )<< 8 );
                idx += 2;
                int v_len = (row[idx] & 0xFF) | ((row[idx+1] & 0xFF) << 8);
                idx += 2;

                byte[] v_string;
                v_string = row_b.get_data(v_off,v_len);

                object_list.add(new String(v_string));
            }

        }
        return object_list.toArray();

    }
    private String getFsPath() throws IOException, ParseException {

        String modelPath = Sources.of(CsvRowConverter.class.getResource("/" + "model.json")).file().getAbsolutePath();
        JSONObject json = (JSONObject) new JSONParser().parse(new FileReader(modelPath));
        JSONArray schemas = (JSONArray) json.get("schemas");

        Iterator itr = schemas.iterator();

        while (itr.hasNext()) {
            JSONObject next = (JSONObject) itr.next();
            if (next.get("name").equals("FILM_DB")) {
                JSONObject operand = (JSONObject) next.get("operand");
                String directory = operand.get("directory").toString();
                return Sources.of(CsvRowConverter.class.getResource("/" + directory)).file().getAbsolutePath();
            }
        }
        return null;
    }

    // write schema block for a relational file
    private Block createSchemaBlock(List<String> columnNames, List<RelDataType> typeList) {

        Block schema = new Block();

        // write number of columns
        byte[] num_columns = new byte[2];
        num_columns[0] = (byte) (columnNames.size() & 0xFF);
        num_columns[1] = (byte) ((columnNames.size() >> 8) & 0xFF);

        schema.write_data(0, num_columns);

        int idx = 0, curr_offset = schema.get_block_capacity();
        for(int i = 0 ; i < columnNames.size() ; i ++){
            // if column type is fixed, then write it
            if(!typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF);
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        // write variable length fields
        for(int i = 0; i < columnNames.size(); i++) {
            if(typeList.get(i).getSqlTypeName().getName().equals("VARCHAR")) {
                
                // write offset
                curr_offset = curr_offset - (columnNames.get(i).length() + 2);
                byte[] offset = new byte[2];
                offset[0] = (byte) (curr_offset & 0xFF);
                offset[1] = (byte) ((curr_offset >> 8) & 0xFF); 
                // IMPORTANT: Take care of endianness
                schema.write_data(2 + 2 * idx, offset);
                
                // convert column name to bytes
                byte[] column_name_type = new byte[columnNames.get(i).length() + 2];
                // first byte will tell datatype, 2nd byte will tell length of column name
                // Thus, assert that column name length is less than 256
                assert(columnNames.get(i).length() < 256);

                column_name_type[0] = (byte) (ColumnType.valueOf(typeList.get(i).getSqlTypeName().getName()).ordinal() & 0xFF);
                column_name_type[1] = (byte) (columnNames.get(i).length() & 0xFF);
                for(int j = 0; j < columnNames.get(i).length(); j++) {
                    column_name_type[2 + j] = (byte) columnNames.get(i).charAt(j);
                }

                schema.write_data(curr_offset, column_name_type);
                idx++;
            }
        }

        return schema;
    }

    // should only read one block at a time
    public byte[] get_data_block(String table_name, int block_id){
        int file_id = file_to_fileid.get(table_name);
        return db.get_data(file_id, block_id);
    }

    public boolean check_file_exists(String table_name) {
        return file_to_fileid.get(table_name) != null;
    }

    public boolean check_index_exists(String table_name, String column_name) {
        String index_file_name = table_name + "_" + column_name + "_index";
        return file_to_fileid.get(index_file_name) != null;
    }

    // the order of returned columns should be same as the order in schema
    // i.e., first all fixed length columns, then all variable length columns
    public List<Object[]> get_records_from_block(String table_name, int block_id){
        /* Write your code here */
        // return null if file does not exist, or block_id is invalid
        // return list of records otherwise
        if (!check_file_exists(table_name) || block_id < 0) {
            return null;
        }
        List<Object[]> records = new ArrayList<>();
        byte[] dataBlock = get_data_block(table_name, block_id);
        Block b = new Block(dataBlock);
        int offset = 0;
        int num_rows = (dataBlock[1] & 0xFF) | ((dataBlock[0] & 0xFF) << 8);
        offset += 2;
        int last_offset = b.get_block_capacity() - 1;

        for (int i = 0; i < num_rows; i++) {
            int r_off = 0;
            r_off = (dataBlock[offset+1] & 0xFF) | ((dataBlock[offset] & 0xFF) << 8);

            offset+=2;
            byte[] record = b.get_data(r_off, last_offset-r_off);
            Object[] r;
            r = convertToObjectArray(record,table_name);


//            Object[] r = Arrays.copyOf(record, record.length);
            records.add(r);
            last_offset = r_off;
        }

        return records;
    }
    private int get_block_count(int file_id) {
        int i = 0;
        while (db.get_data(file_id, i) != null) {
            i++;
        }
        return i;
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
    public boolean create_index(String table_name, String column_name, int order) {
        /* Write your code here */
        if (!check_file_exists(table_name)) {
            return false;
        }

        if (check_index_exists(table_name, column_name)) {
            return false;
        }
        int file_id = file_to_fileid.get(table_name);
        int block_count = get_block_count(file_id);
        byte[] schema_Block = get_data_block(table_name, 0);
        int v_start = 0;
        int match = -1;
        int match_type = -1;
        int s_off = 0;
        int c_v = 0;
        int c_f = 0;
        int flag = 0;
        List<String> columnNames = new ArrayList<>();
        byte[] schema = db.get_data(file_id,0);
        int num_cols = ( schema[0] & 0xFF) | ((schema[1] & 0xFF) <<8);
        int j = 2;
        Block schema_block = new Block(schema);
        int last_off = schema_block.get_block_capacity()-1;
        List<Integer> typeList = new ArrayList<>();
        for(int i=0;i<num_cols;i++){
            int col_off = ((schema[j+1] & 0xFF) <<8) | (schema[j] & 0xFF);

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
                flag = 1;
                // break;
            }
            int dataType = typeList.get(i);
            switch(dataType){
                case 1:
                    c_f ++;
                    if (flag == 0) {
                        s_off +=4;
                    }

                    if(columnName.equals(column_name)){
                        match_type = 1;
                    }
                    break;
                case 3:
                    c_f ++;
                    if (flag == 0) {
                        s_off +=4;
                    }
                    if(columnName.equals(column_name)){
                        match_type = 3;
                    }
                    break;
                case 4:
                    c_f ++;
                    if (flag == 0) {
                        s_off +=8;
                    }
                    if(columnName.equals(column_name)){
                        match_type = 4;
                    }
                    break;
                case 2:
                    c_f ++;
                    if (flag == 0) {
                        s_off +=1;
                    }
                    if(columnName.equals(column_name)){
                        match_type = 2;
                    }
                    break;
                case 0:
                    c_v ++;
                    // s_off +=2;
                    if(columnName.equals(column_name)){
                        match_type = 0;
                    }
                    break;
            }
        }

        if (match_type == 0) {
            BPlusTreeIndexFile<String> indexFile = new BPlusTreeIndexFile<>(order, String.class);

            // Iterate through each block of the table
            for (int block_id = 1; block_id < block_count; block_id++) {
                // Get the data block
                byte[] dataBlock = get_data_block(table_name, block_id);

                // Parse the data block and extract the records
                List<Object[]> records = get_records_from_block(table_name, block_id);

                // Iterate through each record
                for (Object[] record : records) {
                    // Get the value of the column for indexing
                    // Object columnValue = record[getColumnIndex(table_name, column_name)];
                    byte[] record_bytes = convertToByteArray(record, convertInttoTypeList(typeList));
                    Block record_block = new Block(record_bytes);

                    match = match - c_f;
                    int offset = 4*match;
                    int key = 0;
                    byte[] off = record_block.get_data(offset,2);

                    int offi = (off[0] & 0xFF) | ((off[1] & 0xFF) << 8);
                    byte[] len = record_block.get_data(offset+2,2);
                    int leni = (len[0] & 0xFF) | ((len[1] & 0xFF) << 8);
                    byte[] b = record_block.get_data(offi, leni);
                    indexFile.insert(new String(b), block_id);

                    // TODO: Insert the record into the B+ tree index

                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, counter);
        } else if (match_type == 1) {
//            System.out.println("Hello Integer\n");
            BPlusTreeIndexFile<Integer> indexFile = new BPlusTreeIndexFile<>(order, Integer.class);
//            System.out.println("Loop Start\n");
            // Iterate through each block of the table
            for (int block_id = 1; block_id < block_count; block_id++) {
//                System.out.println("Loop Entered\n");
                // Get the data block
                byte[] dataBlock = get_data_block(table_name, block_id);
//                System.out.println("Step 1\n");
                // Parse the data block and extract the records
                List<Object[]> records = get_records_from_block(table_name, block_id);
//                System.out.println("Step 2\n");
                // Iterate through each record
                for (Object[] record : records) {
//                    System.out.println("Second Loop Enter\n");
                    // Get the value of the column for indexing
                    // Object columnValue = record[getColumnIndex(table_name, column_name)];
                    byte[] record_bytes = convertToByteArray(record, convertInttoTypeList(typeList));
//                    System.out.println("Step 3\n");
                    Block record_block = new Block(record_bytes);
//                    System.out.println("Step 4\n");

                    int offset = 4*c_v + s_off;
                    int key = 0;

                    byte[] b = record_block.get_data(offset,4);
//                    System.out.println("Step 5\n");
                    key = (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16) | ((b[3] & 0xFF) << 24);
//                        indexFile.insert(key, block_id);
//                    System.out.println("Step 6\n");

                    indexFile.insert(key, block_id);
//                    System.out.println("Step 7\n");

                    // TODO: Insert the record into the B+ tree index

                }
//                System.out.println("Second loop exit\n");
            }

//            System.out.println("First loop exit\n");

            // TODO: Save the B+ tree index to a file
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, counter);

        } else if (match_type == 2) {
            BPlusTreeIndexFile<Boolean> indexFile = new BPlusTreeIndexFile<>(order, Boolean.class);

            // Iterate through each block of the table
            for (int block_id = 1; block_id < block_count; block_id++) {
                // Get the data block
                byte[] dataBlock = get_data_block(table_name, block_id);

                // Parse the data block and extract the records
                List<Object[]> records = get_records_from_block(table_name, block_id);

                // Iterate through each record
                for (Object[] record : records) {
                    // Get the value of the column for indexing
                    // Object columnValue = record[getColumnIndex(table_name, column_name)];
                    byte[] record_bytes = convertToByteArray(record, convertInttoTypeList(typeList));
                    Block record_block = new Block(record_bytes);

                    int offset = 4*c_v + s_off;
                    byte[] b = record_block.get_data(offset,1);
                    boolean key = b[0] != 0;
                    indexFile.insert(key, block_id);

                }

                // TODO: Insert the record into the B+ tree index

            }

            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, counter);

        } else if (match_type == 3) {
            BPlusTreeIndexFile<Float> indexFile = new BPlusTreeIndexFile<>(order, Float.class);

            // Iterate through each block of the table
            for (int block_id = 1; block_id < block_count; block_id++) {
                // Get the data block
//                byte[] dataBlock = get_data_block(table_name, block_id);

                // Parse the data block and extract the records
                List<Object[]> records = get_records_from_block(table_name, block_id);

                // Iterate through each record
                for (Object[] record : records) {
                    // Get the value of the column for indexing
                    // Object columnValue = record[getColumnIndex(table_name, column_name)];
                    byte[] record_bytes = convertToByteArray(record, convertInttoTypeList(typeList));
                    Block record_block = new Block(record_bytes);

                    int offset = 4*c_v + s_off;
                    int key = 0;

                    byte[] b = record_block.get_data(offset,4);
                    key = (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16) | ((b[3] & 0xFF) << 24);
                    float key_val = Float.intBitsToFloat(key);
                    indexFile.insert(key_val, block_id);
                }
            }
            String index_file_name = table_name + "_" + column_name + "_index";
            int counter = db.addFile(indexFile);
            file_to_fileid.put(index_file_name, counter);

        } else if (match_type == 4) {
            BPlusTreeIndexFile<Double> indexFile = new BPlusTreeIndexFile<>(order, Double.class);

            // Iterate through each block of the table
            for (int block_id = 1; block_id < block_count; block_id++) {
                // Get the data block
//                byte[] dataBlock = get_data_block(table_name, block_id);

                // Parse the data block and extract the records
                List<Object[]> records = get_records_from_block(table_name, block_id);

                // Iterate through each record
                for (Object[] record : records) {
                    byte[] record_bytes = convertToByteArray(record, convertInttoTypeList(typeList));
                    Block record_block = new Block(record_bytes);
                    int offset = 4*c_v + s_off;
                    int key = 0;
                    byte[] b = record_block.get_data(offset,8);
                    key = (b[0] & 0xFF) | ((b[1] & 0xFF) << 8) | ((b[2] & 0xFF) << 16) | ((b[3] & 0xFF) << 24) | ((b[4] & 0xFF) << 32) | ((b[5] & 0xFF) << 40) | ((b[6] & 0xFF) << 48) | ((b[7] & 0xFF) << 56);
                    double val = Double.longBitsToDouble(key);
                    indexFile.insert(val, block_id);
                }
                String index_file_name = table_name + "_" + column_name + "_index";
                int counter = db.addFile(indexFile);
                file_to_fileid.put(index_file_name, counter);
            }
        }

        return true;
    }

    // returns the block_id of the leaf node where the key is present
    public int search(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        System.out.println("Search started\n");
        int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
        System.out.println("File id is "+file_id);
//        byte[] key = value.getValue2();
        System.out.println("\nByte array converted\n");
        return db.search_index(file_id, value);
//        return -1;
    }

    public boolean delete(String table_name, String column_name, RexLiteral value) {
        /* Write your code here */
        // Hint: You need to delete from both - the file and the index
        return false;
    }

    // will be used for evaluation - DO NOT modify
    public DB getDb() {
        return db;
    }

    public <T> ArrayList<T> return_bfs_index(String table_name, String column_name) {
        if(check_index_exists(table_name, column_name)) {
            int file_id = file_to_fileid.get(table_name + "_" + column_name + "_index");
            return db.return_bfs_index(file_id);
        } else {
            System.out.println("Index does not exist");
        }
        return null;
    }

}