package createHiveTable;

import columnType.columnType;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import subColumns.subColumns;

public class createHiveTable {

    public void externalTable(Connection con, String filePath, String tableName, String[] selectedTableFamilies)
            throws ClassNotFoundException, SQLException, IOException {

        columnFamily.family columns = new columnFamily.family();
        String[] familyName = columns.columnfamily(filePath);

        columnType subColumnsType = new columnType();
        HashMap<String, String> columnType = subColumnsType.typeOf(filePath);

        subColumns subColumnsAll = new subColumns();
        String[] subColumns = subColumnsAll.columns(filePath);

        //Each hive column is mapping to the corresponding HBase column.
        StringBuilder builder1 = new StringBuilder();
        int count = 0;
        for (int i = 2; i < familyName.length; i++) {
            if (familyName[i].equalsIgnoreCase("INFO") && Arrays.asList(selectedTableFamilies).contains("INFO")) {
                for (String a : subColumns) {
                    if (!a.contains("format")) {
                        builder1.append(",INFO:");
                        builder1.append(a);
                    }
                }
            } else if (!familyName[i].equalsIgnoreCase("FORMAT") && i > 7 && Arrays.asList(selectedTableFamilies).contains(familyName[i])) {
                for (String b : subColumns) {
                    if (b.contains("format")) {
                        builder1.append(",");
                        builder1.append(familyName[i]);
                        builder1.append(":");
                        builder1.append(b.substring(6));
                    }
                }
                count++;
            } else if (!familyName[i].equalsIgnoreCase("FORMAT") && Arrays.asList(selectedTableFamilies).contains(familyName[i])) {
                builder1.append(",");
                builder1.append(familyName[i]);
                builder1.append(":");
                builder1.append(familyName[i]);
            }
        }
        if (count == 0) {
            for (String b : subColumns) {
                if (b.contains("format")) {
                    builder1.append(",");
                    builder1.append("SAMPLE");
                    builder1.append(":");
                    builder1.append(b.substring(6));
                }
            }
        }
        String hiveColumns = builder1.toString();

        //Define Hive columns and their names.
        StringBuilder builder2 = new StringBuilder();
        for (int i = 2; i < familyName.length; i++) {
            if (familyName[i].equalsIgnoreCase("INFO") && Arrays.asList(selectedTableFamilies).contains("INFO")) {
                for (String c : subColumns) {
                    if (!c.contains("format")) {
                        if (columnType.containsKey(c)) {
                            builder2.append(",");
                            builder2.append(c);
                            builder2.append(" ");
                            builder2.append(columnType.get(c).toString());
                        } else {
                            builder2.append(",");
                            builder2.append(c);
                            builder2.append(" string");
                        }
                    }
                }
            } else if (!familyName[i].equalsIgnoreCase("FORMAT") && i > 7 && Arrays.asList(selectedTableFamilies).contains(familyName[i])) {
                for (String d : subColumns) {
                    if (d.contains("format")) {
                        if (columnType.containsKey(d)) {
                            builder2.append(",");
                            builder2.append(familyName[i]);
                            builder2.append("_");
                            builder2.append(d.substring(6));
                            builder2.append(" ");
                            builder2.append(columnType.get(d).toString());
                        } else {
                            builder2.append(",");
                            builder2.append(familyName[i]);
                            builder2.append("_");
                            builder2.append(d.substring(6));
                            builder2.append(" string");
                        }
                    }
                }
            } else if (familyName[i].equalsIgnoreCase("QUAL") && Arrays.asList(selectedTableFamilies).contains("QUAL")) {
                builder2.append(",");
                builder2.append(familyName[i]);
                builder2.append(" float");
            } else if (!familyName[i].equalsIgnoreCase("FORMAT") && Arrays.asList(selectedTableFamilies).contains(familyName[i])) {
                builder2.append(",");
                builder2.append(familyName[i]);
                builder2.append(" string");
            }
        }
        if (count == 0) {
            for (String d : subColumns) {
                if (d.contains("format")) {
                    if (columnType.containsKey(d)) {
                        builder2.append(",");
                        builder2.append("SAMPLE");
                        builder2.append("_");
                        builder2.append(d.substring(6));
                        builder2.append(" ");
                        builder2.append(columnType.get(d).toString());
                    } else {
                        builder2.append(",");
                        builder2.append("SAMPLE");
                        builder2.append("_");
                        builder2.append(d.substring(6));
                        builder2.append(" string");
                    }
                }
            }
        }
        String hiveHeader = builder2.toString();

        //Execute the Hive command to create external table mapping to HBase table.
        Statement stmt = con.createStatement();
        stmt.execute("drop table " + tableName);
        stmt.execute("create external table " + tableName + "(row_key string" + hiveHeader + ") "
                + "stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' "
                + "with serdeproperties (\"hbase.columns.mapping\" = \":key" + hiveColumns + "\") "
                + "tblproperties (\"hbase.table.name\"=\"" + tableName + "\")");

    }

}
