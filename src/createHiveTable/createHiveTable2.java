package createHiveTable;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;

public class createHiveTable2 {

    public void externalTable(Connection con, String tableName, String newTableName, String[] selectedFamilies, HashMap<String, String> map)
            throws ClassNotFoundException, SQLException, IOException {

        StringBuilder builder1 = new StringBuilder();
        for (String s : selectedFamilies) {
            builder1.append(",");
            builder1.append(s);
        }
        String hiveColumns = builder1.toString();

        //Define Hive columns and their names.
        StringBuilder builder2 = new StringBuilder();
        for (String c : selectedFamilies) {
            String[] tmp = c.split(":");
            String columnName = tmp[0] + "_" + tmp[1];
            builder2.append(",");
            builder2.append(columnName);
            builder2.append(" ");
            builder2.append(map.get(c).toString());
        }
        String hiveHeader = builder2.toString();

        //Execute the Hive command to create external table mapping to HBase table.
        Statement stmt = con.createStatement();
        stmt.execute("drop table " + newTableName);
        stmt.execute("create external table " + newTableName + "(row_key string" + hiveHeader + ") "
                + "stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' "
                + "with serdeproperties (\"hbase.columns.mapping\" = \":key" + hiveColumns + "\") "
                + "tblproperties (\"hbase.table.name\"=\"" + tableName + "\")");

    }

}
