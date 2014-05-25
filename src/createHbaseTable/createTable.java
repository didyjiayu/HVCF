package createHbaseTable;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

public class createTable {

    public void table(String tableName, String[] columnFamily, String[] selectedFamilies) throws MasterNotRunningException, ZooKeeperConnectionException, IOException {
        //Create HBase Admin.
        Configuration hc = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(hc);
        //Use table name and columns to create table.
        HTableDescriptor ht = new HTableDescriptor(TableName.valueOf(tableName));
        if (selectedFamilies==null) {
            for (int i=2; i< columnFamily.length; i++) {
                if (!columnFamily[i].equalsIgnoreCase("FORMAT")) {
                    ht.addFamily(new HColumnDescriptor(columnFamily[i]));
                }
            }
        } else {
            for (String a: selectedFamilies) {
                ht.addFamily(new HColumnDescriptor(a));
            }
        }
        
        hba.createTable(ht);

    }

}
