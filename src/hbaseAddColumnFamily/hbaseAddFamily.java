/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hbaseAddColumnFamily;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 *
 * @author yujia1986
 */
public class hbaseAddFamily {

    public void addFamily(String tableName, String[] selectedTableFamilies, String newFamily) throws IOException, InterruptedException {
        
        Configuration hbconf = HBaseConfiguration.create();
        HBaseAdmin hba = new HBaseAdmin(hbconf);
        if (!Arrays.asList(selectedTableFamilies).contains(newFamily)) {
            hba.addColumn(tableName, new HColumnDescriptor(newFamily));
        }
        hba.flush(tableName);
        hba.close();
    }
}
