/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package importToNewTable;

import columnFamily.family;
import createHbaseTable.createTable;
import java.io.File;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author yujia1986
 */
public class createDataJob {
    public boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }

    public void importData(String filePath, String[] selectedFamilies, String[] keys) throws Exception {

        //Use family class get columns names.
        family familyName = new family();
        String[] column = familyName.columnfamily(filePath);
        //Get input file name to be used as table name.
        File f = new File(filePath);
        String fname = f.getName();
        String vcfName = FilenameUtils.removeExtension(fname);
        
        createTable ht = new createTable();
        ht.table(vcfName, column, selectedFamilies);

        //Mapreduce job.
        Configuration conf = new Configuration();
        conf.setStrings("column", column);
        conf.setStrings("keys", keys);
        conf.setStrings("selectedFamilies", selectedFamilies);
        conf.set("tableName", vcfName);
        Job job = Job.getInstance(conf, "VCF");
        job.setJarByClass(importToNewTable.createDataJob.class);
        job.setMapperClass(importToNewTable.createDataMapper.class);

        job.setReducerClass(org.apache.hadoop.hbase.mapreduce.PutSortReducer.class);

        // TODO: specify output types
        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);
        job.setOutputFormatClass(HFileOutputFormat.class);

        // TODO: specify input and output DIRECTORIES (not files)
        FileInputFormat.setInputPaths(job, new Path(filePath));
        File temp = new File("/tmp/HVCF/output");
        this.deleteDir(temp);
        FileOutputFormat.setOutputPath(job, new Path("/tmp/HVCF/output"));

        Configuration hbconf = HBaseConfiguration.create();
        HTable table = new HTable(hbconf, vcfName);
        HFileOutputFormat.configureIncrementalLoad(job, table);

        if (!job.waitForCompletion(true)) {
            return;
        }

        //Bulkload Hfiles into HBase table.
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbconf);
        loader.doBulkLoad(new Path("/tmp/HVCF/output"), table);

    }
    
}
