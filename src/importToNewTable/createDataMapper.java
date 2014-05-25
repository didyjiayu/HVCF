package importToNewTable;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class createDataMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //Get columns names from mapreduce job class via configuration.
        Configuration conf = context.getConfiguration();
        String[] family = conf.getStrings("column");
        String[] keys = conf.getStrings("keys");
        String[] selectedFamilies = conf.getStrings("selectedFamilies");
        String tableName = conf.get("tableName");
        
        String path = System.getProperty("user.dir");
        path = path+"/HVCF/HBase/"+tableName+"/columns.txt";
        String tableColumns = getColumns(path);
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(path, true)));

        //Get line contents.
        String line = value.toString();
        String[] values = null;
        if (!line.contains("#")) {
            values = line.split("\t", -1);
            for (int i = 0; i < values.length; i++) {
                values[i] = values[i].trim();
            }

            ArrayList<String> keyList = new ArrayList<>();
            for (int i = 0; i < keys.length; i++) {
                int position = positionOf(keys[i], selectedFamilies);
                keyList.add(selectedFamilies[position]);
            }
            StringBuilder builder = new StringBuilder();
            for (int i=0; i<values.length; i++) {
                if (keyList.contains(family[i])) {
                    builder.append(values[i]);
                    builder.append("-");
                }
            }
            String rowkey = builder.toString();
            rowkey = rowkey.substring(0, rowkey.length()-1);
            ImmutableBytesWritable bRowKey = new ImmutableBytesWritable(rowkey.getBytes());
            Put p = new Put(rowkey.getBytes());

            //Loop each column.
            String[] formatColumn = null;
            for (int i = 0; i < values.length; i++) {
                if (family[i].equalsIgnoreCase("INFO") && Arrays.asList(selectedFamilies).contains("INFO")) {
                    //Find column family INFO.
                    String infoColumn[] = values[i].split(";", -1);
                    for (int j = 0; j < infoColumn.length; j++) {
                        String infoSingleColumn[] = infoColumn[j].trim().split("=", -1);
                        if (infoSingleColumn.length > 1) {
                            for (int k = 0; k < infoSingleColumn.length; k++) {
                                infoSingleColumn[k] = infoSingleColumn[k].trim();
                            }
                            p.add("INFO".getBytes(), infoSingleColumn[0].getBytes(), System.currentTimeMillis(), infoSingleColumn[1].getBytes());
                            if (!tableColumns.contains("INFO:"+infoSingleColumn[0])) 
                            out.print("INFO:"+infoSingleColumn[0]+",");
                        } else {
                            p.add("INFO".getBytes(), infoSingleColumn[0].getBytes(), System.currentTimeMillis(), "True".getBytes());
                            if (!tableColumns.contains("INFO:"+infoSingleColumn[0])) 
                            out.print("INFO:"+infoSingleColumn[0]+",");
                        }
                    }
                } else if (family[i].equalsIgnoreCase("FORMAT")) {
                    //Find each sample as a column family.
                    formatColumn = values[i].split(":", -1);
                    for (int l = 0; l < formatColumn.length; l++) {
                        formatColumn[l] = formatColumn[l].trim();
                    }
                } else if (!family[i].equalsIgnoreCase("FORMAT") && i > 7 && Arrays.asList(selectedFamilies).contains(family[i])) {
                    String[] sampleColumn = values[i].split(":", -1);
                    for (int m = 0; m < sampleColumn.length; m++) {
                        p.add(family[i].getBytes(), formatColumn[m].getBytes(), System.currentTimeMillis(), sampleColumn[m].trim().getBytes());
                        if (!tableColumns.contains(family[i]+":"+formatColumn[m]))
                        out.print(family[i]+":"+formatColumn[m]+",");
                    }
                } else if (Arrays.asList(selectedFamilies).contains(family[i])) {
                    //Fixed columns.
                    p.add(family[i].getBytes(), family[i].getBytes(), System.currentTimeMillis(), values[i].getBytes());
                    if (!tableColumns.contains(family[i]+":"+family[i]))
                    out.print(family[i]+":"+family[i]+",");
                }
            }
            context.write(bRowKey, p);
            out.close();
        }
    }
    public int positionOf(String target, String[] pool) {
        int i = 0;
        for (String a : pool) {
            if (target.equals(a)) {
                break;
            }
            i++;
        }
        return i;
    }
    
    public String getColumns(String path) throws FileNotFoundException, IOException {
        FileInputStream input = new FileInputStream(path);
        DataInputStream data = new DataInputStream(input);
        InputStreamReader line = new InputStreamReader(data);
        BufferedReader br = new BufferedReader(line);
        String fLine;
        String columns = null;
        while ((fLine = br.readLine()) != null) {
            columns = fLine;
        }
        return columns;
    }
}
