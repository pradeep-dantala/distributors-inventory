package com.penton.inventory.mapper.reducer;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/**
 * Main Entry point to run the map-reduce job.
 * 
 * @author Pradeep Dantala
 * 
 */
public class InventoryData {

    public static void main(final String[] args) throws IOException {

        final JobConf conf = new JobConf(InventoryData.class);

        conf.setJobName("Inventory Data");

        conf.setMapperClass(InventoryMapper.class);
        conf.setReducerClass(InventoryReducer.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);

    }

}
