package com.penton.inventory.mapper.reducer;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper class gets the string as a line and filter out invalid keys and map keys to respective values. Key : First tokenized word of a given line.
 * Value: Remaining tokenized words of a given line.
 * 
 * @author Pradeep Dantala
 * 
 */
public class InventoryMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

        final Text outputKey = new Text();
        final Text outputValue = new Text();

        final String line = value.toString();

        final StringTokenizer fieldValues = new StringTokenizer(line, ",");
        int count = 0;
        String output = "";
        while (fieldValues.hasMoreTokens()) {
            if (count == 0) {
                final String fieldValue = fieldValues.nextToken();
                if (fieldValue.length() >= 3 && fieldValue.matches("^.*[A-Za-z0-9]+.*$") && fieldValue.contains("-")) {
                    outputKey.set(fieldValue);
                } else {
                    break;
                }
            } else {
                output += fieldValues.nextToken() + ",";
            }
            count++;
        }

        if (count != 0) {
            outputValue.set(output.substring(0, output.length() - 1));
            collector.collect(outputKey, outputValue);
        }

    }

}
