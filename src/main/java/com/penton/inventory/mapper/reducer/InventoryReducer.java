package com.penton.inventory.mapper.reducer;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer class groups the values of the sorted keys that came in and find the max quantity field value in iterator quantity field and map that value
 * to the key.
 * 
 * @author Pradeep Dantala
 * 
 */
public class InventoryReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

    final Text outputValue = new Text();
    final Text outputKey = new Text();

    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

        int max = 0;
        while (values.hasNext()) {
            final String value = values.next().toString();
            final StringTokenizer st = new StringTokenizer(value, ",");
            if (st.hasMoreTokens()) {
                final int quantity = Integer.parseInt(st.nextToken());
                if (quantity > max) {
                    max = quantity;
                    outputValue.set(value);
                }
            }
        }

        outputKey.set(key);

    }

}
