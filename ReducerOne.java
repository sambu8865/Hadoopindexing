package hadoopprjt1;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public  class ReducerOne extends MapReduceBase implements
Reducer<Text, Text, Text, NullWritable> {
@Override
public void reduce(Text key, Iterator<Text> values,
	OutputCollector<Text, NullWritable> output, Reporter reporter)
	throws IOException {

StringBuilder str=new StringBuilder(key.toString());
while (values.hasNext()) {
	str.append(", "+values.next().toString());
}
output.collect(new Text(str.toString()),null);
}

}// end of reducer
