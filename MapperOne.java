package hadoopprjt1;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class MapperOne extends MapReduceBase implements
Mapper<LongWritable, Text, Text, Text> {

private Text word = new Text("");
private Text file = new Text();

@Override
public void map(LongWritable key, Text value,
	OutputCollector<Text, Text> output, Reporter reporter)
	throws IOException {

String line = value.toString();
StringTokenizer tokenizer = new StringTokenizer(line);

FileSplit split = (FileSplit) reporter.getInputSplit();

String name = split.getPath().getName();
//String name="filename";

file.set(name + "@" + key);

while (tokenizer.hasMoreTokens()) {
	word.set(tokenizer.nextToken().toString());
	output.collect(word, new Text(file));
}

}

}// end of mapper
