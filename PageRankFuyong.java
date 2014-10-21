// Fuyong Xing
// Department of Electrical and Computer Engineering
// University of Florida
// This code is for Programming Assignment 2 at the course: Cloud Computing and Storage.
// It implements PageRank given a graph. It is implemented using OpenJDK 6 and Hadoop 0.20.2. 

// Usage: $ bin/hadoop jar PageRankFuyong.jar PageRankFuyong inputFolder maximumIteration

import java.io.*;
import java.util.*;
	
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRankFuyong {

 /* Define enum type. */
 public static enum DiffPR {	
	SUM_DIFFPR,
	COUNT_NODE
 }
	
 /* PageRank Mapper implementation. It splits a line of strings to single strings and then emits a key-value pair of <node, PR>. It also emits the graph structure. */	
 public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String lineStr = value.toString();	
        Text textKey = new Text();
        Text textValue = new Text();
	if (lineStr.contains("#")) { // non-first iteration
		String[] strParts = lineStr.split("#");
		String[] strIOPR = strParts[0].split("\\t");
		String nodeID = strIOPR[0];
		String nodePR = strIOPR[1];
		if (strParts.length == 1) {
			textKey.set(nodeID);
			textValue.set("#");
 			output.collect(textKey, textValue);
			textValue.set(nodePR + "@");
			output.collect(textKey, textValue);
		} else {
		
			String[] nodeAdjList = strParts[1].split(" ");
			int numAdjNode = nodeAdjList.length;
			float nodePRV = Float.valueOf(nodePR)/numAdjNode;
			textKey.set(nodeID);
			textValue.set("#" + strParts[1]);
 			output.collect(textKey, textValue);
			textValue.set(nodePR + "@");
			output.collect(textKey, textValue);
			for (int i=0; i<numAdjNode; i++) {
				textKey.set(nodeAdjList[i]);
				textValue.set(Float.toString(nodePRV));
				output.collect(textKey, textValue);
			}  
		}
	} else { // first iteration
		String[] strParts = lineStr.split(" ");
		int strLength = strParts.length;
		if (strLength < 1) {
			return;
		} else if (strLength < 2) { // no adjacent nodes
			if (!("\\n".equals(strParts[0]) || "".equals(strParts[0]))) {
			
				String nodeID = strParts[0];
				textKey.set(nodeID);
				textValue.set("#");
				output.collect(textKey, textValue);
				textValue.set("1.0@");
				output.collect(textKey, textValue);
			}
		}
		else {	// with adjacent nodes 		
			String nodeID = strParts[0];
			String nodePR = "1.0";			
			int numAdjNode = strParts.length - 1;
			StringBuffer strResult = new StringBuffer();
			for (int i=1; i<numAdjNode; i++) {
				strResult.append(strParts[i] + " ");			
			}
			strResult.append(strParts[numAdjNode]);
			String nodeAdjStr = strResult.toString();
			float nodePRV = Float.valueOf(nodePR)/numAdjNode;
			textKey.set(nodeID);
			textValue.set("#" + nodeAdjStr);
			output.collect(textKey, textValue);
			textValue.set("1.0@");
			output.collect(textKey, textValue);
			for (int j=1; j<numAdjNode+1; j++) {
				textKey.set(strParts[j]);
				textValue.set(Float.toString(nodePRV));
				output.collect(textKey, textValue);
			}
		}
		
	}
    }
 } 

 /* PageRank Reducer implementation. It sums the values associated with the same key (node ID). */
 public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {	
     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	
	Float sumPR = 0.0f;
	String strM = "";
	String str = "";
	String[] strPre = new String[2];
	
	while (values.hasNext()) {
		str = values.next().toString();
		if (str.contains("#")) { // It is a node.
			strM = str;
		} else if (str.contains("@")) { // Get previous value
			strPre = str.split("@");
		} else {			
			sumPR = sumPR + Float.parseFloat(str);
		}
	}
	output.collect(key, new Text(sumPR.toString()+strM));

	float diff = (sumPR - Float.valueOf(strPre[0]))*(sumPR - Float.valueOf(strPre[0]));
	reporter.getCounter(DiffPR.SUM_DIFFPR).increment((long)(diff*100000000));
	reporter.getCounter(DiffPR.COUNT_NODE).increment(1L);
    }		
 }

 /* PageRank Job running function. It configures and submits the jobs. */
 private long[] runPageRank(String inputFolder, String outputFolder) throws Exception {	

	/* Create a new job and set up job parameters. */
	JobConf conf = new JobConf(PageRankFuyong.class);
     	conf.setJarByClass(PageRankFuyong.class);
     	conf.setJobName("PageRankFuyong");
     	conf.setOutputKeyClass(Text.class);
     	conf.setOutputValueClass(Text.class);	
     	conf.setMapperClass(Map.class);
     	conf.setReducerClass(Reduce.class);	
     	conf.setInputFormat(TextInputFormat.class);
     	conf.setOutputFormat(TextOutputFormat.class);
     
     	/* Set up input and output file paths. */
     	FileInputFormat.setInputPaths(conf, new Path(inputFolder));
     	FileOutputFormat.setOutputPath(conf, new Path(outputFolder));

     	/* Submit the jobs. */
     	RunningJob job=JobClient.runJob(conf);
     	job.waitForCompletion();
	Counters countJob = job.getCounters();
	long[] countValue = new long[2];
	countValue[0] = countJob.findCounter(DiffPR.SUM_DIFFPR).getValue();
	countValue[1] = countJob.findCounter(DiffPR.COUNT_NODE).getValue();
	return countValue;
 }

/* Sorting Mapper: sort the PageRank results. */
 public static class SortMap extends MapReduceBase implements Mapper<LongWritable, Text, FloatWritable, Text> { 
    public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output, Reporter reporter) throws IOException {

	String lineStr = value.toString();	
	if (lineStr.contains("#")) {
		String[] strParts = lineStr.split("#");
		String[] strIOPR = strParts[0].split("\\t");
		float nodeID = Float.parseFloat(strIOPR[1]);
		FloatWritable prKey = new FloatWritable(nodeID);
		Text textValue = new Text(strIOPR[0]);
		output.collect(prKey, textValue);
	}
    }
 }

 /* Sorting PageRank Job running function. It configures and submits the jobs. */
 private void runSortPageRank(String inputFolder, String outputFolder) throws Exception {	

	/* Create a new job and set up job parameters. */
	JobConf conf = new JobConf(PageRankFuyong.class);
     	conf.setJarByClass(PageRankFuyong.class);
     	conf.setJobName("SortPageRankFuyong");
     	conf.setOutputKeyClass(FloatWritable.class);
     	conf.setOutputValueClass(Text.class);	
     	conf.setMapperClass(SortMap.class);	
     	conf.setInputFormat(TextInputFormat.class);
     	conf.setOutputFormat(TextOutputFormat.class);
     
     	/* Set up input and output file paths. */
     	FileInputFormat.setInputPaths(conf, new Path(inputFolder));
     	FileOutputFormat.setOutputPath(conf, new Path(outputFolder));

     	/* Submit the jobs. */
     	RunningJob job=JobClient.runJob(conf);
     	job.waitForCompletion();
 }

/* Mapper of getting graph properties. It calculates the graph statistics and emits a key-value pair of <Graph, Property>. */	
 public static class StatMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        String lineStr = value.toString();	
        final Text textKey = new Text("Graph");
        Text textValue = new Text();
	if (lineStr.contains("#")) {
		String[] strParts = lineStr.split("#");
		if (strParts.length == 1) {
			textValue.set("0");
 			output.collect(textKey, textValue);
		} else {		
			String[] nodeAdjList = strParts[1].split(" ");
			int numAdjNode = nodeAdjList.length;
			textValue.set(Integer.toString(numAdjNode));
			output.collect(textKey, textValue);
		}
	} 
    }
 } 

 /* Reducer of getting graph properties. It computes the statistics of the graph. */
 public static class StatReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {	
     public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    	
	int numNode = 0;
	int numEdge = 0;
	int curEdge = 0;
	int maxOutDeg = 0;
	int minOutDeg = 1000;	
	while (values.hasNext()) {
		curEdge = Integer.parseInt(values.next().toString());
		if (maxOutDeg < curEdge) {
			maxOutDeg = curEdge;
		}
		if (minOutDeg > curEdge) {
			minOutDeg = curEdge;
		}
		
		numNode += 1;
		numEdge += curEdge;
	}
	float aveOutDeg = (float)numEdge/numNode;
	String str = "\n\nNumber of nodes = " + Integer.toString(numNode) + "\n" + "Number of edges = " + Integer.toString(numEdge) + "\n";
	str = str + "Maximum out-degree = " + Integer.toString(maxOutDeg) + "\n" + "Minimum out-degree = " + Integer.toString(minOutDeg) + "\n";
	str = str + "Average out-degree = " + Float.toString(aveOutDeg);
	output.collect(new Text("Graph properties:"), new Text(str));
    }		
 }

 /* Compute graph property Job running function. It configures and submits the jobs. */
 private void runStatPageRank(String inputFolder, String outputFolder) throws Exception {	

	/* Create a new job and set up job parameters. */
	JobConf conf = new JobConf(PageRankFuyong.class);
     	conf.setJarByClass(PageRankFuyong.class);
     	conf.setJobName("StatPageRankFuyong");
     	conf.setOutputKeyClass(Text.class);
     	conf.setOutputValueClass(Text.class);	
     	conf.setMapperClass(StatMap.class);
     	conf.setReducerClass(StatReduce.class);	
     	conf.setInputFormat(TextInputFormat.class);
     	conf.setOutputFormat(TextOutputFormat.class);
     
     	/* Set up input and output file paths. */
     	FileInputFormat.setInputPaths(conf, new Path(inputFolder));
     	FileOutputFormat.setOutputPath(conf, new Path(outputFolder));

     	/* Submit the jobs. */
     	RunningJob job=JobClient.runJob(conf);
     	job.waitForCompletion();
 }


 /* Main function. */
 public static void main(String[] args) throws Exception {
	PageRankFuyong pageRankObj = new PageRankFuyong();

	/* Run PageRank job. */
	int maxIter = Integer.parseInt(args[1]);
	double stopCriterion = 0.001;
	double sumdiffprv = 0.0;
	long[] countValue = new long[2];
	long startTime = 0;
	long totalPRtime = 0;
	int iter=1;
	ArrayList<Long> iterTime = new ArrayList<Long>();
	do {
		startTime = System.currentTimeMillis();
		if (iter == 1) {
			countValue = pageRankObj.runPageRank(args[0], args[0]+Integer.toString(iter)); 			
		} else {
			countValue = pageRankObj.runPageRank(args[0]+Integer.toString(iter-1), args[0]+Integer.toString(iter)); 
		}
		sumdiffprv = Math.sqrt((double)countValue[0]/countValue[1]/100000000); 			
		iterTime.add(System.currentTimeMillis() - startTime);
		totalPRtime += iterTime.get(iter-1);

		iter++;
	} while (sumdiffprv > stopCriterion && iter <= maxIter);

	/* Run SortPageRank job. */
	startTime = System.currentTimeMillis();
	pageRankObj.runSortPageRank(args[0]+Integer.toString(iter-1), args[0]+"SortedResults");
	long sortTime = System.currentTimeMillis() - startTime;
	totalPRtime += sortTime;

	/* Run StatPageRank job. */
	pageRankObj.runStatPageRank(args[0]+Integer.toString(1), args[0]+"StatResults");

	/* Print iteration number and time cost. */
	System.out.println("Total iterations of PageRank: " + (iter-1));
	System.out.println("Total time (including sorting time): " + totalPRtime + " miliseconds.");
	for (int i=0; i<iterTime.size(); i++) {
		
		System.out.println("PageRank time for iteration " + (i+1) + ": " + iterTime.get(i) + " miliseconds.");	
	}
	System.out.println("Sort PageRank time: " + sortTime + " miliseconds.");
 }
}
