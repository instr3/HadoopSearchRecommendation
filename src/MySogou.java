import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


 
public class MySogou 
{
	public static int K=20;
	public static int ITER=6;
	public static class NodeFormat
	{
		public String source;
		public float sourceWeight;
		public String edge;
		public int edgeWeight;
		public NodeFormat()
		{
			
		}
		public NodeFormat(String str)
		{
			String[] tokens=str.split("\\|");
			int p=0;
			source=tokens[p++];
			sourceWeight=Float.parseFloat(tokens[p++]);
			edge=tokens[p++];
			edgeWeight=Integer.parseInt(tokens[p++]);
		}
		@Override
		public String toString()
		{
			String result=source+"|"+sourceWeight+"|"+edge+"|"+edgeWeight;
			return result;
		}
	}
	public static class SemiCompFormat
	{
		public String id;
		public float sourceWeight;
		public ArrayList<String> edges;
		public ArrayList<Integer> edgeWeights;
		public SemiCompFormat()
		{
			edges=new ArrayList<String>();
			edgeWeights=new ArrayList<Integer>();
		}
		public SemiCompFormat(String str)
		{
			String[] tokens=str.split("\\|");
			int p=0;
			id=tokens[p++];
			sourceWeight=Float.parseFloat(tokens[p++]);
			int edgeSize=Integer.parseInt(tokens[p++]);
			edges=new ArrayList<String>(edgeSize);
			edgeWeights=new ArrayList<Integer>(edgeSize);
			for(int i=0;i<edgeSize;++i)
			{
				edges.add(tokens[p++]);
				edgeWeights.add(Integer.parseInt(tokens[p++]));
			}
		}
		@Override
		public String toString()
		{
			String result=id+"|"+sourceWeight;
			result+="|"+edges.size();
			for(int i=0;i<edges.size();++i)
			{
				result+="|"+edges.get(i)+"|"+edgeWeights.get(i);
			}
			return result;
		}
	}
	public static class CompFormat
	{
		public String id;
		public ArrayList<String> sources;
		public ArrayList<Float> sourceWeights;
		public ArrayList<String> edges;
		public ArrayList<Integer> edgeWeights;
		public CompFormat()
		{
			sources=new ArrayList<String>();
			sourceWeights=new ArrayList<Float>();
			edges=new ArrayList<String>();
			edgeWeights=new ArrayList<Integer>();
		}
		public CompFormat(String str)
		{
			String[] tokens=str.split("\\|");
			int p=0;
			id=tokens[p++];
			int sourceSize=Integer.parseInt(tokens[p++]);
			sources=new ArrayList<String>(sourceSize);
			sourceWeights=new ArrayList<Float>(sourceSize);
			for(int i=0;i<sourceSize;++i)
			{
				sources.add(tokens[p++]);
				sourceWeights.add(Float.parseFloat(tokens[p++]));
			}
			int edgeSize=Integer.parseInt(tokens[p++]);
			edges=new ArrayList<String>(edgeSize);
			edgeWeights=new ArrayList<Integer>(edgeSize);
			for(int i=0;i<edgeSize;++i)
			{
				edges.add(tokens[p++]);
				edgeWeights.add(Integer.parseInt(tokens[p++]));
			}
		}
		
		@Override
		public String toString()
		{
			String result=id+"|"+sources.size();
			for(int i=0;i<sources.size();++i)
			{
				result+="|"+sources.get(i)+"|"+sourceWeights.get(i);
			}
			result+="|"+edges.size();
			for(int i=0;i<edges.size();++i)
			{
				result+="|"+edges.get(i)+"|"+edgeWeights.get(i);
			}
			return result;
		}
	}
	public static String[] GetToken(String input) 
	{
		String res[] = new String[6];
		String regex = "(\\d\\d:\\d\\d:\\d\\d)\\s+([\\S]+)\\s+(\\[.*?\\])\\s+(\\d+)\\s+(\\d+)\\s+(.*)";
		Pattern r = Pattern.compile(regex);
		Matcher m = r.matcher(input);

		if (!m.find()) System.out.println(input);
		for (int i = 0; i < 6; i ++ )
		{
			res[i] = m.group(i + 1);
		}
		return res;
	}

	public static class IntroMap1 extends Mapper<LongWritable, Text, Text, Text> {        

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {
			try
			{
				Text word = new Text();
				Text myresult = new Text();
				String line = new String(value.getBytes(), 0, value.getLength(), "GB18030");
				if(line.charAt(2)!=':')
					line="00:00:00\t"+line;
				String[] tokenizer = GetToken(line);
				tokenizer[2]=tokenizer[2].replace('|', '/');
				tokenizer[5]=tokenizer[5].replace('|', '/');
				
				NodeFormat node=new NodeFormat();
				node.edge=tokenizer[2];
				node.edgeWeight=1;
				node.source="N/A";
				node.sourceWeight=0.0f;
				word.set(tokenizer[5]);
				myresult.set(node.toString());
	        	//System.out.println(word+" : "+myresult);
				context.write(word, myresult);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
      
		}
	}
	public static class IntroReduce1 extends Reducer<Text, Text, Text, NullWritable> {

		class KVPair implements Comparable<KVPair>  
		{  
			public String id;
			public int value;
			public KVPair(String inputId, int inputValue)  
		    {  
				id=inputId;
				value=inputValue;
		    }
			public int compareTo(KVPair other)  
		    {  
				if (value<other.value) 
					return -1;  
				if (value>other.value)  
					return 1;  
				return 0;
		    }
		}
		@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			try
			{
	            HashMap<String,Integer> mym=new HashMap<String,Integer>();
	            int tot=0;
	            for (Text val: values) {
	            	NodeFormat node=new NodeFormat(val.toString());
	            	if(mym.containsKey(node.edge))
	            		mym.put(node.edge, mym.get(node.edge)+1);
	            	else
	            		mym.put(node.edge, 1);
	            	tot+=1;
	            }
	            if(mym.size()==0)
	            	return;
	            int p=0;
	            for(String val:mym.keySet())
	            {
	            	mym.put(val, mym.get(val)*tot+p);
	            	p+=1;
	            }
	            Integer[] valArray=mym.values().toArray(new Integer[0]);
	    		Arrays.sort(valArray);
	    		int threshold=valArray[valArray.length-Math.min(K, mym.size())];
	    		CompFormat comp=new CompFormat();
	    		comp.id=key.toString();
	            for(String val:mym.keySet())
	            {
	            	if(mym.get(val)>=threshold)
	            	{
	            		comp.edges.add(val);
	            		comp.edgeWeights.add((mym.get(val))/tot);
	            	}
	            }
	        	Text answer = new Text();
	        	answer.set(comp.toString());
	        	// System.out.println(answer);
	        	context.write(answer, null);
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
        }
	}
	public static class IntroMap2 extends Mapper<LongWritable, Text, Text, Text> {   

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			try
			{     
				Text word = new Text();
				Text myresult = new Text();
				CompFormat comp=new CompFormat(value.toString());
	        	//System.out.println(value.toString());
				String urlNode=comp.id;
				for(int i=0;i<comp.edges.size();++i)
				{
					String wordNode=comp.edges.get(i);
					NodeFormat node=new NodeFormat();
					node.edge=urlNode;
					node.edgeWeight=comp.edgeWeights.get(i);
					node.source="N/A";
					node.sourceWeight=0.0f;
					word.set(wordNode);
					myresult.set(node.toString());
					context.write(word, myresult);
				}

			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	public static class InitMap extends Mapper<LongWritable, Text, Text, Text> {   

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			try
			{     
				Text word = new Text();
				Text myresult = new Text();
				CompFormat comp=new CompFormat(value.toString());
	            int nodeWeightSum=0;
				for(int i=0;i<comp.edges.size();++i)
					nodeWeightSum+=comp.edgeWeights.get(i);
	        	//System.out.println(value.toString());
				String wordNode=comp.id;
				for(int i=0;i<comp.edges.size();++i)
				{
					String urlNode=comp.edges.get(i);
					NodeFormat node=new NodeFormat();
					node.edge=wordNode;
					node.edgeWeight=comp.edgeWeights.get(i);
					node.source=wordNode;
					node.sourceWeight=(1.0f*node.edgeWeight)/nodeWeightSum;
					word.set(urlNode);
					myresult.set(node.toString());
					//System.out.println(word+" : "+myresult);
					context.write(word, myresult);
				}

			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
		}
	}
	public static class CleanMap extends Mapper<LongWritable, Text, Text, Text> {   

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			try
			{     
				Text word = new Text();
				Text myresult = new Text();
				CompFormat comp=new CompFormat(value.toString());
				SemiCompFormat semiComp=new SemiCompFormat();
				semiComp.edges=comp.edges;
				semiComp.edgeWeights=comp.edgeWeights;
	            int nodeWeightSum=0;
				for(int i=0;i<comp.edges.size();++i)
					nodeWeightSum+=comp.edgeWeights.get(i);
				for(int s=0;s<comp.sources.size();++s)
				{
					semiComp.id=comp.id;
					// The division needs to be first performed
					// As we cannot tell the right nodeWeightSum in the following
					// LRMap run where sources replace wordNodes in this case
					semiComp.sourceWeight=comp.sourceWeights.get(s)*1.0f/nodeWeightSum;
					word.set(comp.sources.get(s));
					myresult.set(semiComp.toString());
					//System.out.println(word+" :(Cleaned): "+myresult);
					context.write(word, myresult);
				}
			}
			catch(Exception ex)
			{
				System.out.println("Error:" + value.toString());
			}
		}
	}
	
	public static class CleanReduce extends Reducer<Text, Text, Text, NullWritable> {
		
		class KVPair implements Comparable<KVPair>  
		{  
			public String id;
			public float value;
			public KVPair(String inputId, float inputValue)  
		    {  
				id=inputId;
				value=inputValue;
		    }
			public int compareTo(KVPair other)  
		    {  
				if (value<other.value) 
					return -1;  
				if (value>other.value)  
					return 1;  
				return 0;
		    }
		}
		@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			try
			{
	            ArrayList<KVPair> list=new ArrayList<KVPair>();
	        	//System.out.println("Input " + key + " : ");
	            for (Text val: values) {
	            	SemiCompFormat node=new SemiCompFormat(val.toString());
	            	list.add(new KVPair(val.toString(),node.sourceWeight));
	            }
	            KVPair[] pairs=list.toArray(new KVPair[0]);
	            Arrays.sort(pairs);
	            int lim=Math.min(K, pairs.length);
	            for(int i=pairs.length-lim;i<pairs.length;++i)
	            {
	            	SemiCompFormat node=new SemiCompFormat(pairs[i].id);
		            CompFormat comp=new CompFormat();
		            comp.id=key.toString(); // Swapped with source
	            	comp.edges=node.edges;
	            	comp.edgeWeights=node.edgeWeights;
            		comp.sources.add(node.id);
            		comp.sourceWeights.add(pairs[i].value);
    	        	Text answer = new Text();
    	        	answer.set(comp.toString());
    	        	//System.out.println("Rev:"+answer);
    	        	context.write(answer, null);
	            }
			}
			catch(Exception ex)
			{
				ex.printStackTrace();
			}
        }
	}
	public static class LRMap extends Mapper<LongWritable, Text, Text, Text> {   

		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			try
			{     
				Text word = new Text();
				Text myresult = new Text();
				CompFormat comp=new CompFormat(value.toString());
	            // Meaningless
				//int nodeWeightSum=0;
				//for(int i=0;i<comp.edges.size();++i)
				//	nodeWeightSum+=comp.edgeWeights.get(i);
	        	//System.out.println(value.toString());
				for(int s=0;s<comp.sources.size();++s)
				{
					for(int e=0;e<comp.edges.size();++e)
					{
						String wordNode=comp.edges.get(e);
						NodeFormat node=new NodeFormat();
						String urlNode=comp.sources.get(s); // Swapped
						node.source=comp.id; // Swapped
						node.edge=urlNode;
						node.edgeWeight=comp.edgeWeights.get(e);
						// The division has been performed before (in CleanMap)
						// Only multiplication is needed
						node.sourceWeight=comp.sourceWeights.get(s)*node.edgeWeight;
						word.set(wordNode);
						myresult.set(node.toString());
						//System.out.println(word + ":" + myresult);
						context.write(word, myresult);
					}
				}
			}
			catch(Exception ex)
			{
				System.out.println("Error:" + value.toString());
			}
		}
	}
	public static class LRReduce extends Reducer<Text, Text, Text, NullWritable> {
		@Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			try
			{
	            CompFormat comp=new CompFormat();
	            HashMap<String, Float> mys=new HashMap<String,Float>();
	            HashMap<String, Integer> mym=new HashMap<String,Integer>();
	            comp.id=key.toString();
	        	//System.out.println("Input " + key + " : ");
	            for (Text val: values) {
		        	//System.out.println(val);
	            	NodeFormat node=new NodeFormat(val.toString());
	            	if(mys.containsKey(node.source))
	            		mys.put(node.source, mys.get(node.source)+node.sourceWeight);
	            	else
	            		mys.put(node.source, node.sourceWeight);
	            	if(!mym.containsKey(node.edge))
	            		mym.put(node.edge, node.edgeWeight);
	            }
	            for(String source:mys.keySet())
	            {
	            	comp.sources.add(source);
	            	comp.sourceWeights.add(mys.get(source));
	            }

	            for(String edge:mym.keySet())
	            {
	            	comp.edges.add(edge);
	            	comp.edgeWeights.add(mym.get(edge));
	            }
	        	Text answer = new Text();
	        	answer.set(comp.toString());
	        	//System.out.println(answer);
	        	context.write(answer, null);
			}
			catch(Exception ex)
			{
				
				ex.printStackTrace();
			}
        }
	}
	public static class TestMap extends Mapper<LongWritable, Text, Text, Text> {   

		String keyword;
		@Override
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException {

			try
			{     
				Text word = new Text();
				Text myresult = new Text();
				CompFormat comp=new CompFormat(value.toString());
	            // Meaningless
				//int nodeWeightSum=0;
				//for(int i=0;i<comp.edges.size();++i)
				//	nodeWeightSum+=comp.edgeWeights.get(i);
	        	//System.out.println(value.toString());
				if(comp.id=="["+keyword+"]")
				{
					for(int s=0;s<comp.sources.size();++s)
					{
						for(int e=0;e<comp.edges.size();++e)
						{
							NodeFormat node=new NodeFormat();
							node.source=comp.id; // Swapped
							String urlNode=comp.sources.get(s); // Swapped
							node.edge=urlNode;
							node.edgeWeight=comp.edgeWeights.get(e);
							// The division has been performed before (in CleanMap)
							// Only multiplication is needed
							node.sourceWeight=comp.sourceWeights.get(s)*node.edgeWeight;
							word.set("sogouQ");
							myresult.set(node.toString());
							//System.out.println(word + ":" + myresult);
							context.write(word, myresult);
						}
					}
				}
				
			}
			catch(Exception ex)
			{
				System.out.println("Error:" + value.toString());
			}
		}
	}
    public static boolean deleteDir(String dir) throws IOException { 
    	try
    	{
            Configuration conf = new Configuration();  
            FileSystem fs = FileSystem.get(URI.create(dir), conf);  
            fs.delete(new Path(dir), true);  
            fs.close();  
            return true;  
    	}
    	catch(Exception ex)
    	{
    		return false;
    	}
    }  
    static void DefaultJobModifier(Job job)
    {
        //FileInputFormat.setMaxInputSplitSize(job, 10*1048576);
        //job.setNumReduceTasks(200);
    }
    public static void main(String[] args) throws Exception {

        if (args.length != 2) 
        {
            System.err.println("Usage: MySogou <input path> <output path>");
            System.err.println("Usage: MySogou keyword");
            System.exit(-1);
        }
        deleteDir(args[1]+"tmp0");
        deleteDir(args[1]+"tmp1");
        
        Job job1 = new Job();
        job1.setJarByClass(MySogou.class);
        job1.setJobName("first job");
        DefaultJobModifier(job1);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]+"tmp0"));
        job1.setMapperClass(IntroMap1.class);
        job1.setReducerClass(IntroReduce1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(NullWritable.class);
        System.out.println("Job1 returned "+job1.waitForCompletion(true));
        
        Job job2 = new Job();
        job2.setJarByClass(MySogou.class);
        job2.setJobName("second job");
        DefaultJobModifier(job2);
        FileInputFormat.addInputPath(job2, new Path(args[1]+"tmp0"));
//        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]+"tmp1"));
        job2.setMapperClass(IntroMap2.class);
        job2.setReducerClass(IntroReduce1.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);

        System.out.println("Job2 returned "+job2.waitForCompletion(true));

        String inputDir=args[1]+"tmp1";
        String outputDir=args[1]+"tmp0";
        for(int i=0;i<ITER;++i)
        {
        	
            deleteDir(outputDir);
            Job job3 = new Job();
            job3.setJarByClass(MySogou.class);
            job3.setJobName("third job iter " + (i+1));
            DefaultJobModifier(job3);
            FileInputFormat.addInputPath(job3, new Path(inputDir));
//            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job3, new Path(outputDir));
            job3.setMapperClass(i==0?InitMap.class:LRMap.class);
            job3.setReducerClass(LRReduce.class);
            job3.setMapOutputKeyClass(Text.class);
            job3.setMapOutputValueClass(Text.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(NullWritable.class);
            
            System.out.println("Job3 iter " + (i+1) + " returned "+job3.waitForCompletion(true));
            // String tmp=inputDir;inputDir=outputDir;outputDir=tmp;

            deleteDir(inputDir);
            Job job4 = new Job();
            job4.setJarByClass(MySogou.class);
            job4.setJobName("fourth job iter " + (i+1));
            DefaultJobModifier(job4);
            FileInputFormat.addInputPath(job4, new Path(outputDir));
//            FileOutputFormat.setOutputPath(job1, new Path(args[1]));
            FileOutputFormat.setOutputPath(job4, new Path(inputDir));
            job4.setMapperClass(CleanMap.class);
            job4.setReducerClass(CleanReduce.class);
            job4.setMapOutputKeyClass(Text.class);
            job4.setMapOutputValueClass(Text.class);
            job4.setOutputKeyClass(Text.class);
            job4.setOutputValueClass(NullWritable.class);
            
            System.out.println("Job4 iter " + (i+1) + " returned "+job4.waitForCompletion(true));
            // String tmp=inputDir;inputDir=outputDir;outputDir=tmp;

        }

    }
	
}
