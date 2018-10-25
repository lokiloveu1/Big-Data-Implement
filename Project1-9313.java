package comp9313.proj1;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.StringTokenizer;

//import org.apache.avro.file.FileReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Project1 {
	static int N;
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		//private HashMap<String, Integer> map = new HashMap<String, Integer>();
	    // map<key,Value>
	    // MapReduce docid 1
		
	    //private Text keyInfo = new Text(); // keyInfo = <word,docid>
	    //private IntWritable valueInfo = new IntWritable(); // valueInfo = <count(word)>
		int total = 0;
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			total++;
			String line = value.toString();
	        String[] fields = line.split(" ");//get split values
	        String docid = fields[0];
	        String valuetostring = "";
	        for (int i=1;i<fields.length;i++){
	        	valuetostring = valuetostring + fields[i]+" ";
	        }
			StringTokenizer itr = new StringTokenizer(valuetostring, " *$&#/\t\n\f\"'\\,.:;?![](){}<>~-_");
			//System.out.println(N);
			while (itr.hasMoreTokens()) {
				String term = itr.nextToken().toLowerCase();
				//set keyInfo and valueInfo
				String s9 = term + " " + docid;
				context.write(new Text(s9), new IntWritable(1));
				
			}
			Project1.N = total;
		}
	}
		

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();
		//run reducer to count the number of word in ever line
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
    public static class MyPartitoner extends Partitioner<Text, IntWritable>{  
        //my Partitioner to save data
        @Override  
        public int getPartition(Text key, IntWritable value, int numPartitions) {   
            String ip1 = key.toString();  
            ip1 = ip1.substring(0, ip1.indexOf(" "));  
            Text p1 = new Text(ip1);  
        return Math.abs((p1.hashCode() * 127) % numPartitions); //use hash function 
        }  
}  
	
	//part2-----------------------------------------------------  
    public static  class Mapper_Part2 extends  
    	Mapper<LongWritable, Text, Text, Text>{  //my second mapper
    	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException{  
    		String val = value.toString().replaceAll("  ", " "); //remove tab to " "
    		int index = val.indexOf(" ");  
    		String s1 = val.substring(0, index); //key eg: hello  
    		String s2 = val.substring(index + 1); //value eg: 1 6  
    		s2 += " ";  
    		s2 += "1";  //count =  “1” 。 eg: docid1 3
    		context.write(new Text(s1), new Text(s2));  
        	}  
    }  

    
    public static class Reduce_Part2 extends Reducer<Text, Text, Text, Text>{  
    	double file_count;  //my second reducer to count tf and df
	    public void reduce(Text key, Iterable<Text>values, Context context)throws IOException, InterruptedException{  
	    	//int fc = Project1.N;
			//file_count = fc;
	    	//file_count = 10000;
	    	file_count = N;
	        double sum = 0;  
	        List<String> vals = new ArrayList<String>();  
	
	        for (Text str : values){
	        	String con = str.toString();
	        	//System.out.println("print [con]:"+count+con);
	        	if (con=="\t"||con=="	"){
	        		con = " ";
	        	}
	            int index = con.lastIndexOf(" ");  
	            sum += Integer.parseInt(con.substring(index + 1)); //
	            vals.add(con.substring(0, index)); //save values to array vals
	          
	        }  
	        double tmp = file_count / sum;  //my 1/DF  
	        for (int j = 0;j < vals.size(); j++){  
	            String val = vals.get(j);  
	            String end = val.substring(val.lastIndexOf("\t")); 
	            double f_end = Double.valueOf(end); //TF 
	            val = val.substring(0,val.lastIndexOf("	"));
	            val += ",";   
	            val +=Math.log10(tmp)*f_end;// tf-idf
	            context.write(key, new Text(val));  
	        }  
	    }  
    }
    
    public static class MyPartitoner2 extends Partitioner<Text, Text>{  
        //my Partitioner2 to save data in Path:tmp2
        @Override  
        public int getPartition(Text key, Text value, int numPartitions) {  
            //the same as before
            String ip1 = key.toString(); 
            Text p1 = new Text(ip1);  
        return Math.abs((p1.hashCode() * 127) % numPartitions);  
        }  
}  
    
    //part3------sort-----------------------------------------------
 

    public static class SecondarySortMapper extends //
        Mapper<LongWritable,Text,PairWritable,Text>{
    	// mapper class for SecondarySortMapper
        private PairWritable mapOutputKey = new PairWritable() ;
        private IntWritable mapOutputValue = new IntWritable() ;

        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            // line value
            String lineValue = value.toString();
            // split
            String[] strs = lineValue.split(",") ;
            // invalidate
            if(2 != strs.length){
                return ;
            }
            String k0 = strs[0].substring(0,strs[0].lastIndexOf("\t"));
            String k1 = strs[0].substring(strs[0].lastIndexOf("\t")+1);
            int v1 = Integer.valueOf(k1);
            // set map  output key and value
            mapOutputKey.set(k0, v1);
            mapOutputValue.set(v1);
            context.write(mapOutputKey, value);
        }
    }

    
 
    public static class SecondarySortReducer extends Reducer<PairWritable,Text,Text,Text>{
        private Text outputKey = new Text() ;
        //reducer class for SecondarySortReducer
        public void reduce(PairWritable key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
	        List<String> vals = new ArrayList<String>();  
	        for (Text str : values){
	        	
	        	String con = str.toString();
	        	if (con=="\t"||con==" "){
	        		continue;
	        	}
	        	vals.add(con); //save values to vals
	        } 
            outputKey.set(key.getFirst());//get the first element in Pair<key,value>
	        for (int j = 0;j < vals.size(); j++){  
	            String val = vals.get(j);  
	            val = val.substring(val.lastIndexOf("\t")+1);
	            context.write(outputKey, new Text(val));  //output my result
	        }  
        }
    }
    
    public static class PairWritable implements WritableComparable<PairWritable> {
    	//my PairWritable to count the value and compare them and to sort
        private String first;
        private int second;

        public PairWritable() {
        }

        public PairWritable(String first, int second) {
            this.set(first, second);
        }

        public void set(String first, int second) {
            this.first = first;
            this.setSecond(second);
        }

        public String getFirst() {
            return first;
        }

        public void setFirst(String first) {
            this.first = first;
        }

        public int getSecond() {
            return second - Integer.MAX_VALUE;
        }

        public void setSecond(int second) {
            this.second = second + Integer.MAX_VALUE;
        }

        public void write(DataOutput out) throws IOException {
            out.writeUTF(first);//
            out.writeInt(second);
        }

        public void readFields(DataInput in) throws IOException {
            this.first = in.readUTF();
            this.second = in.readInt();
        }

        public int compareTo(PairWritable o) {
            // compare first
            int comp =this.first.compareTo(o.getFirst()) ;

            // eqauls
            if(0 != comp){
                return comp ;
            }

            // compare
            return Integer.valueOf(this.getSecond()).compareTo(Integer.valueOf(o.getSecond())) ;
        }

    }
    
    public static class FirstPartitioner extends Partitioner<PairWritable,Text> {
    	//my Partitioner3 to save data
        @Override
        public int getPartition(PairWritable key, Text value,
                int numPartitions) {
            return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }

    }
    
    
    public static class FirstGroupingComparator implements RawComparator<PairWritable> {

        // object compare
        public int compare(PairWritable o1, PairWritable o2) {
            return o1.getFirst().compareTo(o2.getFirst());
        }

        // bytes compare
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return WritableComparator.compareBytes(b1, 0, l1 - 4, b2, 0, l2 - 4);
        }

    }
    
   
    //main----------------------------------------------------------
	public static void main(String[] args) throws Exception {
		Path tmp = new Path("tmp");
		Path tmp2 = new Path("tmp2");
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "step1:word-count");
		
		FileSystem hdfs = FileSystem.get(conf);  
		job.setJarByClass(Project1.class);
		
		job.setMapperClass(TokenizerMapper.class);
		job.setPartitionerClass(MyPartitoner.class); //MyPartitoner  
		job.setReducerClass(IntSumReducer.class);
		
    	job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
		job.setNumReduceTasks(Integer.valueOf(args[2]));  
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));//input path
		FileOutputFormat.setOutputPath(job, tmp);  
		//System.exit(job.waitForCompletion(true) ? 0 : 1);
		job.waitForCompletion(true);  
		//-----------------------------------------------------
        //part2----------------------------------------  
        Configuration conf2 = new Configuration();  
  
        Job job2 = Job.getInstance(conf2, "step2-tf-idf");  
          
        job2.setJarByClass(Project1.class);  
        
        job2.setMapperClass(Mapper_Part2.class);  
        job2.setReducerClass(Reduce_Part2.class);  
        job2.setNumReduceTasks(Integer.valueOf(args[2]));  
        
//        job.setInputFormatClass(KeyValueTextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
          
        job2.setMapOutputKeyClass(Text.class);  
        job2.setMapOutputValueClass(Text.class);  
        job2.setPartitionerClass(MyPartitoner2.class);
        job2.setOutputKeyClass(Text.class);  
        job2.setOutputValueClass(Text.class);  

        //job2.setNumReduceTasks(1);  
          
        FileInputFormat.setInputPaths(job2, tmp); 
        FileOutputFormat.setOutputPath(job2, tmp2);
        
        job2.waitForCompletion(true); 
        hdfs.delete(tmp, true); 
      //-----------------------------------------------------
        //part3----------------------------------------  
        Configuration conf3 = new Configuration();  
        
        Job job3 = Job.getInstance(conf3, "My_sort");  
          
        job3.setJarByClass(Project1.class);  
          
        job3.setMapOutputKeyClass(PairWritable.class);  
        job3.setMapOutputValueClass(Text.class);  
        job3.setPartitionerClass(FirstPartitioner.class);//
        job3.setNumReduceTasks(Integer.valueOf(args[2]));  
        job3.setOutputKeyClass(Text.class);  
        job3.setOutputValueClass(Text.class);  
        job3.setGroupingComparatorClass(FirstGroupingComparator.class);//
        job3.setMapperClass(SecondarySortMapper.class); 
        job3.setReducerClass(SecondarySortReducer.class);
        //job3.setNumReduceTasks(1);  
        
        FileInputFormat.setInputPaths(job3, tmp2);  
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));  

        job3.waitForCompletion(true); 
        hdfs.delete(tmp2, true); 
	}
}