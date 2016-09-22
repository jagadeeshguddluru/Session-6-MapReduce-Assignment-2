# Session-6-MapReduce-Assignment-2
public class Olymap extends Mapper<LongWritable,Text,Text,IntWritable>  {
	Text country=new Text();
	IntWritable totalmedals=new IntWritable();
	int i=0;
	public void map(LongWritable key,Text value,Context c) throws IOException, InterruptedException{
		String s=value.toString();
		String s1[]=s.split("\t");
		if(s1[2].equalsIgnoreCase("India")){
			country.set(s1[3]);
			i=Integer.parseInt(s1[9]);
			totalmedals.set(i);
			c.write(country, totalmedals);
		}
	}

}

public class Olyred extends Reducer<Text,IntWritable,Text,IntWritable> {
public void reduce(Text value,Iterable<IntWritable> key,Context c) throws IOException, InterruptedException{
	int sum=0;
	for(IntWritable x:key)
		sum=sum+x.get();
	c.write(value, new IntWritable(sum));	
}
}


public class Olydriver {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		Configuration conf=new Configuration();
		Job j=Job.getInstance(conf, "p1.Olydriver.class");
		j.setMapperClass(p1.Olymap.class);
		j.setReducerClass(p1.Olyred.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(IntWritable.class);
		j.setJarByClass(p1.Olydriver.class);
		j.setJobName("Olydriver");
		FileInputFormat.addInputPath(j, new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		System.exit(j.waitForCompletion(true)?0:1);
	}

}
