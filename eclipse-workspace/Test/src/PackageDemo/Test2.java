package PackageDemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class Test2{
	
	public static class Mykey implements WritableComparable<Mykey> {

		private String algoritmo;
		private String tempo;

		public Mykey() { }
		

		public Mykey(String algoritmo, String tempo) {
			this.algoritmo = algoritmo;
			this.tempo = tempo;
		}
		
		@Override
		public String toString() {
			return (new StringBuilder())
					.append('{')
					.append(algoritmo)
					.append(',')
					.append(tempo)
					.append('}')
					.toString();
		}
		
		@Override
		public void readFields(DataInput in) throws IOException {
			algoritmo = WritableUtils.readString(in);
			tempo = WritableUtils.readString(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, algoritmo);
			WritableUtils.writeString(out, tempo);
		}

		@Override
		public int compareTo(Mykey o) {
			int result = algoritmo.compareTo(o.algoritmo);
			if(0 == result) {
				result = Float.valueOf(tempo).compareTo(Float.valueOf(o.tempo));
				
			}
			return result;
		}


		public String getAlgoritmo() {
			return algoritmo;
		}

		public void setAlgoritmo(String algoritmo) {
			this.algoritmo = algoritmo;
		}

	
		public String getTempo() {
			return tempo;
		}

		public void setTempo(String tempo) {
			this.tempo = tempo;
		}

	}
	
	public static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(Mykey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Mykey k1 = (Mykey)w1;
			Mykey k2 = (Mykey)w2;
			
			int result = k1.getAlgoritmo().compareTo(k2.getAlgoritmo());
			if(0 == result) {
				result = -1* Float.valueOf(k1.getTempo()).compareTo(Float.valueOf(k2.getTempo()));
			}
			return result;
		}
	}
	
	public static class KeyGroupingComparator extends WritableComparator {
		protected KeyGroupingComparator() {
			super(Mykey.class, true);
		}
		
		@SuppressWarnings("rawtypes")
		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			Mykey k1 = (Mykey)w1;
			Mykey k2 = (Mykey)w2;
			
			return k1.getAlgoritmo().compareTo(k2.getAlgoritmo());
		}
	}
	
	public static class TokenizeMapper extends Mapper<Object, Text, Mykey, Text>{	

		String valore = new String();
		Mykey mykey = new Mykey();
		Text fval = new Text();

		@Override
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			String[] stringa = value.toString().split("\t");
			if((!stringa[0].equals("Solver")) && stringa[14].equals("solved")) {
				valore=stringa[11];
				mykey.setAlgoritmo(stringa[0]);
				mykey.setTempo(valore);
				fval.set(valore);
				context.write(mykey,fval);
			}
		}
	}

	public static class ReducerOne extends Reducer<Mykey, Text, Mykey, Text>{
		Text t = new Text();
		protected void reduce(Mykey arg0, Iterable<Text> arg1,
				org.apache.hadoop.mapreduce.Reducer<Mykey, Text, Mykey, Text>.Context arg2)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			String res = new String();
			for (Text floatto : arg1) {
				res+=floatto.toString()+" ";
			}
			t.set(res);
			arg2.write(arg0, t);
		}

	}


	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "U megghiu test");
		job.setJarByClass(Test2.class);
		job.setMapperClass(TokenizeMapper.class);
		job.setCombinerClass(ReducerOne.class);
		job.setReducerClass(ReducerOne.class);
		job.setSortComparatorClass(KeyComparator.class);
		job.setGroupingComparatorClass(KeyGroupingComparator.class);
		job.setMapOutputKeyClass(Mykey.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Mykey.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);


	}
}
