import java.io.IOException;
import java.io.StringReader;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

public class TopCount {
    public static class XmlIDMapper extends Mapper<Object, Text, IntWritable, IntWritable> {

        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        private TreeMap<Integer, Integer> topReputationIDs = new TreeMap<>();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String valueString = value.toString();
            if(!valueString.trim().startsWith("<row")){
                return;
            }

            try {
                DocumentBuilder builder = factory.newDocumentBuilder();
                InputSource is = new InputSource(new StringReader(value.toString()));
                Document doc = builder.parse(is);
                NamedNodeMap attrs = doc.getFirstChild().getAttributes();

                Integer id = Integer.parseInt(attrs.getNamedItem("Id").getNodeValue());
                Integer reputation = Integer.parseInt(attrs.getNamedItem("Reputation").getNodeValue());
                topReputationIDs.put(reputation, id);
            } catch (Exception exc) {
                System.err.printf("========================= got a fucking exception ========================\n");
                return;
            }

            if(topReputationIDs.size() > 10) {
                topReputationIDs.pollFirstEntry();
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map.Entry<Integer, Integer> e = topReputationIDs.pollFirstEntry();
            while(e != null) {
                context.write(new IntWritable(e.getValue()), new IntWritable(e.getKey()));
                e = topReputationIDs.pollFirstEntry();
            }
            super.cleanup(context);
        }
    }

    public static class TopTenReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        private TreeMap<Integer, Integer> topReputationIDs = new TreeMap<>();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable i: values) {
                topReputationIDs.put(i.get(), key.get());
            }

            if(topReputationIDs.size() > 10) {
                topReputationIDs.pollFirstEntry();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Map.Entry<Integer, Integer> e = topReputationIDs.pollLastEntry();
            while(e != null) {
                context.write(new IntWritable(e.getValue()), new IntWritable(e.getKey()));
                e = topReputationIDs.pollLastEntry();
            }
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top count");
        job.setJarByClass(TopCount.class);

        job.setMapperClass(XmlIDMapper.class);
        job.setCombinerClass(TopTenReducer.class);
        job.setReducerClass(TopTenReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}