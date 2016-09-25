/**
 * ID2221 - Lab1 - TopCount
 * Bampi Roberto - <bampi@kth.se>
 * Buso Fabio - <buso@kth.se>
 *
 */
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

        /*
         * The map function takes lines of the XML file as input.
         * It then parses the rows starting with <row and extracts the id and reputation values and converts them into integers.
         * It puts the values into a TreeMap with key the reputation and value the user id.
         * When the map grows over size 10, the smallest value is dropped.
         */
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
                exc.printStackTrace();
                return;
            }

            if(topReputationIDs.size() > 10) {
                topReputationIDs.pollFirstEntry();
            }

        }

        /*
         * The cleanup writes the ten top IDs by reputation and writes them as key and value into the context
         */
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

        /*
         * The reduce function receives as input a pair (userid, list(reputation)) and put it into a TreeMap.
         * Given that the user id is unique in the file there is only one reputation value.
         * If the TreeMap grows over size 10, the entry with the smallest reputation is dropped
         * The approach only works because we configure the job to have only one instance of the reducer.
         */
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for(IntWritable i: values) {
                topReputationIDs.put(i.get(), key.get());
            }

            if(topReputationIDs.size() > 10) {
                topReputationIDs.pollFirstEntry();
            }
        }

        /*
         * The cleanup writes the ten top IDs by reputation and writes them as key and value into the context
         */
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

        // Configure the job to only have on reducer
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
