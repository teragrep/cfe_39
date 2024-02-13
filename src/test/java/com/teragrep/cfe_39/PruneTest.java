package com.teragrep.cfe_39;

import com.teragrep.cfe_39.avro.SyslogRecord;
import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

// TODO: The main function that will call for pruning will know the topic name (aka. folder path). The pruning will be done in folder basis, aka. in topic basis, so tracking the topic name is not important for the MapReduce.
//  Instead the partition and offset values together with timestamp are important for pruning. The function should return a list of key-value pairs where key is the partition and value is the last offset that is outside of the given timestamp limit.
//  The pruning of old records can be called in KafkaController.java row 112. This way the records are pruned every time new ones are added. Make sure there are no concurrency issues with the HDFS writer. Most likely there is a need for pruning-controller class that will manage the folder/topic scanning etc.
public class PruneTest extends Configured implements Tool {


    // TODO: Change mapper in a way that it extracts timestamp from the input SyslogRecord.
    // TimestampMapper takes a SyslogRecord as input and outputs a key-value pair based on the input.
    public static class TimestampMapper extends Mapper<AvroKey<SyslogRecord>, NullWritable, Text, LongWritable> {
        @Override
        public void map(AvroKey<SyslogRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            Long timestamp = key.datum().getTimestamp();
            CharSequence partition = key.datum().getPartition();
            long offset = key.datum().getOffset();
            context.write(new Text(partition + "." + offset), new LongWritable(timestamp)); // TODO: Maybe change the output so the key will contain both the partition and offset (partition + "." + offset), while value will contain the timestamp. This way reduce can do the pruning with the mapper output.
        }
    }


    public static class TimestampReducer extends Reducer<Text, LongWritable, AvroKey<CharSequence>, AvroValue<Integer>> {
        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            // TODO: Filter the input (a list of key-value pairs where value is a timestamp) in a way that any value over X is filtered out of the key-value list.
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Long>(sum)); // FIXME
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MapReduceTimestampPrune <input path> <output path>");
            return -1;
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "timestamp prune");
        job.setJarByClass(PruneTest.class);

        FileInputFormat.setInputPaths(job, new Path(args[0])); // The input path should be the folder where the AVRO-files are held. setInputPaths can take either folder or file as input, not sure if using folder has the same effect as having a list of files.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(TimestampMapper.class);
        AvroJob.setInputKeySchema(job, SyslogRecord.getClassSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(TimestampReducer.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Schema.create(Schema.Type.LONG));

        return (job.waitForCompletion(true) ? 0 : 1);
    }

    // Set input folder to be the topic folder.
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PruneTest(), args); // arg1 is <input path> and arg2 is <output path>, output path should be a new HDFS folder that does not exist and input path should be the HDFS folder with AVRO-files that we have generated in tests.
        System.exit(res);
    }
}
