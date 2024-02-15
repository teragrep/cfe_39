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

/*
The records are stored inside files that are 64MB in size and named depending on which Kafka partition offset the last stored record belongs to.
In other words the files are inside topic_name-folder and there are at least one file per partition, depending on the load size of records that are fetched from Kafka topics.

The AVRO-files that hold the records can house over 240,000 records each (at 64MB), but that is just an estimate because the record sizes vary widely.
This means that the partition offsets are not guaranteed to be same for all the files in a topic. In other words the same number of records could be distributed between 3 files on one partition and 2 files on another partition because of the different individual record sizes.
Lets take 2 topic partitions and their files as an example:

topic_name/0.25 (contains partition 0 records between offsets 0 and 25)
topic_name/0.50 (contains partition 0 records between offsets 26 and 50)
topic_name/1.35 (contains partition 1 records between offsets 0 and 35)
topic_name/1.55 (contains partition 1 records between offsets 36 and 55)

Timestamps for the record offsets are linear: record 0.1 is timed first, record 1.1 is second, 0.2 is third, 1.2 is fourth, etc.
Lets say that pruning cutoff epoch lands between records 0.30 and 1.30. This means that the file topic_name/0.25 is pruned but topic_name/1.35 is not.
topic_name/1.35 will contain records from between offsets 0 and 35. Only the records at or above offset 30 are coherent, everything under that can be considered to be garbage data if they are ever queried.

Should the file be altered so the garbage data can be removed from the file? Or should the records only be filtered out when responding to queries with result sets?
The filtering method is most likely the least resource intensive method as it can be done during the MapReduce, and the amount of garbage record shouldn't be too much. If the main function decides on the cutoff epoch, then it can also track it for the datasource function to use for filtering.
But in any case the pruning should include deleting AVRO-files that hold only outdated records that should be pruned. The handling of the leftover garbage records can be handled later.
*/

// TODO: The main function that will call for pruning will know the topic name (aka. folder path). The pruning will be done in folder basis, aka. in topic basis, so tracking the topic name is not important for the MapReduce.
//  Instead, the partition and offset values together with timestamp are important for pruning. The function should return a list of key-value pairs where key is the partition and value is the last offset that is outside of the given timestamp limit.
//  The pruning of old records can be called in KafkaController.java row 112. This way the records are pruned every time new ones are added. Make sure there are no concurrency issues with the HDFS writer. Most likely there is a need for pruning-controller class that will manage the folder/topic scanning etc.
public class PruneTest extends Configured implements Tool {
    long cutoff_epoch = 0L;
    // TODO: Change mapper in a way that it extracts timestamp from the input SyslogRecord.
    // TimestampMapper takes a SyslogRecord as input and outputs a key-value pair based on the input.
    public static class TimestampMapper extends Mapper<AvroKey<SyslogRecord>, NullWritable, Text, LongWritable> {
        @Override
        public void map(AvroKey<SyslogRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
            Long timestamp = key.datum().getTimestamp();
            CharSequence partition = key.datum().getPartition();
            long offset = key.datum().getOffset();
            context.write(new Text(partition + "." + offset), new LongWritable(timestamp)); // Changed the output so the key will contain both the partition and offset (partition + "." + offset), while value will contain the timestamp. This way reduce can do the pruning with the mapper output.
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
        if (args.length != 3) {
            System.err.println("Usage: MapReduceTimestampPrune <input path> <output path> <cutoff epoch>");
            return -1;
        }
        try {
            cutoff_epoch = Long.parseLong(args[2]);
        } catch (NumberFormatException e) {
            System.err.println("Usage: 3rd input argument should be parseable to long");
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

    // TODO: Should there be more input parameters? Like topic name, a list of file names that hold the records, etc? That would open up a lot more leeway for coding the pruning algorithm using MapReduce.

    // Set input folder to be the topic folder.
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new PruneTest(), args); // arg1 is <input path> and arg2 is <output path>, output path should be a new HDFS folder that does not exist and input path should be the HDFS folder with AVRO-files that we have generated in tests.
        System.exit(res);
    }
}
