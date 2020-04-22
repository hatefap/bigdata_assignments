import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {


    static final String APACHE_REGEX = "^(\\S+) (\\S+) (\\S+) " +
            "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +
            " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";

    static final Pattern pattern = Pattern.compile(APACHE_REGEX, Pattern.MULTILINE);
    static final int SESSION_BREAK = 10 * 60;


    public static Map<String, String> parseLog(String log) {
        Matcher matcher = pattern.matcher(log);
        Map<String, String> parsedLog = new HashMap<>();
        while (matcher.find()) {

            String ip = matcher.group(1);
            String time = matcher.group(4);
            String type = matcher.group(5);
            String file = matcher.group(6);
            String protocol = matcher.group(7);
            String status = matcher.group(8);

            parsedLog.put("ip", ip);
            parsedLog.put("time", time);
            parsedLog.put("type", type);
            parsedLog.put("file", file);
            parsedLog.put("protocol", protocol);
            parsedLog.put("status", status);
        }

        return parsedLog;
    }

    public static class ExtractIpAndDate extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context context) {

            try {
                Map<String, String> parsedLine = parseLog(value.toString());
                String ip = parsedLine.get("ip");
                String time = parsedLine.get("time").split(" ")[0];
                Date date = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss").parse(time);
                int year = date.getYear() + 1900;
                int month = date.getMonth();
                int day = date.getDay();
                context.write(new Text(ip + "#" + year + "#" + month + "#" + day),
                        new LongWritable(1L));
            } catch (IOException | InterruptedException | ParseException e) {
                e.printStackTrace();
            }
        }
    }



    public static class CountVisitPerDay extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            String[] parts = key.toString().split("#");
            String ip = parts[0];
            String year = parts[1];
            String month = parts[2];
            context.write(new Text(ip + "#" + year + "#" + month), new LongWritable(1));
        }
    }



    public static class DummyMap extends Mapper<LongWritable, Text, Text, LongWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            context.write(new Text(value.toString().split("\t")[0]), new LongWritable(1));
        }
    }


    public static class MonthPartitioner extends Partitioner<Text, LongWritable> {

        @Override
        public int getPartition(Text ipYearMonthDay, LongWritable time, int i) {
            return Integer.parseInt(ipYearMonthDay.toString().split("#")[2]);
        }

    }

    public static class CountVisitPerMonth extends Reducer<Text, LongWritable, Text, LongWritable> {
        public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
           String[] parts = key.toString().split("#");
            String ip = parts[0];
            String year = parts[1];
            String month = parts[2];
            long total = 0L;

            Iterator<LongWritable> iter = values.iterator();
            while(iter.hasNext()) {
                total += 1;
                iter.next();
            }

            context.write(new Text(ip + "-" + year + "\\" + month), new LongWritable(total));

        }
    }




    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = new Job(new Configuration(), "extract");
        job.setJarByClass(Main.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(ExtractIpAndDate.class);
        job.setReducerClass(CountVisitPerDay.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/log.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/o1"));
        job.waitForCompletion(true);

        Job job2 = new Job(new Configuration(false), "sort");
        job2.setJarByClass(Main.class);
        //set type of both mapper and reducer if your map produce different type specify it with setMapOutputKeyClass
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setMapperClass(DummyMap.class);
        job2.setPartitionerClass(MonthPartitioner.class);
        job2.setNumReduceTasks(12);
        job2.setReducerClass(CountVisitPerMonth.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job2, new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/o1"));
        FileOutputFormat.setOutputPath(job2,new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/o2"));
        job2.waitForCompletion(true);
    }
}
