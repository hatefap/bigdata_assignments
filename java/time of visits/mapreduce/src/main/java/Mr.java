import java.io.*;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;

import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.util.*;

public class Mr extends Configured  {

    static final String APACHE_REGEX = "^(\\S+) (\\S+) (\\S+) " +
            "\\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+)" +
            " (\\S+)\\s*(\\S+)?\\s*\" (\\d{3}) (\\S+)";

    static final Pattern pattern = Pattern.compile(APACHE_REGEX, Pattern.MULTILINE);
    static final int SESSION_BREAK = 30 * 60;


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

    public static class ExtractIpAndDate extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) {

            try {
                Map<String, String> parsedLine = parseLog(value.toString());
                String ip = parsedLine.get("ip");
                String time = parsedLine.get("time").split(" ")[0];
                context.write(new Text(ip), new Text(time));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static class MonthPartitioner extends Partitioner<Text, Text>{

        @Override
        public int getPartition(Text ip, Text time, int i) {

            try {
                return new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss").parse(time.toString()).getMonth();
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }



    public static class DummyMap extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) {

            String[] tmp = value.toString().split("\t");
            String k = tmp[0];
            String v = tmp[1];
            try {
                context.write(new Text(k), new Text(v));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }

    }



    public static class CountVisitPerDay extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long result = 0;
            try {
                List<Date> v = new ArrayList<>();
                for (Text value : values) {
                    v.add(new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss").parse(value.toString()));
                }
                v.sort(Date::compareTo);
                Date firstOfSession = v.get(0);
                for (int i = 1; i < v.size(); i++){
                    if ((v.get(i).getTime() - v.get(i - 1).getTime())/1000 >= SESSION_BREAK){
                        result += (firstOfSession.getTime() - v.get(i - 1).getTime()) / 1000*60;
                        firstOfSession = v.get(i);
                    }
                }
                context.write(new Text("dummy"), new Text(key + "#" + result));
        } catch (IOException | InterruptedException | ParseException e){
                e.printStackTrace();
            }
        }
    }


    private static class IpTime implements Comparable<IpTime> {
        public String ip;
        public float time;

        public IpTime(String ip, float time) {
            this.ip = ip;
            this.time = time;
        }

        @Override
        public int compareTo(IpTime that) {
            return (int) (this.time - that.time);
        }

        public String toString(){
            return this.ip + this.time;
        }
    }

    public static class SortResult extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<IpTime> result = new ArrayList<>();
            for (Text value : values) {
                String[] parts = value.toString().split("#");
                result.add(new IpTime(parts[0], Float.parseFloat(parts[1])));
            }
            Collections.sort(result);

            context.write(null, new Text(StringUtils.join(",", result)));
        }
    }


    public static void main(String[] args) throws Exception {
        Job job = new Job(new Configuration(), "extract");
        job.setJarByClass(Mr.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(ExtractIpAndDate.class);
        job.setPartitionerClass(MonthPartitioner.class);
        job.setReducerClass(CountVisitPerDay.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setNumReduceTasks(12);
        job.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job, new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/log.txt"));
        FileOutputFormat.setOutputPath(job,new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/x"));
        job.waitForCompletion(true);

        Job job2 = new Job(new Configuration(false), "sort");
        job2.setJarByClass(Mr.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(DummyMap.class);
        job2.setReducerClass(SortResult.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);


        FileInputFormat.setInputPaths(job2, new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/x"));
        FileOutputFormat.setOutputPath(job2,new Path("hdfs://sandbox.hortonworks.com:8020/user/maria_dev/bigdata1/xx"));
        job2.waitForCompletion(true);


    }
}
