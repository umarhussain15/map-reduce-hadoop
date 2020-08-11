package solutions.assignment2;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import examples.MapRedFileUtils;

public class MapRedSolution2 {

    public static class MapTaxiLogs extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Text mapKey = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            // if its the header of csv file ignore it.
            if (line.startsWith("VendorID")) {
                return;
            }
            //split the line with delimiter of csv file ,
            String[] items = line.split(",");

            // extract start date and end date string.
            String start = items[1];
            String end = items[2];

            try {
                // Create date objects using the format of date string.
                Date startDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(start);
                Date endDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(end);

                // create calendar from the date object.
                Calendar startC = Calendar.getInstance();
                startC.setTime(startDate);
                Calendar endC = Calendar.getInstance();
                endC.setTime(endDate);

                // extract hour and ampm value from start date calendar.
                int hour = startC.get(Calendar.HOUR) == 0 ? 12 : startC.get(Calendar.HOUR);
                String ampm = startC.get(Calendar.AM_PM) == Calendar.AM ? "am" : "pm";

                // write mapping if start and end calendar have same hour:
                if (startC.get(Calendar.HOUR) == endC.get(Calendar.HOUR)) {
                    mapKey.set(hour + ampm);
                    context.write(mapKey, one);
                } else {
                    // if start and end hour are different:
                    mapKey.set(hour + ampm);
                    context.write(mapKey, one);
                    // write
//                        hour = endC.get(Calendar.HOUR) == 0 ? 12 : endC.get(Calendar.HOUR);
//                        ampm = endC.get(Calendar.AM_PM) == Calendar.AM ? "am" : "pm";
////                    hs = hour < 10 ? "0" + hour : "" + hour;
//                        hs = "" + hour;
//                        mapKey.set(hs + ampm);
//                        context.write(mapKey, one);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

    }

    public static class ReduceTaxiLogs extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution2 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapRed Solution #2");
        // set mapper class for job
        job.setMapperClass(MapTaxiLogs.class);

        // set reducer and combiner classes for job.
        job.setCombinerClass(ReduceTaxiLogs.class);
        job.setReducerClass(ReduceTaxiLogs.class);

        // set output key and value classes for job.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set input format so that we get single line in mapper class.
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1] + "/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();

        String[] validMd5Sums = {"03357cb042c12da46dd5f0217509adc8", "ad6697014eba5670f6fc79fbac73cf83", "07f6514a2f48cff8e12fdbc533bc0fe5",
                "e3c247d186e3f7d7ba5bab626a8474d7", "fce860313d4924130b626806fa9a3826", "cc56d08d719a1401ad2731898c6b82dd",
                "6cd1ad65c5fd8e54ed83ea59320731e9", "59737bd718c9f38be5354304f5a36466", "7d35ce45afd621e46840627a79f87dac"};

        for (String validMd5 : validMd5Sums) {
//            System.out.println("Comparing " + md5 + " with " + validMd5);
            if (validMd5.contentEquals(md5)) {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}
