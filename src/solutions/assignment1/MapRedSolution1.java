package solutions.assignment1;

import examples.MapRedFileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class MapRedSolution1 {

    public static class MapAccessLogs extends Mapper<LongWritable, Text, Text, LongWritable> {

        private final static LongWritable one = new LongWritable(1);
        private Text url = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString();
            // split the apache log record by space.
            String[] splits = line.split(" ");

            // 7th item in the split is the url requested.
            String path = splits[6];
            // remove any quote present in the path.
            path = path.replace("\"", "");

//            if (!path.startsWith("http")) {
//                path = "http://localhost" + path;
//            }

            // write mapping out.
            url.set(path);
            context.write(url, one);
        }
    }

    public static class ReduceAccessLogs extends Reducer<Text, LongWritable, Text, LongWritable> {
        private LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text url, Iterable<LongWritable> values, Context context) throws
                IOException, InterruptedException {

            // sum the values of the given url.
            int sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(url, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] otherArgs =
                new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MapRedSolution1 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "MapRed Solution #1");
        // set mapper class for job
        job.setMapperClass(MapAccessLogs.class);

        // set reducer and combiner classes for job.
        job.setCombinerClass(ReduceAccessLogs.class);
        job.setReducerClass(ReduceAccessLogs.class);

        // set output key and value classes for job.
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // set input format so that we get single line in mapper class.
        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        MapRedFileUtils.deleteDir(otherArgs[1]);
        int exitCode = job.waitForCompletion(true) ? 0 : 1;

        FileInputStream fileInputStream = new FileInputStream(new File(otherArgs[1] + "/part-r-00000"));
        String md5 = org.apache.commons.codec.digest.DigestUtils.md5Hex(fileInputStream);
        fileInputStream.close();

        String[] validMd5Sums = {"ca11be10a928d07204702b3f950fb353", "6a70a6176249b0f16bdaeee5996f74cb",
                "54893b270934b63a25cd0dcfd42fba64", "d947988bd6f35078131ce64db48dfad2", "3c3ded703f60e117d48c3c37e2830866"};

        for (String validMd5 : validMd5Sums) {
            System.out.println("Comparing " + md5 + " with " + validMd5);
            if (validMd5.contentEquals(md5)) {
                System.out.println("The result looks good :-)");
                System.exit(exitCode);
            }
        }
        System.out.println("The result does not look like what we expected :-(");
        System.exit(exitCode);
    }
}
