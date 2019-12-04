package CommonFriend;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Administrator
 */
public class TwoDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //conf
        Configuration conf = new Configuration();
        //job
        Job job = Job.getInstance(conf);
        //jar
        job.setJarByClass(TwoDriver.class);

        //map和reduce类
        job.setMapperClass(TwoMap.class);
        job.setReducerClass(TwoReduce.class);

        //设置map的输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //设置最终的输出类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //设置输入和输出文件
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //启动程序
        boolean b = job.waitForCompletion(true);
        System.exit(b ? 0 : 1);
    }
}
