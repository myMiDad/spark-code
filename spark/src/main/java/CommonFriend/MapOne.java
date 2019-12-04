package CommonFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Administrator
 */
public class MapOne  extends Mapper<LongWritable, Text,Text,Text> {
    Text k = null;
    Text v = null;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        k = new Text();
        v = new Text();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取value值
        String line = value.toString();
        //切分
        String[] infos = line.split(" ", -1);
        //user
        String user = infos[0];
        String[] friends = infos[1].split(",",-1);
        //遍历循环friends
        for (String friend : friends) {
            k.set(friend);
            v.set(user);
            context.write(k,v);
        }
    }
}
