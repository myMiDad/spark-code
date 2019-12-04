package CommonFriend;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;

/**
 * @author Administrator
 */
public class TwoMap extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        //获取value值
        String line = value.toString();
        //切分
        String[] infos = line.split("\t", -1);
        String friend = infos[0];
        String[] users = infos[1].substring(0, infos[1].length() - 1).split(",", -1);
        Arrays.sort(users);
        //创建list存放用户配对
        LinkedList<String> list = new LinkedList<String>();
        for (int i = 0; i < users.length - 1; i++) {
            for (int j = i + 1; j <users.length;j++){
               list.add(users[i]+"-"+users[j]);
            }
        }
        //遍历循环list
        for (String s : list) {
            context.write(new Text(s),new Text(friend));
        }
    }
}
