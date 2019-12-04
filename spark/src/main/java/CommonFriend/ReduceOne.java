package CommonFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * @author Administrator
 */
public class ReduceOne extends Reducer<Text, Text, Text, Text> {
    Text v = null;
    int count = 0;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        v = new Text();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        count++;
        System.out.println(count);
        System.out.println(key.toString()+"============key");
        //创建一个字符串
        StringBuffer str = new StringBuffer();
        //遍历循环values
        for (Text value : values) {
            str.append(value.toString()+",");
            System.out.println(str);
        }
        System.out.println(str);
        //写出
        v.set(str.toString());
        context.write(key,v);
    }
}
