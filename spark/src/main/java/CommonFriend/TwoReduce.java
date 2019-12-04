package CommonFriend;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Administrator
 */
public class TwoReduce extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //创建一个string
        StringBuffer friends = new StringBuffer();
        for (Text value : values) {
            friends.append(value.toString()).append(",");
        }
        context.write(key,new Text(friends.toString()));
    }
}
