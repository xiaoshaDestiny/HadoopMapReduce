package friends;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OneShareFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
			throws IOException, InterruptedException {
		// 1 ��ȡһ�� A:B,C,D,F,E,O
		String line = value.toString();
		// 2 �и�
		String[] fields = line.split(":");
		// 3 ��ȡ person �ͺ���
		String person = fields[0];
		String[] friends = fields[1].split(",");
		//4 д��ȥ
		for (String friend : friends) {
			//��� <���ѣ���>
			context.write(new Text(friend), new Text(person));
		}
	}
}