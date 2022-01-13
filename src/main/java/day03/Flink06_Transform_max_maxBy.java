package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink06_Transform_max_maxBy {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.将数据转换成javaBean
        SingleOutputStreamOperator<WaterSensor> map = streamSource.map(new MapFunction<String, WaterSensor>() {
            @Override
            public WaterSensor map(String value) throws Exception {
                String[] words = value.split(" ");
                return new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));
            }
        });

        //4.对相同的元素进行聚合
        KeyedStream<WaterSensor, Tuple> keyedStream = map.keyBy("id");

        //5.求最大的水位
        SingleOutputStreamOperator<WaterSensor> max = keyedStream.maxBy("vc",false);

        max.print();

        env.execute();
    }
}

/*注意:
        滚动聚合算子： 来一条，聚合一条
        1、聚合算子在 keyby之后调用，因为这些算子都是属于 KeyedStream里的
        2、聚合算子，作用范围，都是分组内。 也就是说，不同分组，要分开算。
        3、max、maxBy的区别：
        max：取指定字段的当前的最大值，如果有多个字段，其他非比较字段，以第一条为准
        maxBy：取指定字段的当前的最大值，如果有多个字段，其他字段以最大值那条数据为准；
        如果出现两条数据都是最大值，由第二个参数决定： true => 其他字段取 比较早的值； false => 其他字段，取最新的值*/
