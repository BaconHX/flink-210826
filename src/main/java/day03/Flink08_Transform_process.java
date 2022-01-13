package day03;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;

public class Flink08_Transform_process {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9999);

        //TODO 3.利用process实现flatmap算子可实现的效果,将数据按照空格切分,组成tuple2元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordtoOne = streamSource.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordtoOne.keyBy(0);

        //4.利用process实现sum可实现的效果
        keyedStream.process(new KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String,Integer>>() {
            //自定义一个累加器
            private HashMap<String,Integer> count = new HashMap<>();
            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                if (count.containsKey(value.f0)) {
                    //如果不是第一次来,则累加上一次的结果
                    count.put(value.f0, value.f1 + count.get(value.f0));
                }else {
                    //如果不是第一次来,则把自己存到map集合中
                    count.put(value.f0,value.f1);
                }
                out.collect(Tuple2.of(value.f0,count.get(value.f0)));
            }
        }).print();

        env.execute();
    }
}
