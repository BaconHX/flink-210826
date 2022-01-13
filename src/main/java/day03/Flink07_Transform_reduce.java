package day03;

import bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_Transform_reduce {
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

        //5.求水位之和
        keyedStream.reduce(new ReduceFunction<WaterSensor>() {
            /**
             * 每个Key的第一条数据不进入reduce方法
             * @param value1 value1为上一次聚合后的结果
             * @param value2 value2位当前的数据
             * @return
             * @throws Exception
             */
            @Override
            public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                System.out.println("reduce...");
                return new WaterSensor(value1.getId(),value2.getTs(),value1.getVc()+value2.getVc());
            }
        }).print();

        env.execute();

    }
}

/*
注意:
        1、 一个分组的第一条数据来的时候，不会进入reduce方法。
        2、 输入和输出的 数据类型，一定要一样。*/
