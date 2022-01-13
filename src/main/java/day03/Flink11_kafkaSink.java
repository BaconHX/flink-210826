package day03;

import bean.WaterSensor;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class Flink11_kafkaSink {
    public static void main(String[] args) throws Exception {
        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口读取数据
        DataStreamSource<String> streamSource = env.socketTextStream("hadoop102", 9000);

        //TODO 3.将数据转换成javaBean //注意数据健壮性,进来的数据必须切分后要大于等于三段,这样指针才不会越界
        SingleOutputStreamOperator<String> map = streamSource.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] words = value.split(" ");
                WaterSensor waterSensor = new WaterSensor(words[0], Long.parseLong(words[1]), Integer.parseInt(words[2]));

                return JSONObject.toJSONString(waterSensor);
            }
        });

        //TODO 4.将数据写入kafkaSink
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>("topic_sensor", new SimpleStringSchema(), properties);


        map.addSink(kafkaProducer);

        env.execute();
    }
}

