package kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.BufferedReader;
import java.io.FileInputStream;

import java.io.InputStreamReader;
import java.util.Properties;

public class ReadTextToKafa {

    public static void main(String[] args) throws Exception {
        /**
         * 在Java中用BufferedReader读取文件中的数据
         */
        // BufferedReader包装了字节流，并且可以按指定的编码集将字节转成字符
        BufferedReader br = new BufferedReader(new InputStreamReader(
                new FileInputStream("E:\\mrdata\\exam\\input\\exam.log"),"UTF-8"));
        /**
         * kafka自带的生产数据的api
         */
        Properties props = new Properties();
        props.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<>(config);
       //读取每一行的数据
        String line =""; // 按照回车符来判断是否满一行，并用上面指定的编码集来将二进制数据转成字符串
        while((line = br.readLine())!=null) {
            //发送到kafka中
            producer.send(new KeyedMessage<String, String>("exam",line));
        }
        br.close();


    }

}


