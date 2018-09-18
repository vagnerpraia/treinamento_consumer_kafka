package treinamento_kafka.consumer;

import java.lang.Runnable;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerRunnable implements Runnable {
    final Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());

    private CountDownLatch latch;
    private KafkaConsumer<String, String> consumer;

    public ConsumerRunnable(CountDownLatch latch, String bootstrapServers, String groupId, String autoOffsetReset, List<String> topics){
        this.latch = latch;

        // Propriedades do consumer
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);

        // Criação do consumer
        this.consumer = new KafkaConsumer<String, String>(properties);

        // Inscrição de tópico(s)
        this.consumer.subscribe(topics);
    }

    @Override
    public void run(){
        // Pesquisa novos dados
        try{
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for(ConsumerRecord<String, String> record : records){
                    logger.info(record.value());
                }
            }
        }catch(WakeupException e){
            logger.info("Consumer desligado.");
            shutdown();
        }finally{
            consumer.close();
            latch.countDown();
        }
    }

    public void shutdown(){
        consumer.wakeup();
    }
}
