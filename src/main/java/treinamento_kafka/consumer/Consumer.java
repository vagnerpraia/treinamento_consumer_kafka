package treinamento_kafka.consumer;

import java.lang.Runnable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {
    public static void main(String[] args){
        final Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

        // Get the managed bean for the thread system of the Java virtual machine.
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();

        // Get the current number of live threads including both daemon and non-daemon threads.
        Integer threadCount = bean.getThreadCount();

        CountDownLatch latch = new CountDownLatch(threadCount);
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "teste_consumer";
        String autoOffsetReset = "earliest";
        List<String> topics = Arrays.asList("teste");

        final Runnable consumerRunnable = new ConsumerRunnable(latch, bootstrapServers, groupId, autoOffsetReset, topics);

        Thread consumer = new Thread(consumerRunnable);
        consumer.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutdown();
        }));

        try{
            latch.await();
        }catch(InterruptedException e){
            logger.error("Aplicação foi enterrompida.", e);
        }finally{
            logger.info("Close");
        }
    }
}
