package com.fs.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaUtil {

    private static Properties prop = null;
    private static final String CONFIG_FILE_PATH = "kafka.properties";

    static {
        InputStream in = null;
        try {
            prop = new Properties();
            in = KafkaUtil.class.getClassLoader().getResourceAsStream(CONFIG_FILE_PATH);
            prop.load(in);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                assert in != null;
                in.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }


    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId) {
        return new FlinkKafkaConsumer<String>(topic, new SimpleStringSchema(), prop);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<String>(topic, new SimpleStringSchema(), prop);
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(String topic, KafkaSerializationSchema<T> kafkaSerializationSchema) {
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, 5 * 60 * 1000 + "");
        return new FlinkKafkaProducer<T>(topic,
                kafkaSerializationSchema,
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static String getKafkaDDLOld(String topic, String groupId,String bootstrapServers,String zkServer,String updateMode,String startupMode,Boolean deriveSchema) {
        return String.format("WITH (\n" +
                "  'connector.type' = 'kafka',\n" +
                "  'connector.version' = 'universal',\n" +
                "  'connector.topic' = '%s',\n" +
                "  'connector.properties.zookeeper.connect' = '%s',\n" +
                "  'connector.properties.bootstrap.servers' = '%s',\n" +
                "  'connector.properties.group.id' = '%s',\n" +
                "  'update-mode' = '%s',\n" +
                "  'connector.startup-mode' = '%s',\n" +
                "  'format.type' = 'json',\n" +
                "  'format.derive-schema' = '%b'" +
                ")",topic,zkServer,bootstrapServers,groupId,updateMode,startupMode,deriveSchema);

    }


    public static String getKafkaDDL(String topic, String groupId,String bootstrapServers,String startupMode,String format,String options) {

        StringBuilder builder = new StringBuilder();
        StringBuilder ddl = builder.append("WITH (")
                .append("'connector' = 'kafka',")
                .append("'topic' = '%s',")
                .append("'properties.bootstrap.servers' = '%s',")
                .append("'properties.group.id' = '%s',")
                .append("'scan.startup.mode' = '%s',")
                .append("'format' = '%s'")
                .append(" %s")
                .append(")");
        return String.format(ddl.toString(),topic,bootstrapServers,groupId,startupMode,format,options);
/*        return String.format("WITH (\n" +
                "  'connector' = 'kafka',\n" +
                "  'topic' = '%s',\n" +
                "  'properties.bootstrap.servers' = '%s',\n" +
                "  'properties.group.id' = '%s',\n" +
                "  'scan.startup.mode' = '%s',\n" +
                "  'format' = '%s'\n" +
                "   %s" +
                ")",topic,bootstrapServers,groupId,startupMode,format,options);*/
    }

    public static String getKafkaDDL(String topic, String groupId,String startupMode,String format,String options){
        return getKafkaDDL(topic,groupId,prop.getProperty("bootstrap.servers"),startupMode,format,options);
    }

    /**
     * When you use "options" parameter, don't forget to add ',' in front of the value
     */
    public static String getKafkaJsonDDL(String topic, String groupId,String startupMode,String options){
        return getKafkaDDL(topic,groupId,startupMode,"json",options);
    }

    /**
     * @param startupMode   ['earliest-offset', 'latest-offset', 'group-offsets', 'timestamp' , 'specific-offsets']
     * @return
     */
    public static String getKafkaJsonDDL(String topic, String groupId,String startupMode){
        return getKafkaDDL(topic,groupId,startupMode,"json","");
    }
}
