package com.flink.wudy.config;

public class KafkaProperties {
    // Kafka服务器配置
    public static final String KAFKA_BROKER_ADDRESS = "127.0.0.1:9092";
    public static final String KAFKA_CLUSTER_ADDRESS = "node1:9092,node2:9092,node3:9092";

    // 序列化器配置
    public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    // 生产者配置
    public static final String ACKS_CONFIG = "all";
    public static final String RETRIES_CONFIG = "3";
    public static final String BATCH_SIZE_CONFIG = "16384";
    public static final String LINGER_MS_CONFIG = "1";
    public static final String BUFFER_MEMORY_CONFIG = "33554432";

    // 消费者配置
    public static final String GROUP_ID = "default-group";
    public static final String ENABLE_AUTO_COMMIT = "true";
    public static final String AUTO_COMMIT_INTERVAL_MS = "1000";
    public static final String SESSION_TIMEOUT_MS = "30000";
    public static final String AUTO_OFFSET_RESET = "latest";

    // 安全配置
    public static final String SECURITY_PROTOCOL = "SASL_PLAINTEXT";
    public static final String SASL_MECHANISM = "PLAIN";

    // 主题配置
    public static final String DEFAULT_TOPIC = "default-topic";
    public static final String TEST_TOPIC = "test-topic";
    public static final String USER_SERVER_TOPIC = "user-server-topic";
    public static final String USER_SERVER_GROUP_ID = "user-server-group-id";
    public static final String ORDER_SERVER_TOPIC = "order-server-topic";
    public static final String ORDER_SERVER_GROUP_ID = "order-server-group-id";
}
