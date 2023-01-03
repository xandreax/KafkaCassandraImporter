package esteco.ubies.importer;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import esteco.ubies.importer.propertiesHelper.PropertyReadHelper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * MongoDB Importer which consumes data elements from Kafka, converts them to MongoDB documents and adds them to the corresponding MongoDB collection.
 */
public class CassandraImporter implements Runnable {

    private final AtomicBoolean stopped = new AtomicBoolean(false);

    Consumer<Long, Document> kafkaConsumer;

    private final String uuid_registration;

    private static final String queryTag = "insert into reg_data_test (registration_id, protocol, timestamp, id, " +
            "temp, batt, position, hposition, acc, gyro) values (?,?,?,?,?,?,?,?,?,?)";

    private static final String queryPower = "insert into reg_data_test (registration_id, protocol, " +
            "timestamp, A1, A2, A3, A4, A5, A6, A6, A7) values (?,?,?,?,?,?,?,?,?,?)";

    private static final String queryConstellation = "insert into config_data (registration_id, " +
            "protocol, timestamp, A0, A1, A2, A3, A4, A5, A6, A6, A7) values (?,?,?,?,?,?,?,?,?,?,?)";
    /*
     * Slf4j logger
     */
    private static final Logger logger = LoggerFactory.getLogger(CassandraImporter.class);

    /**
     * Poll timeout
     */
    private static long pollTimeout;

    /*
     *  Name of the Kafka Topic
     * */
    private static String topic;

    /*
     * MongoDB database
     * */
    private final CqlSession session;

    private final String dataCenter;

    /*
     * Broker list
     */
    private final String brokerList;

    /*
     * Group id
     * */
    private final String groupIdPrefix;

    /**
     * StreamImporter constructor.
     *
     * @param properties Properties
     */
    public CassandraImporter(Properties properties, String uuid_registration) {
        logger.info("Read properties");
        pollTimeout = PropertyReadHelper.readLongOrDie(properties, "kafka.pollTimeout");
        brokerList = PropertyReadHelper.readStringOrDie(properties, "kafka.brokerList");
        groupIdPrefix = PropertyReadHelper.readStringOrDie(properties, "kafka.groupIdPrefix");
        String keySpace = PropertyReadHelper.readStringOrDie(properties, "cassandra.keyspace");
        this.dataCenter = PropertyReadHelper.readStringOrDie(properties, "cassandra.datacenter");
        topic = PropertyReadHelper.readStringOrDie(properties, "kafka.topic");
        logger.info("Initializing KafkaConsumer");
        logger.info("KafkaConsumer initialized");
        logger.info("Initialize Cassandra");
        session = CqlSession.builder().withLocalDatacenter(dataCenter).withKeyspace(keySpace).build();
        logger.info("Cassandra initialized");
        this.uuid_registration = uuid_registration;
    }

    @Override
    public void run() {

        logger.info("Start consumption loop");
        KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory(brokerList, groupIdPrefix);
        kafkaConsumer = KafkaConsumerFactory.subscribe(topic);

        try {
            while (!stopped.get()) {
                ConsumerRecords<Long, Document> records = kafkaConsumer.poll(Duration.ofMillis(pollTimeout));
                for (ConsumerRecord<Long, Document> record : records) {
                    Document msg = record.value();
                    msg.append("registration_id", uuid_registration);

                    if (msg.containsKey("tag")) {
                        Document nested_msg = msg.get("tag", Document.class);
                        BsonTimestamp timestamp = nested_msg.get("timestamp",
                                BsonTimestamp.class);
                        System.out.println(timestamp);
                        if (timestamp != null) {
                            //msg.append("datatype", "tag");
                            PreparedStatement prepared = session.prepare(queryTag);
                            BoundStatement bound = prepared.bind(uuid_registration, nested_msg.get("protocol"),
                                    timestampToDate(timestamp), nested_msg.get("id"),
                                    nested_msg.get("temp"), nested_msg.get("batt"), nested_msg.get("position"),
                                    nested_msg.get("hposition"), nested_msg.get("acc"), nested_msg.get("gyro"));
                            insertBound(bound);
                        }
                    }
                    if (msg.containsKey("power")) {
                        Document nested_msg = msg.get("power", Document.class);
                        BsonTimestamp timestamp = nested_msg.get("timestamp",
                                BsonTimestamp.class);
                        System.out.println(timestamp);
                        if (timestamp != null) {
                            //msg.append("datatype", "power");
                            PreparedStatement prepared = session.prepare(queryPower);
                            BoundStatement bound = prepared.bind(uuid_registration, nested_msg.get("protocol"),
                                    timestampToDate(timestamp), nested_msg.get("A1"),
                                    nested_msg.get("A2"), nested_msg.get("A3"), nested_msg.get("A4"),
                                    nested_msg.get("A5"), nested_msg.get("A6"), nested_msg.get("A7"));
                            insertBound(bound);
                        }
                    }
                    if (msg.containsKey("constellation")) {
                        Document nested_msg = msg.get("constellation", Document.class);
                        BsonTimestamp timestamp = nested_msg.get("timestamp",
                                BsonTimestamp.class);
                        System.out.println(timestamp);
                        if (timestamp != null) {
                            //msg.append("datatype", "constellation");
                            PreparedStatement prepared = session.prepare(queryConstellation);
                            BoundStatement bound = prepared.bind(uuid_registration, nested_msg.get("protocol"),
                                    timestampToDate(timestamp), nested_msg.get("A0"), nested_msg.get("A1"),
                                    nested_msg.get("A2"), nested_msg.get("A3"), nested_msg.get("A4"),
                                    nested_msg.get("A5"), nested_msg.get("A6"), nested_msg.get("A7"));
                            insertBound(bound);
                        }
                    }
                    kafkaConsumer.commitSync();
                }
            }
        } catch (WakeupException we) {
            logger.info("wake up exception");
            if (!stopped.get())
                throw we;
        } finally {
            kafkaConsumer.close();
        }
    }

    public void stopImporter() {
        stopped.set(true);
        kafkaConsumer.wakeup();
        logger.info("STOP SENT");
    }

    private void insertBound(BoundStatement bound) {
        try (session) {
            session.execute(bound);
            logger.info("Inserted row");
        } catch (Exception e) {
            logger.info(e.getLocalizedMessage());
        }
    }

    private static Instant timestampToDate(BsonTimestamp timestamp) {
        int seconds = timestamp.getTime();
        int inc = timestamp.getInc();
        System.out.println(seconds + "   " + inc);
        //if timestamp precision is nanoseconds
        //long millisecond = ((seconds * 1000000L) + inc) / 1000;
        //if timestamp precision is millieseconds
        long millisecond = ((seconds * 1000L) + inc);
        System.out.println(millisecond);
        Instant date = Instant.from(Instant.ofEpochMilli(millisecond).atZone(ZoneId.systemDefault()));
        System.out.println(date);
        return date;
    }
}
