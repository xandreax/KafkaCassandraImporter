package esteco.ubies.importer.bsonserializators;

import esteco.ubies.importer.modelData.ConstellationMessage;
import esteco.ubies.importer.modelData.PowerMessage;
import esteco.ubies.importer.modelData.TagMessage;
import org.apache.kafka.common.serialization.Deserializer;
import org.bson.BsonBinaryReader;
import org.bson.BsonDocument;
import org.bson.BsonReader;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.Codec;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.ClassModel;
import org.bson.codecs.pojo.PojoCodecProvider;

import java.nio.ByteBuffer;
import java.util.Map;

import static com.mongodb.MongoClient.getDefaultCodecRegistry;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

public class BsonDeserializer implements Deserializer<Document> {

    @Override
    public void configure(Map map, boolean b) {
    }

    @Override
    public Document deserialize(String s, byte[] bytes) {
        String print = new String(bytes);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        ClassModel<TagMessage> tagMessageClassModel = ClassModel.builder(TagMessage.class).enableDiscriminator(true).build();
        ClassModel<PowerMessage> powerMessageClassModel = ClassModel.builder(PowerMessage.class).enableDiscriminator(true).build();
        ClassModel<ConstellationMessage> constellationMessageClassModel = ClassModel.builder(ConstellationMessage.class).enableDiscriminator(true).build();
        //message
        CodecProvider pojoCodecProvider = PojoCodecProvider.builder().register(tagMessageClassModel,
                powerMessageClassModel, constellationMessageClassModel).build();
        CodecRegistry pojoCodecRegistry = fromRegistries(getDefaultCodecRegistry(), fromProviders(pojoCodecProvider));
        BsonDocumentCodec bsonDocumentCodec = new BsonDocumentCodec(pojoCodecRegistry);
        BsonReader reader = new BsonBinaryReader(buf);
        BsonDocument bsonDocument = bsonDocumentCodec.decode(reader, DecoderContext.builder().build());
        Codec<Document> codecDocument = pojoCodecRegistry.get(Document.class);
        return codecDocument.decode(bsonDocument.asBsonReader(), DecoderContext.builder().build());
    }

    @Override
    public void close() {
    }
}
