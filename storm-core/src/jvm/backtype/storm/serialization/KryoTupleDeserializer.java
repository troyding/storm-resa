package backtype.storm.serialization;

import backtype.storm.task.GeneralTopologyContext;
import backtype.storm.tuple.MessageId;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import com.esotericsoftware.kryo.io.Input;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class KryoTupleDeserializer implements ITupleDeserializer {
    GeneralTopologyContext _context;
    KryoValuesDeserializer _kryo;
    SerializationFactory.IdDictionary _ids;
    Input _kryoInput;

    public KryoTupleDeserializer(final Map conf,
	    final GeneralTopologyContext context) {
	_kryo = new KryoValuesDeserializer(conf);
	_context = context;
	_ids = new SerializationFactory.IdDictionary(context.getRawTopology());
	_kryoInput = new Input(1);
    }

    public Tuple deserialize(byte[] ser) {
	try {
	    _kryoInput.setBuffer(ser);
	    int taskId = _kryoInput.readInt(true);
	    int streamId = _kryoInput.readInt(true);
	    String componentName = _context.getComponentId(taskId);
	    String streamName = _ids.getStreamName(componentName, streamId);
	    MessageId id = MessageId.deserialize(_kryoInput);
	    List<Object> values = _kryo.deserializeFrom(_kryoInput);
	    TupleImpl tuple = new TupleImpl(_context, values, taskId,
		    streamName, id);
	    long transferStartTime = _kryoInput.readLong(true);
	    if (transferStartTime != -1) {
		tuple.setTransferSampleStartTime(transferStartTime);
	    }
	    return tuple;
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }
}
