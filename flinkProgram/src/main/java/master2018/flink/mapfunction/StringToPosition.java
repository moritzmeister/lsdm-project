package master2018.flink.mapfunction;

import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 *  STRINGTOPOSITION CLASS
 */
public class StringToPosition implements MapFunction<String, PositionEvent> {
    @Override
    public PositionEvent map(String line) throws Exception {
        PositionEvent position = new PositionEvent(line);
        return position;
    }
}
