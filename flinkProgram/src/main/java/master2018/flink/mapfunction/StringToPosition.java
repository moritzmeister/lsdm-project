package master2018.flink.mapfunction;

import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.common.functions.MapFunction;

/**
 *  STRINGTOPOSITION CLASS
 *
 *  A map function that maps an input string line from the file to a PositionEvent.
 *  Not being used in the current solution. But in principle it could be used to parallelize this step.
 */
public class StringToPosition implements MapFunction<String, PositionEvent> {
    @Override
    public PositionEvent map(String line) throws Exception {
        PositionEvent position = new PositionEvent(line);
        return position;
    }
}
