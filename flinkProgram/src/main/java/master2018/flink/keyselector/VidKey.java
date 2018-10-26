package master2018.flink.keyselector;

import master2018.flink.datatypes.PositionEvent;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class VidKey implements KeySelector<PositionEvent, Tuple3<String, Integer, Integer>> {

    Tuple3 key = new Tuple3;

    @Override
    public Tuple3 getKey(PositionEvent data) {
        key.f0 = data.getVid();
        key.f1 = data.getXway();
        key.f2 = data.getDirection();
        return key;
    }
}