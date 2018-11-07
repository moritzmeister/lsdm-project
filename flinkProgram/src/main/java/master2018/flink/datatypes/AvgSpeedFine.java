package master2018.flink.datatypes;

import org.apache.flink.api.java.tuple.Tuple6;

public class AvgSpeedFine extends Tuple6<Long, Long, String, Integer, Integer, Double> {

    public AvgSpeedFine() {

    }

    public void setTime1(Long time1) {
        this.f0 = time1;
    }

    public void setTime2(Long time2) {
        this.f1 = time2;
    }

    public void setVid(String vid) {
        this.f2 = vid;
    }

    public void setXway(int xway) {
        this.f3 = xway;
    }

    public void setDirection(int direction) {
        this.f4 = direction;
    }

    public void setAvgSpeed(double avgSpeed) {
        this.f5 = avgSpeed;
    }

    public String toString() {
        // Returns the object attributes as a comma separated string
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(f0)).append(",");
        sb.append(Long.toString(f1)).append(",");
        sb.append(f2).append(",");
        sb.append(Integer.toString(f3)).append(",");
        sb.append(Integer.toString(f4)).append(",");
        sb.append(Double.toString(f5));

        return sb.toString();
    }
}

