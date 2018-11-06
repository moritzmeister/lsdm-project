package master2018.flink.datatypes;

public class AvgSpeedFine {

    private Long time1;
    private Long time2;
    private String vid;
    private int xway;
    private int direction;
    private double avgSpeed;

    public AvgSpeedFine(PositionEvent first, PositionEvent last, double avgSpeed) {
        this.time1 = first.getTime();
        this.time2 = last.getTime();
        this.vid = first.getVid();
        this.xway = first.getXway();
        this.direction = first.getDirection();
        this.avgSpeed = avgSpeed;
    }

    public String toString() {
        /* Returns the object attributes as a comma separated string */
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toString(time1)).append(",");
        sb.append(Long.toString(time2)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(xway)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Double.toString(avgSpeed));

        return sb.toString();
    }
}
