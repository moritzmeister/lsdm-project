package master2018.flink.datatypes;

public class Accident {

    private Long time1;
    private Long time2;
    private String vid;
    private int x_way;
    private int segment;
    private int direction;
    private int position;

    public Accident() {}

    public void setTime1(Long time1) {
        this.time1 = time1;
    }

    public void setTime2(Long time2) {
        this.time2 = time2;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public void setXWay(int x_way) {
        this.x_way = x_way;
    }

    public void setSegment(int segment) { this.segment = segment; }

    public void setDirection(int direction) {
        this.direction = direction;
    }

    public void setPosition(int position) {
        this.position = position;
    }

    public String toString() {
        /* Returns the object attributes as a comma separated string */
        StringBuilder sb = new StringBuilder();

        sb.append(Long.toString(time1)).append(",");
        sb.append(Long.toString(time2)).append(",");
        sb.append(vid).append(",");
        sb.append(Integer.toString(x_way)).append(",");
        sb.append(Integer.toString(segment)).append(",");
        sb.append(Integer.toString(direction)).append(",");
        sb.append(Integer.toString(position));

        return sb.toString();
    }
}
