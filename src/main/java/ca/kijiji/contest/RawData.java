package ca.kijiji.contest;

/**
 * Container for raw bytes from the stream.
 * <p/>
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 18:32
 */
public final class RawData {
    public static final RawData POISON = new RawData(0);

    public static final int BUFF_MAIN_SIZE = 1024 * 1024;
    private static final int MAX_LINE_LENGTH = 200;
    private static final int DATA_SIZE = BUFF_MAIN_SIZE + MAX_LINE_LENGTH;

    private final byte[] data;
    private int size = 0;

    private RawData(int size){
        this.data = new byte[size];
    }

    public RawData() {
        this.data = new byte[DATA_SIZE];
    }

    public byte[] getData() {
        return data;
    }

    public int getSize() {
        return size;
    }

    public void incSize(int delta) {
        this.size += delta;
    }

}
