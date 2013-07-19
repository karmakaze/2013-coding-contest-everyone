package ca.kijiji.contest;

public class ParkingTicketMessage {

    // Marks the end of processing and signals threads to stop listening for messages
    public static final ParkingTicketMessage END = new ParkingTicketMessage(null);

    private final String mTicket;

    public ParkingTicketMessage(String ticket) {
        mTicket = ticket;
    }

    public String getTicket() {
        return mTicket;
    }
}
