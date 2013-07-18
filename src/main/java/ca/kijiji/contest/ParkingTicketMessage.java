package ca.kijiji.contest;

public class ParkingTicketMessage {

    private String mTicket;

    public ParkingTicketMessage(String ticket) {
        mTicket = ticket;
    }

    public String getTicket() {
        return mTicket;
    }

    public boolean isLastMessage() {
        return mTicket == null;
    }
}
