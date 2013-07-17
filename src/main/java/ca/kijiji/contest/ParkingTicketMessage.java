package ca.kijiji.contest;

public class ParkingTicketMessage {

    private String[] mTicketCols;

    public ParkingTicketMessage(String[] ticketCols) {
        mTicketCols = ticketCols;
    }

    public String[] getTicketCols() {
        return mTicketCols;
    }

    public boolean isLastMessage() {
        return mTicketCols == null;
    }
}
