package ca.kijiji.contest;

import java.util.Arrays;
import java.util.StringTokenizer;

/**
 * Created with IntelliJ IDEA.
 * User: darren
 * Date: 31/07/13
 * Time: 11:38 PM
 * To change this template use File | Settings | File Templates.
 */
public class WorkerThread implements Runnable {
    private ParkingTicketsStats parent;
    private int size;
    private int offset;
    private int currentColumn;
    private int currentFare;
    private States currentState;

    private enum States {
        IRRELEVANT,
        FARE,
        STREET_NAME,
        DONE
    }

    public WorkerThread(int threadId, int numberOfThreads, ParkingTicketsStats parent){
        this.currentState = States.IRRELEVANT;
        this.currentColumn = 0;

        this.parent = parent;
        this.size =  (parent.data.length-1) / numberOfThreads;
        this.offset = size * threadId;
        //Skip first line of CSV
        if (threadId == 0)
            this.offset = 1;
    }

    public void run() {
        for (int i = offset; i < offset + size;) {
            i = stateParseAt(i);
        }
    }


    //Takes in index of data and tries to parse it
    //by what state it is in. Returns index parsing finished at
    //Column 4 is fine amount Column 7 is street (starting from column 0)
    private int stateParseAt(int index) {
        byte thisByte = parent.data[index];
        //State machine always resets on carriage return (ASCII 13dec)
        if (thisByte == (byte)13){
            resetState();
            return index + 1;
        }

        switch (currentState) {
            //Always assume current data is irrelevant, this may be incorrect
            //sometimes but it is a good assumption. If we jump halfway into a line
            //or a line is malformed we will reset the parser at the newline
            case IRRELEVANT:
                // "," = ASCII 44dec
                if (thisByte == 44) {
                    currentColumn++;
                    if (currentColumn == 4) {
                        currentState = States.FARE;
                    } else if (currentColumn == 7) {
                        currentState = States.STREET_NAME;
                    }
                }
                return index + 1;
            case FARE:
                currentColumn++;
                currentState = States.IRRELEVANT;
                return readFare(index);
            case STREET_NAME:
                currentColumn++;
                currentState = States.DONE;
                return readStreet(index);
            default:
                //Either done, or state broke in some way, either way find next line and reset
                resetState();
                return findNextLineIndex(index);
        }
    }

    private int readFare(int index) {
        byte[] buffer;
        int end;
        int start = index;

        byte currentByte = parent.data[index];

        while (currentByte != 13 && currentByte != 44) {
            index++;
            currentByte = parent.data[index];
        }

        //found new line while trying to parse fare, reset to next line
        if (currentByte == 13) {
            resetState();
            return index + 1;
        } else {
            end = index;
            buffer = Arrays.copyOfRange(parent.data, start, end);

            try {
                currentFare = Integer.parseInt(new String(buffer));
            } catch (NumberFormatException numberFormatException) {
                currentFare = 0;
            }
            return end + 1;
        }
    }

    private int readStreet(int index) {
        byte[] buffer;
        int end;
        int start = index;

        byte currentByte = parent.data[index];

        while (currentByte != 13 && currentByte != 44) {
            index++;
            if (index + 1 != parent.data.length) {
                currentByte = parent.data[index];
            }
        }

        //found new line while trying to parse fare, reset to next line
        if (currentByte == 13) {
            resetState();
            return index + 1;
        } else {
            end = index;
            buffer = Arrays.copyOfRange(parent.data, start, end);
            String key = cleanUp(new String(buffer));
            if (parent.streetProfitMap.containsKey(key))
                currentFare = currentFare + parent.streetProfitMap.get(key);

            parent.streetProfitMap.put(key, currentFare);
            return end + 1;
        }
    }

    private String cleanUp(String in) {
        StringTokenizer tokenizer = new StringTokenizer(in);

        String result = "";
        String space = " ";

        //don't check for suffix if count is too low to find street name
        int count = 0;
        while (tokenizer.hasMoreTokens()){
            String temp = tokenizer.nextToken();
            count++;

            //ignore street number
            if (!temp.matches("[0-9]+")){
                if (count > 2) {
                    temp = temp.replaceAll("[[^A-Z.]]", "");
                    //Check if name over and we are at suffix
                    switch (temp) {
                        case "ST":
                        case "DR":
                        case "AVE":
                        case "CT":
                        case "CIR":
                        case "WAY":
                        case "CRT":
                        case "TER":
                        case "BLVD":
                            return result;
                        default:
                            if (result == ""){
                                result = result.concat(temp);
                            } else {
                                result = result.concat(space).concat(temp);
                            }
                            break;
                    }
                } else {
                    result = result.concat(temp);
                }
            }
        }
        return result;
    }

    private int findNextLineIndex(int index) {
        byte currentByte = parent.data[index];

        while (currentByte != 13) {
            index++;
            currentByte = parent.data[index];
        }

        return index + 1;
    }

    private void resetState(){
        currentState = States.IRRELEVANT;
        currentColumn = 0;
        currentFare = 0;
    }
}