package ca.kijiji.contest;

import gnu.trove.map.hash.TObjectIntHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

/**
 * Process input string as array of bytes. Converting each character to 1 byte.
 * NOTE: don't support non ASCII characters.
 * <p/>
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 14:10
 */
public final class RawDataProcessor implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(RawDataProcessor.class);

    private final TObjectIntHashMap<String> results = new TObjectIntHashMap<>();

    private final BlockingQueue<RawData> dataQueue;

    private final int amountFieldIndex;
    private final int streetFieldIndex;
    private final int tagNumberFieldIndex;
    private final int infractionDateIndex;

    private int processed = 0;

    public RawDataProcessor(BlockingQueue<RawData> dataQueue, RawDataParams params) {
        this.dataQueue = dataQueue;

        this.amountFieldIndex = params.getIndex(RawDataParams.AMOUNT_FIELD_NAME);
        this.streetFieldIndex = params.getIndex(RawDataParams.STREET_FIELD_NAME);
        this.tagNumberFieldIndex = params.getIndex(RawDataParams.TAG_NUMBER_NAME);
        this.infractionDateIndex = params.getIndex(RawDataParams.DATE_OF_INFRACTION_NAME);
    }

    @Override
    public void run() {
        try {
            final RawData data = dataQueue.take();
            if (data != RawData.POISON) {
                processRawData(data);
                processed++;
                run();
            } else {
                dataQueue.put(data);
                LOG.debug("Thread {} processed {} raw data bunches", Thread.currentThread().getName(), processed);
            }

        } catch (InterruptedException e) {
            LOG.error(e.getMessage(), e);
        }
    }

    private void processRawData(RawData data) {
        final byte[] bytes = data.getData();

        int wordIndex = 0;
        int wordStart = 0;
        int amount = 0;
        byte character = 0;

        for (int i = 0; i < data.getSize(); i++) {
            character = bytes[i];

            if (character == Constants.EOL_CODE_INT) {
                wordIndex = 0;
                wordStart = i + 1;
                continue;
            }

            if (character == Constants.DATA_SEPARATOR_BYTE) {
                if (wordIndex == amountFieldIndex) {
                    amount = Constants.parseAmount(bytes, wordStart, i - 1);
                } else if (wordIndex == streetFieldIndex) {
                    final String streetName = StreetNameParser.parse(bytes, wordStart, i - 1);
                    results.adjustOrPutValue(streetName, amount, amount);
                }

                wordIndex++;
                wordStart = i + 1;

                //we can skip parsing of the tagNumber and infraction date since we know their exact size
                if (wordIndex == tagNumberFieldIndex) {
                    wordIndex++;
                    i += RawDataParams.TAG_NUMBER_LENGTH + 1;
                } else if (wordIndex == infractionDateIndex) {
                    wordIndex++;
                    i += RawDataParams.DATE_OF_INFRACTION_LENGTH + 1;
                }
            }


        }
    }

    public TObjectIntHashMap<String> getResults() {
        return results;
    }
}
