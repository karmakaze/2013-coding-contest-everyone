package ca.kijiji.contest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * Reads raw data from stream. Guaranteed that each bunch of data contains integer number of strings.
 * <p/>
 * User: Eugene Loykov
 * Date: 13.07.13
 * Time: 17:44
 */
public final class RawDataReader {
    private static final Logger LOG = LoggerFactory.getLogger(RawDataProcessor.class);

    private final InputStream stream;

    public RawDataReader(InputStream stream) {
        this.stream = stream;
    }

    public boolean readRawData(RawData data) throws IOException {
        int readCount = 0;
        byte[] buff = data.getData();

        readCount = stream.read(buff, 0, RawData.BUFF_MAIN_SIZE);
        data.incSize(readCount);

        if (readCount < RawData.BUFF_MAIN_SIZE) {
            return false;
        }

        // need to read a few more bytes till the end of the line
        for (int i = RawData.BUFF_MAIN_SIZE; buff[i - 1] != -1 && buff[i - 1] != Constants.EOL_CODE_INT; i++) {
            buff[i] = (byte) stream.read();
            data.incSize(1);
        }

        return true;
    }

    public RawDataParams readRawDataParams() {
        final String template;
        try {
            template = retrieveTemplate();
        } catch (IOException e) {
            LOG.error("Can't retrieve template string", e);
            return null;
        }

        final RawDataParams params = new RawDataParams();
        final String[] fields = template.split(Constants.DATA_SEPARATOR_STR);
        for (int i = 0; i < fields.length; i++) {
            params.set(fields[i], i);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Template line parsed. Street index:[{}], amount index: [{}]",
                    params.getIndex(RawDataParams.STREET_FIELD_NAME), params.getIndex(RawDataParams.AMOUNT_FIELD_NAME));
        }

        return params;
    }

    private String retrieveTemplate() throws IOException {
        LOG.debug("Parsing first line for parse template");

        final StringBuilder template = new StringBuilder();
        int character = 0;
        while ((character != -1) && (character != Constants.EOL_CODE_INT)) {
            character = stream.read();
            template.append((char) character);
        }

        final String result = template.toString();
        LOG.debug("Template: {}", result);
        return result;
    }
}
