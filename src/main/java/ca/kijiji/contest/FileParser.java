/**
 * Kijiji programming contest July 2013:
 * http://kijijiblog.ca/so-you-think-you-can-code-eh/
 * 
 * Author: Yuan (yuan.java at gmail.com)
 * 
 * Licensed under Apache License v2.0
 *  
 */

package ca.kijiji.contest;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

public class FileParser {

	// the size of the buffer for reading file
	private static final int FILE_BUFFER_SIZE = 64 * 1024;

	private Map<String, Integer> ticketsStats = null;
	private InputStream inStream = null;
	private byte[] fileBuffer = new byte[FILE_BUFFER_SIZE];

	// local HashMap used to store the results temporarily
	private Map<String, Integer> ticketsStatsInThread = new HashMap<String, Integer>(12800);

	//private int nBuffers = 0;		// number of times of calling read()
	private int nBytes = 0;			// number of bytes returned by read()
	private int idx = 0;			// the index of the buffer.
	
	private char[] locationName = new char[64];		// to store the name of location2
	private int idxNameStart = 0;	// the starting index of the name of location2.
	private int idxNameEnd = 0;		// the ending index of the name of location2.
	
	private static final byte BYTE_COMMA = (byte) ',';
	private static final byte BYTE_NEWLINE = (byte) '\n';
	private static final byte BYTE_SPACE = (byte) ' ';

	
	public FileParser(InputStream inStream, Map<String, Integer> ticketsStats) {
		this.inStream = inStream;
		this.ticketsStats = ticketsStats;
	}
	
	// get next buffer from the input stream, return the actual length of data.
	public int nextBuffer() {
		try {
			nBytes = 0;
			nBytes = inStream.read(fileBuffer, 0, FILE_BUFFER_SIZE);
			if (nBytes == -1) {				
				return -1;		// end of file
			}
			
			//nBuffers++;
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return nBytes;
	}
	
	// parse the CSV file line by line for each buffer read from file
	public int parseFile() {
		do {
			// get a buffer of data from file
			nBytes = nextBuffer();
			
			if (nBytes == -1) {
				// before exiting the function, merge the result of the local HashMap into the SortedMap.
				synchronized (FileParser.class) {
					for (Iterator<Entry<String, Integer>> it = ticketsStatsInThread.entrySet().iterator(); it.hasNext();) {
						Entry<String, Integer> entry = (Entry<String, Integer>) it.next();
						Integer v = ticketsStats.get(entry.getKey());
						if (v == null) {
							ticketsStats.put(entry.getKey(), entry.getValue());
						} else {
							ticketsStats.put(entry.getKey(), entry.getValue() + v);
						}
					}					
				}
				
				return 0;
			}
			
			idx = 0;
			
			// start to parse the csv data line by line			
			while (idx < nBytes) {
				// ignore the first line of the buffer, because the start of buffer might be anywhere in the file.
				// OR ignore the characters left after parsing the name of location2 in the last line
				if (fileBuffer[idx++] != BYTE_NEWLINE) {
					continue;
				}
				
				// skip the following characters, since they are fixed length and have no use.
				// e.g. ***98804,20120101,3
				// tag_number_masked, date_of_infraction, and the first char of infraction_code
				idx += 19;
					
				// skip to the next ','
				while (idx < nBytes) {
					if (fileBuffer[idx++] == BYTE_COMMA) {
						break;
					}
				}
				
				// skip infraction_description
				// here we make a guess that the shortest length of this column is 14.
				idx += 14;
				// skip to the next ',' which is the end of infraction_description
				while (idx < nBytes) {
					if (fileBuffer[idx++] == BYTE_COMMA) {
						break;
					}
				}
				
				// get the fine amount
				int fineAmount = 0; 
				while (idx < nBytes) {
					if (fileBuffer[idx++] == BYTE_COMMA) {
						break;
					}
					// compute the fine amount
					fineAmount = fineAmount * 10 + ((char)fileBuffer[idx - 1] - '0');
				}
				
				// skip time_of_infraction. assume the shortest length of this column is 1.
				idx++;
				// skip to the next ','
				while (idx < nBytes) {
					if (fileBuffer[idx++] == BYTE_COMMA) {
						break;
					}
				}				

				// skip location1
				// skip to the next ','
				while (idx < nBytes) {
					if (fileBuffer[idx++] == BYTE_COMMA) {
						break;
					}
				}				
				
				// =============== start to parse location2 ===============
				// To avoid frequently function calling when parsing each location2, 
				// here we DO NOT use the state machine. 
				// we parse the location2 within this function byte by byte.
				
		        // NUMBER (optional) = digits, ranges of digit (e.g. 1531-1535), letters, characters like ! or ? or % or /
		        // NAME (required) = the name you need to extract, mostly uppercase letters, sometimes spaces (e.g. ST CLAIR), rarely numbers (e.g. 16TH)
		        // SUFFIX (optional) = the type of street such as ST, STREET, AV, AVE, COURT, CRT, CT, RD ...
		        // DIRECTION (optional) = one of EAST, WEST, E, W, N, S
				
				/* Here are some typical values:
				 ?114 FOLLIS AVE, 2? EATON AVE, 3?6 EGLINTON AVE W
				 6!2 MARKHAM ST, 1! BRUNEL CRT 
				 322 1/2 QUEEN ST W, 10/20/30 FASHION ROSEWAY, 47-1/2 LEE AVE, 47 1/2 LEE AVENUE,
				 TTC LOT SUBWAY CR KIPLING N/2
				 09.217 SHEFFER TER
				 250-12TH ST
				 58 16TH STREET
				 95 D'ARCY ST 
				 96 		
				 99' MOUNTVIEW AVE
				 ;1382 ST CLAIR AVE W
				 9i3 QUEEN ST W
				 9C CLAIRTRELL RD, A PIPER ST
				 =80 FRONT ST E
				 BACK/OF   425 ADELAIDE ST W
				 B
				 ENTRANCE 7 BY SECURITY OFFFICE
				 FROM 10 ADELAIDE ST E
				 GREEN P LOT #664,  GREEN P LOT 141
				 GRANGE AVE  /  SPADINA AVE, LAKEVIEW AVE  /
				 O/FOF
				 O'HARA AVE W/SIDE
				 OUT/FRONT OF   487 HOPEWELL AV
				 PLATINUM DR AND TOUCHSTONE DR
				 QUEEN'S PARK CRES E, QUEENS QUAY E
				 Z656 YONGE ST
				 ]125 BEATRICE ST, \194 ROXTON RD, _ MARKHAM ST
				 a40 ROXBOROUGH ST W, bellev BELLEVUE AVE
				*/
				
				// skip all characters until we get first uppercase letter
				// ignore the cases like "96" or " " which have no letters.
				while (idx < nBytes) {
					if (fileBuffer[idx] >= 'A' && fileBuffer[idx] <= 'Z') {
						break;
					}
					idx++;
				}
				
				// handle cases like "272A DANFORTH AVE"; ignore cases like "58 16TH STREET"
				if (idx + 1 < nBytes && fileBuffer[idx + 1] == BYTE_SPACE) {
					idx += 2;
				}
				idxNameStart = idx;		// record the starting and ending index of the name
				idxNameEnd = idx;
				
				// next thing is to parse the optional SUFFIX and DIRECTION
				// SUFFIX: ST, STREET, AV, AVE, COURT, CRT, CT, CR, CRES, R, RD, RAOD, GT
				//         BL, BLVD, BOULEVARD, DR, LA, LN, LANE, PL, PLACE
				// DIRECTION: EAST, WEST, E, W, N, S
								
				// the approximate algorithm is quite simple:
				// stop parsing at the word (except the first one of the NAME) whose length is smaller than 4.
				
				while (idx < nBytes) {
					// copy the first word of the name

					locationName[idx - idxNameStart] = (char)fileBuffer[idx];
					if (fileBuffer[idx] == BYTE_COMMA) {
						// update the ending index of the name, do not include the comma
						idxNameEnd = idx - 1;
						break;
						
					} else if (fileBuffer[idx] == BYTE_SPACE) {
						// now we start to parse the next word of the name
						idxNameEnd = idx - 1;
						idx++;
						
						while (idx < nBytes) {
							// recursively copy the following words

							locationName[idx - idxNameStart] = (char)fileBuffer[idx];					
							if (fileBuffer[idx] == BYTE_SPACE) {
								// now we find a new word
								// if the length of word is smaller than or equal to 3, such as ST, AV, AVE, CR,
								//   skip all following characters
								// otherwise, update the ending index of the name.
								if (idx - idxNameEnd > 5) {
									/*
									 	FirstWord SecondWord Third,
									            ^           ^
									        idxNameEnd     idx
									 */
									idxNameEnd += idx - idxNameEnd - 1;
								} else {
									/*
								 		FirstWord AVE E, 
								            	^    ^
								    	idxNameEnd  idx
									 */
									break;
								}
								// continue copying the next word;
								
							} else if (fileBuffer[idx] == BYTE_COMMA) {
								// same as above
								if (idx - idxNameEnd > 5) {
									idxNameEnd += idx - idxNameEnd - 1;
								}
								break;
							}
							idx++;
						}
						
						break;
					}
					idx++;
				}
				// =============== finish parsing location2 ===============

				
				// store the result into local HashMap
				String key = String.copyValueOf(locationName, 0, idxNameEnd - idxNameStart + 1);
				Integer oldValue = ticketsStatsInThread.get(key);
				if (oldValue == null) {
					ticketsStatsInThread.put(key, fineAmount);
				} else {
					ticketsStatsInThread.put(key, fineAmount + oldValue);
				}		
				
				
				// skip the following columns: location3,location4,province
				// the minimum length of them is 4. e.g. ",,ON"
				idx += 4;
				
			}
		} while (true);
		
	}

}


