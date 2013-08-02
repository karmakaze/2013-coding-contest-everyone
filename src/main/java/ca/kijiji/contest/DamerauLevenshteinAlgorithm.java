package ca.kijiji.contest;

import java.util.HashMap;
import java.util.Map;

/* Copyright (c) 2012 Kevin L. Stern
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

/**
 * The Damerau-Levenshtein Algorithm is an extension to the Levenshtein Algorithm which solves the edit distance problem
 * between a source string and a target string with the following operations:
 * 
 * <ul>
 * <li>Character Insertion</li>
 * <li>Character Deletion</li>
 * <li>Character Replacement</li>
 * <li>Adjacent Character Swap</li>
 * </ul>
 * 
 * Note that the adjacent character swap operation is an edit that may be applied when two adjacent characters in the
 * source string match two adjacent characters in the target string, but in reverse order, rather than a general
 * allowance for adjacent character swaps.
 * <p>
 * 
 * The running time of the Damerau-Levenshtein algorithm is O(n*m) where n is the length of the source string and m is
 * the length of the target string. This implementation consumes O(n*m) space.
 * 
 * 
 * ACBellini: this is an adapted version, removing the optional weights
 * 
 * @author Kevin L. Stern
 * @author Anna Chiara Bellini
 */
public class DamerauLevenshteinAlgorithm {

    /**
     * Compute the Damerau-Levenshtein distance between the specified source string and the specified target string.
     */
    public int execute(String source, String target) {
	if (source.length() == 0) {
	    return target.length();
	}
	if (target.length() == 0) {
	    return source.length();
	}
	int[][] table = new int[source.length()][target.length()];
	Map<Character, Integer> sourceIndexByCharacter = new HashMap<Character, Integer>();
	if (source.charAt(0) != target.charAt(0)) {
	    table[0][0] = 1;
	}
	sourceIndexByCharacter.put(source.charAt(0), 0);
	for (int i = 1; i < source.length(); i++) {
	    int deleteDistance = table[i - 1][0] + 1;
	    int insertDistance = (i + 1) * 1 + 1;
	    int matchDistance = i * 1 + (source.charAt(i) == target.charAt(0) ? 0 : 1);
	    table[i][0] = Math.min(Math.min(deleteDistance, insertDistance), matchDistance);
	}
	for (int j = 1; j < target.length(); j++) {
	    int deleteDistance = table[0][j - 1] + 1;
	    int insertDistance = (j + 1)  + 1;
	    int matchDistance = j + (source.charAt(0) == target.charAt(j) ? 0 : 1);
	    table[0][j] = Math.min(Math.min(deleteDistance, insertDistance), matchDistance);
	}
	for (int i = 1; i < source.length(); i++) {
	    int maxSourceLetterMatchIndex = source.charAt(i) == target.charAt(0) ? 0 : -1;
	    for (int j = 1; j < target.length(); j++) {
		Integer candidateSwapIndex = sourceIndexByCharacter.get(target.charAt(j));
		int jSwap = maxSourceLetterMatchIndex;
		int deleteDistance = table[i - 1][j] + 1;
		int insertDistance = table[i][j - 1] + 1;
		int matchDistance = table[i - 1][j - 1];
		if (source.charAt(i) != target.charAt(j)) {
		    matchDistance += 1;
		} else {
		    maxSourceLetterMatchIndex = j;
		}
		int swapDistance;
		if (candidateSwapIndex != null && jSwap != -1) {
		    int iSwap = candidateSwapIndex;
		    int preSwapCost;
		    if (iSwap == 0 && jSwap == 0) {
			preSwapCost = 0;
		    } else {
			preSwapCost = table[Math.max(0, iSwap - 1)][Math.max(0, jSwap - 1)];
		    }
		    swapDistance = preSwapCost + (i - iSwap - 1) + (j - jSwap - 1)  + 1;
		} else {
		    swapDistance = Integer.MAX_VALUE;
		}
		table[i][j] = Math.min(Math.min(Math.min(deleteDistance, insertDistance), matchDistance), swapDistance);
	    }
	    sourceIndexByCharacter.put(source.charAt(i), i);
	}
	return table[source.length() - 1][target.length() - 1];
    }
}
