public class StringUtils {

    /*
     returns index of the first space character in the string
     or str.length() in case there are no space characters
     */
    private static int findFirstSpaceCharIndex(String str) {
        int answer;
        for (answer = 0; answer < str.length(); ++answer) {
            char currChar = str.charAt(answer);
            if (Character.isSpaceChar(currChar)) {
                break;
            }
        }
        return answer;
    }

    /*
     returns the index of the first character and the index after the last
     character of the first met substring of a given string without space characters
     */
    public static Pair<Integer, Integer> firstNonSpaceSubstring(String str) {
        String trimmed = str.trim();

        int startIndex = str.length() - trimmed.length();
        int endIndex = startIndex + findFirstSpaceCharIndex(trimmed);

        return new Pair<>(startIndex, endIndex);
    }
}