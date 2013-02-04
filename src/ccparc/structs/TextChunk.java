// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//---------------------------------------------------------------------------------------------------------------------------------

public class TextChunk extends Chunk {

    public static final Pattern PATTERN = Pattern.compile (" (?: [^<]+ | <> )+ ", Pattern.COMMENTS);

    private static final Pattern RE_SPACES = Pattern.compile ("\\s+");

    public final String anchors;

    public TextChunk (String content) {
        super (content);
        anchors = findAnchors (content);
    }

    public String normalize (String content) {
        return RE_SPACES.matcher(content).replaceAll(" ").trim();
    }

    public boolean canAlignWith (Chunk o) {
        return o instanceof TextChunk;
    }

    private String findAnchors (String s) {
        StringBuilder sb = new StringBuilder ();
        Matcher m = Pattern.compile("\\b\\d+\\b").matcher (s);
        while (m.find()) {
            if (sb.length() > 0)
                sb.append ('\0');
            sb.append (m.group());
        }
        return sb.toString();
    }


}

//---------------------------------------------------------------------------------------------------------------------------------
