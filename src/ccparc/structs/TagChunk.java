// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

import java.util.regex.Pattern;

//---------------------------------------------------------------------------------------------------------------------------------

public class TagChunk extends Chunk {

    public static Pattern PATTERN = Pattern.compile (" \\s* <[^>]+(?:>|$) \\s* ", Pattern.COMMENTS);

    public TagChunk (String content) {
        super (content);
    }

    public String normalize (String content) {
        return content.toLowerCase().trim().replaceAll ("(?<=\\w)[^\\w>][^>]*", "");
    }

    public boolean canAlignWith (Chunk o) {
        return o instanceof TagChunk && this.equals(o);
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
