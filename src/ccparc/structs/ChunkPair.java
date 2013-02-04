// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

public class ChunkPair {

    private static final int WIDTH = 70;
    private static final String BLANK = String.format ("%" + WIDTH + "s", "");

    public final Chunk enChunk;
    public final Chunk frChunk;

    public ChunkPair (Chunk enChunk, Chunk frChunk) {
        this.enChunk = enChunk;
        this.frChunk = frChunk;
    }

    public String toString () {
        String s1 = enChunk == null ? "" : enChunk.toString();
        String s2 = frChunk == null ? "" : frChunk.toString();
        return
            (s1.length() > WIDTH ? s1.substring(s1.length()-WIDTH) : BLANK.substring (s1.length()) + s1)
            + " | "
            + (s2.length() > WIDTH ? s2.substring(0,WIDTH) : s2);
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
