// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

import ccparc.structs.ChunkPair;
import ccparc.structs.LanguageIndependentUrl;
import ccparc.structs.TextChunk;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

//---------------------------------------------------------------------------------------------------------------------------------

public class DocumentPair
    implements Iterable<ChunkPair>
{

    public final LanguageIndependentUrl enLui;
    public final LanguageIndependentUrl frLui;
    public final List<ChunkPair> chunkPairs;

    private double percentAligned;

    public DocumentPair (LanguageIndependentUrl enLui, LanguageIndependentUrl frLui, List<ChunkPair> chunkPairs) {
        this.enLui = enLui;
        this.frLui = frLui;
        this.chunkPairs = Collections.unmodifiableList (chunkPairs);
        percentAligned = -1;
    }

    public Iterator<ChunkPair> iterator () {
        return chunkPairs.iterator();
    }

    public boolean hasText() {
        for (ChunkPair cp : this)
            if (cp.enChunk instanceof TextChunk)
                return true;
        return false;
    }

    public double getPercentAligned () {
        if (percentAligned < 0) {
            int numAligned = 0;
            for (ChunkPair p : chunkPairs)
                if (p.enChunk != null && p.frChunk != null)
                    numAligned++;
            percentAligned = (double) numAligned / chunkPairs.size();
        }
        return percentAligned;
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
