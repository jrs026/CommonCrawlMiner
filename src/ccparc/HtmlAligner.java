// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import ccparc.structs.Chunk;
import ccparc.structs.ChunkPair;
import ccparc.structs.TextChunk;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//---------------------------------------------------------------------------------------------------------------------------------

public class HtmlAligner {

    public static List<ChunkPair> align (List<Chunk> enDocChunks, List<Chunk> frDocChunks) {

        // Ensure the DP matrix is more tall than wide. This simplifies parts of the algorithm.
        boolean flipped = false;
        if (enDocChunks.size() < frDocChunks.size()) {
            List<Chunk> t = enDocChunks;
            enDocChunks = frDocChunks;
            frDocChunks = t;
            flipped = true;
        }

        final int N = 1 + enDocChunks.size();
        final int M = 1 + frDocChunks.size();
        if (N == 1 || M == 1)
            return Collections.emptyList();

        // 2012-09-13 - herve - I've found by a bit of trial an error that setting this to 15% results in a drop of less than 1% in
        // output volume, and doesn't seem to affect output quality. This was all done on a rather small sample, though, so it
        // could be worth investigating further later on.
        final int MAX_DIST_OFF_DIAG = (int) (M*0.175);

        // In order to conserve memory and save a few CPU cycles, we constrain our search to a fixed-width beam around the diagonal
        // of the matrix.
        // 
        // Since the matrix is guaranteed to be more tall than wide, there is always a single `j' on the diagonal for a given `i',
        // hence the +1
        // 
        final int BEAM_WIDTH = MAX_DIST_OFF_DIAG*2 + 1;

        // weights for the levenshtein distance algo
        final int COST_SCALE = 100;
        final int INS_COST = 1;
        final int DEL_COST = 1;
        final int SUB_SCALE = 3;

        // a score that is greater than the highest possible score given to an alignment
        final int OFF_LIMITS = (M+N) * COST_SCALE * Math.max (INS_COST, Math.max (DEL_COST, SUB_SCALE)) + 1;

        // flags indicating paths taken through the matrix
        final int DEL = 1;
        final int INS = 2;
        final int SUB = 3;


        // Notes on nomenclature:
        // 
        // <> `i' and `j' are indices in the full rectangular DP matrix. They are therefore also indices in enDocChunks and
        //    frDocChunks respectively, if you subtract one from them. Note that since as an optimization we don't instantiate the
        //    full width of the matrix, "the full rectangular DP matrix" is a conceptual matrix only, and`j' is not a valid index
        //    in any actually allocated array. `i' however is a valid index in `matrix'.
        // 
        // <> `beamOffset' is the index, within the (conceptual) full rectangular DP matrix, at a given row `i', of the leftmost
        //    cell that falls inside the beam that we search. The beam is centred on the diagonal of the full matrix. For low
        //    values of `i', `beamOffset' might be negative, and for values of `i' approaching `N' it might be that `j > = M'
        // 
        // <> `beamJ' is an index within the moving window of the beam. Is is a valid index for the 2nd dimension of `matrix'.
        //    However since the moving window of the beam is aligned differently for every `i', `beamJ' is not a valid index in
        //    `frDocChunks'
        // 
        // <> `prevRowBeamJ' is the beamJ in row `i-1' that corresponds to the same `j' in the full rectangular DP matrix as
        //    `beamJ' does in row `i'


        final int[][][] matrix = new int [N][BEAM_WIDTH][2];
        for (int beamJ = 0; beamJ < BEAM_WIDTH; beamJ++) {
            matrix[0][beamJ][0] = beamJ*INS_COST;
            matrix[0][beamJ][1] = INS;
        }

        Chunk e, f;
        for (int i = 1; i < N; i++) {

            int diagJ = i * M/N;
            int beamOffset = diagJ - MAX_DIST_OFF_DIAG;
            int beamStep = diagJ - ((i-1)*M/N);

            e = enDocChunks.get(i-1);
            for (
                int beamJ = 0,
                    j = diagJ - MAX_DIST_OFF_DIAG,
                    prevRowBeamJ = beamJ + beamStep;
                beamJ < BEAM_WIDTH;
                beamJ++, j++, prevRowBeamJ++
            ) {
                int[] cell = matrix[i][beamJ];

                if (j < 0 || j >= M) {
                    cell[0] = OFF_LIMITS;
                    continue;
                } else if (j == 0) {
                    cell[0] = i*DEL_COST;
                    cell[1] = DEL;
                    continue;
                }

                f = frDocChunks.get(j-1);

                int delScore = (prevRowBeamJ < BEAM_WIDTH)
                    ? matrix[i-1][prevRowBeamJ][0] + DEL_COST*COST_SCALE
                    : OFF_LIMITS;
                int insScore = (beamJ > 0 && j > 0)
                    ? matrix[i][beamJ-1][0] + INS_COST*COST_SCALE
                    : OFF_LIMITS;
                int subScore = (prevRowBeamJ > 0 && j > 0 && e.canAlignWith(f))
                    ? matrix[i-1][prevRowBeamJ-1][0] + (int) (alignmentCost(e,f)*SUB_SCALE*COST_SCALE)
                    : OFF_LIMITS;

                if (subScore < OFF_LIMITS && subScore <= delScore && subScore <= insScore) {
                    cell[0] = subScore;
                    cell[1] = SUB;
                } else if (delScore < OFF_LIMITS && delScore <= insScore && delScore <= subScore) {
                    cell[0] = delScore;
                    cell[1] = DEL;
                } else if (insScore < OFF_LIMITS) {
                    cell[0] = insScore;
                    cell[1] = INS;
                } else {
                    cell[0] = OFF_LIMITS;
                }
            }
        }

        ArrayList<ChunkPair> allPairs = new ArrayList<ChunkPair>();
        int i = N-1;
        int j = M-1;
        int diagJ = i * M/N;
        int beamOffset = diagJ - MAX_DIST_OFF_DIAG;
        int beamJ = j - beamOffset;

        // 2012-09-27 - I'm getting ArrayIndexOutOfBoundsException's at the `int[] cell = ...' line below, not sure why. No time to
        // re-figure out the algorithm now, so I'm just adding the >= 0 boundary checks here, though when I originally wrote this I
        // was pretty sure the `i > 0 || j > 0' would suffice
        while (i >= 0 && beamJ >= 0 && (i > 0 || j > 0)) {

            int[] cell = matrix[i][beamJ];
            Chunk enChunk = (cell[1] == INS) ? null : enDocChunks.get(i-1);
            Chunk frChunk = (cell[1] == DEL) ? null : frDocChunks.get(j-1);

            allPairs.add (new ChunkPair (
                flipped ? frChunk : enChunk,
                flipped ? enChunk : frChunk
            ));

            if (cell[1] != INS) {
                i--;
                diagJ = i * M/N;
                beamOffset = diagJ - MAX_DIST_OFF_DIAG;
                beamJ = j - beamOffset;
            }

            if (cell[1] != DEL) {
                j--;
                beamJ--;
            }
        }

        Collections.reverse (allPairs);
        return allPairs;
    }

    // A returned 0 means they align perfectly, 1 or more means they don't align. The gradient btw these two values isn't really
    // defined, which is bad.
    public static double alignmentCost (Chunk e, Chunk f) {
        if (e instanceof TextChunk && f instanceof TextChunk) {
            if (!((TextChunk)e).anchors.equals(((TextChunk)f).anchors)) {
                return 1;
            } else {
                if (e.content.length() > f.content.length()) {
                    Chunk t = e;
                    e = f;
                    f = t;
                }
                return Math.log (
                    (25.0 + f.content.length()) /
                    (25.0 + e.content.length())
                );
            }
        } else {
            return 0;
        }
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public static List<ChunkPair> alignFiles (File f1, File f2)
        throws IOException
    {
        return align (
            HtmlChunker.chunkFile (f1),
            HtmlChunker.chunkFile (f2)
        );
    }

    public static void main (String[] args) {
        try {
            for (ChunkPair p : alignFiles (new File (args[0]), new File (args[1]))) {
                System.out.println (p);
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
