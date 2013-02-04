// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import ccparc.util.TextUtils;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

//---------------------------------------------------------------------------------------------------------------------------------

public class ChurchAndGale {

    public static class SentencePair {
        public List<String> enSents;
        public List<String> frSents;

        public SentencePair (List<String> enSents, List<String> frSents) {
            this.enSents = Collections.unmodifiableList (enSents);
            this.frSents = Collections.unmodifiableList (frSents);
        }

        public String getEnText () {
            return join (enSents);
        }

        public String getFrText () {
            return join (frSents);
        }

        private String join (List<String> sents) {
            StringBuilder sb = new StringBuilder ();
            for (int i = 0; i < sents.size(); i++) {
                if (i > 0) sb.append (' ');
                sb.append (sents.get(i));
            }
            return sb.toString();
        }

        public String toString () {
            return getEnText() + "\n" + getFrText() + "\n";
        }
    }

    public static List<SentencePair> churchAndGale (List<String> allEnSents, List<String> allFrSents) {
        int N = allEnSents.size() + 1;
        int M = allFrSents.size() + 1;

        // Largely copied from Philipp Koehn's `sentence-align-corpus.perl' (http://www.statmt.org/europarl/)

        final double[][] PRIOR = new double[][] {
            {                1,  Math.log(0.01/2),                 1 },
            { Math.log(0.01/2),    Math.log(0.89), Math.log(0.089/2) },
            {                1, Math.log(0.089/2),                 1 }
        };

        List<Integer> allEnLengths = computeLengths (allEnSents);
        List<Integer> allFrLengths = computeLengths (allFrSents);

        double[][] cost = new double [N][M];
        int[][][] backtrace = new int [N][M][2];

        cost[0][0] = 0;
        for (int i = 0; i < N; i++) {
            for (int j = 0; j < M; j++) {
                if (i == 0 && j == 0)
                    continue;
                cost[i][j] = Integer.MAX_VALUE;

                for (int d1 = 0; d1 < PRIOR.length; d1++) {
                    if (d1 > i)
                        continue;
                    for (int d2 = 0; d2 < PRIOR[d1].length; d2++) {
                        if (d2 > j || PRIOR[d1][d2] > 0)
                            continue;
                        double c = cost[i-d1][j-d2] - PRIOR[d1][d2] + match (
                            allEnLengths.get(i) - allEnLengths.get(i-d1),
                            allFrLengths.get(j) - allFrLengths.get(j-d2)
                        );
                        //System.out.println (c);
                        if (c < cost[i][j]) {
                            cost[i][j] = c;
                            backtrace[i][j][0] = d1;
                            backtrace[i][j][1] = d2;
                            // System.out.println (i + "," + j + ": [" + backtrace[i][j][0] + "," + backtrace[i][j][1] + "]");
                        }
                    }
                }
            }
        }

        int i = N-1;
        int j = M-1;
        int[] b;
        List<SentencePair> pairs = new ArrayList<SentencePair>();
        while (i > 0 || j > 0) {
            b = backtrace[i][j];
            pairs.add (new SentencePair (
                allEnSents.subList (i-b[0], i),
                allFrSents.subList (j-b[1], j)
            ));
            i -= b[0];
            j -= b[1];
        }

        Collections.reverse(pairs);
        return pairs;
    }

    private static double match (int len1, int len2) {
        double c = 1;
        double s2 = 6.8;

        if (len1==0 && len2==0)
            return 0;

        double mean = (len1 + len2/c) / 2;
        double z = (c*len1 - len2)/Math.sqrt(s2*mean);
        if (z < 0)
            z = -z;
        double pd = 2 * (1 - pnorm(z));
        if (pd > 0)
            return -Math.log(pd);
        return 25;
    }

    private static double pnorm (double z) {
        double t = 1/(1 + 0.2316419 * z);
        return 1 - 0.3989423 * Math.exp(-z * z/2) *
            ((((1.330274429 * t 
                - 1.821255978) * t 
               + 1.781477937) * t 
              - 0.356563782) * t
             + 0.319381530) * t;
    }

    private static List<Integer> computeLengths (List<String> sents) {
        List<Integer> lengths = new ArrayList<Integer> (sents.size()+1);
        int l = 0;
        lengths.add (0);
        for (String sent : sents) {
            l += sent.replaceAll("\\s+","").length();
            lengths.add (l);
        }
        return lengths;
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public static void main (String[] args) {
        try {
            for (SentencePair p : churchAndGale (
                     TextUtils.splitSentences (new File("dat/txt.en")),
                     TextUtils.splitSentences (new File ("dat/txt.fr"))
                 ))
                System.out.println (p);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
 