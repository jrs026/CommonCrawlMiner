// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.hadoop;

import ccparc.CCParc;

import ccparc.exceptions.SkippedDocumentPair;

import ccparc.structs.Candidate;
import ccparc.structs.CandidatePair;
import ccparc.structs.DocumentPair;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

//---------------------------------------------------------------------------------------------------------------------------------

public class CCParcReducer
    extends MapReduceBase
    implements Reducer<Text, Candidate, Text, Text>
{

    // 2012-09-28 - is the same Reducer instance going to get executed by several threads concurrently? If so this shouldn't be a
    // class field.
    // 

    public void reduce (
        Text urlBase,
        Iterator<Candidate> iterCandidates,
        final OutputCollector<Text,Text> output,
        Reporter reporter
    )
        throws IOException
    {

        // We need them as a list because `pairUpCandidates' needs to sort them
        ArrayList<Candidate> candidates = new ArrayList<Candidate> ();

        // Remove candidates that don't pass the language test.
        // 
        // Note that we only do the test at this stage, rather than above, to avoid running it uselessly on pages that don't
        // get paired up with others
        // 
        while (iterCandidates.hasNext()) {
            Candidate cand = iterCandidates.next();
            candidates.add (cand.copy());
        }

        for (CandidatePair cpair : CCParc.pairUpCandidates (candidates)) {
                        
            DocumentPair docPair;
            try {
                docPair = CCParc.alignCandidates (cpair);
            } catch (SkippedDocumentPair ex) {
                reporter.getCounter ("SkippedDocumentPair", ex.reason.toString()).increment (1);
                continue;
            }

            final Text langPair = new Text (docPair.enLui.lang + "-" + docPair.frLui.lang);
            CCParc.extractSentencePairs (docPair, new CCParc.SentencePairConsumer () {
                public void consume (String enSentence, String frSentence) throws IOException {
                    output.collect (langPair, new Text (enSentence + "\t" + frSentence));
                }
            });
        }
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
