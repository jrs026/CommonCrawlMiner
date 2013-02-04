// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import ccparc.ChurchAndGale;

import ccparc.exceptions.SkippedDocument;
import ccparc.exceptions.SkippedDocumentPair;

import ccparc.structs.Candidate;
import ccparc.structs.CandidatePair;
import ccparc.structs.Chunk;
import ccparc.structs.ChunkPair;
import ccparc.structs.DocumentPair;
import ccparc.structs.Language;
import ccparc.structs.TextChunk;

import ccparc.util.TextUtils;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import java.util.regex.Pattern;

import static org.apache.commons.lang.StringEscapeUtils.unescapeHtml;

import org.commoncrawl.hadoop.mapred.ArcRecord;

//---------------------------------------------------------------------------------------------------------------------------------

public class CCParc {

    //-----------------------------------------------------------------------------------------------------------------------------
    // config

    // 2012-09-27 - initial tests suggest this throws away more good stuff than it filters bad stuff out
    public static final boolean DO_DETECT_LANGUAGES = false;

    // 2012-09-27 - herve - now that the alignment algorithm focuses on the diagonal only, I think we could push this up
    public static final int MAX_DOC_LENGTH_IN_CHARS = 500*1000;


    //-----------------------------------------------------------------------------------------------------------------------------
    // Extraction of HTML (as String data) from an ArcRecord

    public static String getHtmlStr (ArcRecord rec)
        throws SkippedDocument, IOException
    {
        if (!rec.getContentType().contains("html"))
            throw new SkippedDocument (rec.getURL(), SkippedDocument.Reason.NOT_HTML);

        String htmlStr = unescapeHtml (
            TextUtils.readText (
                CharacterSets.decodeHtml (
                    ArcIO.recordInputStream (rec),
                    ArcIO.getCharsetNameFromHttpHeaders (rec)
                )
            )
        );

        if (htmlStr.length() > MAX_DOC_LENGTH_IN_CHARS)
            throw new SkippedDocument (rec.getURL(), SkippedDocument.Reason.TOO_LARGE, htmlStr.length() + " chars");

        return htmlStr;
    }

    public static List<CandidatePair> pairUpCandidates (List<Candidate> candidatesHavingSameBaseUrl) {

        // Sort them by language to avoid grouping en-fr and fr-en as two different language pairs
        Collections.sort (candidatesHavingSameBaseUrl, new Comparator<Candidate> () {
            public int compare (Candidate a, Candidate b) {
                return a.lui.lang.compareTo (b.lui.lang);
            }
        });

        List<CandidatePair> ret = new ArrayList<CandidatePair> ();
        int n = candidatesHavingSameBaseUrl.size();
        for (int i = 0; i < n-1; i++) {
            Candidate c1 = candidatesHavingSameBaseUrl.get(i);
            for (int j = i+1; j < n; j++) {
                Candidate c2 = candidatesHavingSameBaseUrl.get(j);
                if (!c1.lui.lang.equals(c2.lui.lang))
                    ret.add (new CandidatePair (c1, c2));
            }
        }

        return ret;
    }


    //-----------------------------------------------------------------------------------------------------------------------------
    // Align document pair candidates and select those whose documents align well

    public static DocumentPair alignCandidates (CandidatePair cpair)
        throws SkippedDocumentPair
    {
        List<Chunk> enChunks = HtmlChunker.splitIntoChunks (cpair.enCandidate.html.toString());
        List<Chunk> frChunks = HtmlChunker.splitIntoChunks (cpair.frCandidate.html.toString());

        DocumentPair docPair = new DocumentPair (
            cpair.enCandidate.lui,
            cpair.frCandidate.lui,
            HtmlAligner.align (enChunks, frChunks)
        );

        if (docPair.getPercentAligned() < 0.85)
            throw new SkippedDocumentPair (
                cpair.enCandidate.lui.fullUrl.toString(),
                cpair.frCandidate.lui.fullUrl.toString(),
                SkippedDocumentPair.Reason.MISALIGNED,
                String.valueOf (docPair.getPercentAligned())
            );

        if (!docPair.hasText())
            throw new SkippedDocumentPair (
                cpair.enCandidate.lui.fullUrl.toString(),
                cpair.frCandidate.lui.fullUrl.toString(),
                SkippedDocumentPair.Reason.CONTAINS_NO_TEXT
            );

        return docPair;
    }


    //-----------------------------------------------------------------------------------------------------------------------------
    // Extract SentencePair objects from a DocumentPair

    public static final Pattern RE_ALNUM = Pattern.compile ("\\p{Alnum}");

    public static interface SentencePairConsumer {
        public void consume (String enSentence, String frSentence) throws IOException;
    }

    public static void extractSentencePairs (DocumentPair docPair, SentencePairConsumer consumer)
        throws IOException
    {
        for (ChunkPair c : docPair) {
            if (
                c.enChunk instanceof TextChunk
                && c.frChunk instanceof TextChunk
                && !c.enChunk.content.toLowerCase().equals (c.frChunk.content.toLowerCase())
            ) {
                for (ChurchAndGale.SentencePair sp : ChurchAndGale.churchAndGale (
                         TextUtils.splitSentences (c.enChunk.content),
                         TextUtils.splitSentences (c.frChunk.content)
                     )) {
                    String enSentence = sp.getEnText();
                    String frSentence = sp.getFrText();
                    
                    // NB this ensures both that we don't have "sentences" consisiting of punctuation only, and that we don't have
                    // empty sentences, as the C&G algo is able to align 1-to-0
                    if (RE_ALNUM.matcher(enSentence).find() && RE_ALNUM.matcher(frSentence).find()) {

                        consumer.consume (enSentence, frSentence);
                    }
                }
            }
        }
    }


    //-----------------------------------------------------------------------------------------------------------------------------

}

//---------------------------------------------------------------------------------------------------------------------------------
