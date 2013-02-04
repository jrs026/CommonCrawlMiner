// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.hadoop;

import ccparc.CCParc;
import ccparc.Urls;

import ccparc.exceptions.SkippedDocument;

import ccparc.structs.Candidate;
import ccparc.structs.LanguageIndependentUrl;

import java.io.IOException;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import org.commoncrawl.hadoop.mapred.ArcRecord;

//---------------------------------------------------------------------------------------------------------------------------------

public class CCParcMapper
    extends MapReduceBase
    implements Mapper <Text, ArcRecord, Text, Candidate>
{

    public void map (Text key, ArcRecord rec, OutputCollector<Text,Candidate> output, Reporter reporter)
        throws IOException
    {

        String htmlStr;
        try {
            htmlStr = CCParc.getHtmlStr (rec);
        } catch (SkippedDocument ex) {
            reporter.getCounter ("SkippedDocument", ex.reason.toString()).increment (1);
            return;
        }

        for (LanguageIndependentUrl lui : Urls.getLanguageIndependentUrls (rec.getURL()))
            output.collect (
                new Text (lui.urlBase),
                new Candidate (lui, htmlStr)
            );
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
