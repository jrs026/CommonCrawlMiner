// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//---------------------------------------------------------------------------------------------------------------------------------

public class LanguageIndependentUrl
    implements Writable
{

    public final Text urlBase;
    public final Text fullUrl;
    public final Language lang;

    public LanguageIndependentUrl () {
        this (new Text(), new Text(), new Language());
    }

    public LanguageIndependentUrl (String urlBase, String fullUrl, Language lang) {
        this (new Text (urlBase), new Text (fullUrl), lang);
    }

    public LanguageIndependentUrl (Text urlBase, Text fullUrl, Language lang) {
        this.urlBase = urlBase;
        this.fullUrl = fullUrl;
        this.lang = lang;
    }

    public LanguageIndependentUrl copy () {
        return new LanguageIndependentUrl (
            new Text (urlBase),
            new Text (fullUrl),
            lang.copy ()
        );
    }


    public int hashCode () {
        throw new UnsupportedOperationException ("This class is mutable, don't hash it");
    }

    public void readFields (DataInput in) throws IOException {
        urlBase.readFields (in);
        fullUrl.readFields (in);
        lang.readFields (in);
    }

    public void write (DataOutput out) throws IOException {
        urlBase.write (out);
        fullUrl.write (out);
        lang.write (out);
    }


    public String hostname () {
        return urlBase.toString().replaceAll(".+//(?:www\\.)?","").replaceAll("/.+","");
    }

    public String toString () {
        return lang + ": " + fullUrl + " (" + urlBase.toString().replace('\0','*') + ")";
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
