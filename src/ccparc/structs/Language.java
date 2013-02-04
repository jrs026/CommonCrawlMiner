// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.structs;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

//---------------------------------------------------------------------------------------------------------------------------------

public class Language
    implements Comparable<Language>, Writable
{

    public final Text id;

    public Language () {
        this (new Text());
    }

    public Language (String id) {
        this (new Text (id));
    }

    public Language (Text id) {
        this.id = id;
    }

    public Language copy () {
        return new Language (
            new Text (id)
        );
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public int hashCode () {
        throw new UnsupportedOperationException ("This class is mutable, don't hash it");
    }

    public int compareTo (Language o) {
        return id.compareTo (o.id);
    }

    public boolean equals (Object o) {
        return o instanceof Language && ((Language) o).id.equals (id);
    }

    public String toString () {
        return id.toString();
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public void readFields (DataInput in) throws IOException {
        id.readFields (in);
    }

    public void write (DataOutput out) throws IOException {
        id.write (out);
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public static final Language AR = new Language ("ar");
    public static final Language BG = new Language ("bg");
    public static final Language CS = new Language ("cs");
    public static final Language DE = new Language ("de");
    public static final Language EN = new Language ("en");
    public static final Language ES = new Language ("es");
    public static final Language FR = new Language ("fr");
    public static final Language ZH = new Language ("zh");

    public static final List<Language> ALL_LANGS = Collections.unmodifiableList (Arrays.asList (AR, BG, CS, DE, EN, ES, FR, ZH));

    public static final Map<String,Language> ALL_LANGS_BY_IDS; static {
        HashMap<String,Language> allLangsById = new HashMap<String,Language>();
        for (Language lang : ALL_LANGS)
            allLangsById.put (lang.id.toString(), lang);
        ALL_LANGS_BY_IDS = Collections.unmodifiableMap (allLangsById);
    }
    

}

//---------------------------------------------------------------------------------------------------------------------------------
