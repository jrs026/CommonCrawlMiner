// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import ccparc.structs.Language;
import ccparc.structs.LanguageIndependentUrl;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//---------------------------------------------------------------------------------------------------------------------------------

public class Urls {

    public static final Map<String,Language> LANGUAGES_BY_ID;
    public static final Pattern RE_LANGUAGE_IDS;

    static {

        Map<String,Language> languagesById = new HashMap<String,Language>();

        // 2012-09-12 - herve - some quick testing suggests that single-letter language codes ("e" for English, "f" for French,
        // etc) bring in more noise that good data, so I've removed them

        languagesById.put ("arabic",    Language.AR);
        languagesById.put ("ara",       Language.AR);
        languagesById.put ("ar",        Language.AR);
        languagesById.put ("bulgarian", Language.BG);
        languagesById.put ("bul",       Language.BG);
        languagesById.put ("bg",        Language.BG);
        languagesById.put ("czech",     Language.CS);
        languagesById.put ("cze",       Language.CS);
        languagesById.put ("cz",        Language.CS);
        languagesById.put ("cs",        Language.CS);
        languagesById.put ("deutsch",   Language.DE);
        languagesById.put ("german",    Language.DE);
        languagesById.put ("ger",       Language.DE);
        languagesById.put ("deu",       Language.DE);
        languagesById.put ("de",        Language.DE);
        languagesById.put ("english",   Language.EN);
        languagesById.put ("eng",       Language.EN);
        languagesById.put ("en",        Language.EN);
        languagesById.put ("espanol",   Language.ES);
        languagesById.put ("spanish",   Language.ES);
        languagesById.put ("spa",       Language.ES);
        languagesById.put ("esp",       Language.ES);
        languagesById.put ("es",        Language.ES);
        languagesById.put ("francais",  Language.FR);
        languagesById.put ("french",    Language.FR);
        languagesById.put ("fra",       Language.FR);
        languagesById.put ("fre",       Language.FR);
        languagesById.put ("fr",        Language.FR);
        languagesById.put ("chinese",   Language.ZH);
        languagesById.put ("chi",       Language.ZH);
        languagesById.put ("zh",        Language.ZH);

        LANGUAGES_BY_ID = Collections.unmodifiableMap (languagesById);

        StringBuilder reLanguageIds = new StringBuilder ();
        for (String id : LANGUAGES_BY_ID.keySet()) {
            reLanguageIds.append (reLanguageIds.length() == 0 ? "(?<![a-zA-Z0-9])(?:" : "|");
            reLanguageIds.append (id);
        }
        reLanguageIds.append (")(?![a-zA-Z0-9])");
        RE_LANGUAGE_IDS = Pattern.compile (reLanguageIds.toString());
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public static List<LanguageIndependentUrl> getLanguageIndependentUrls (String strUrl) {
        List<LanguageIndependentUrl> ret = new ArrayList<LanguageIndependentUrl>();

        try {
            URI uri = new URI (strUrl);

            for (int i = 0; i < 2; i++) {
                String urlPart = i == 0 ? uri.getPath() : uri.getQuery();
                if (urlPart == null)
                    continue;
                Matcher m = RE_LANGUAGE_IDS.matcher (urlPart);
                while (m.find())
                    ret.add (
                        new LanguageIndependentUrl (
                            new URI (
                                uri.getScheme(),
                                uri.getUserInfo(),
                                uri.getHost(),
                                uri.getPort(),
                                (i == 0
                                 ? uri.getPath().substring(0,m.start()) + "\0" + uri.getPath().substring(m.end())
                                 : uri.getPath()),
                                (i == 1
                                 ? uri.getQuery().substring(0,m.start()) + "\0" + uri.getQuery().substring(m.end())
                                 : uri.getQuery()),
                                uri.getFragment()
                            ).toString(),
                            strUrl,
                            LANGUAGES_BY_ID.get (m.group())
                        )
                    );
            }

        } catch (URISyntaxException ex) {
            // System.err.println ("Can't parse URL: " + ex.getInput());
        }

        return ret;
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
