// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.ArrayList;
import java.util.List;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//---------------------------------------------------------------------------------------------------------------------------------

public class TextUtils {

    public static List<String> readLines (final File file)
        throws IOException
    {
        BufferedReader reader = new BufferedReader (new FileReader (file));
        List<String> lines = new ArrayList<String> ();
        for (;;) {
            String line = reader.readLine();
            if (line == null)
                break;
            lines.add (line);
        }
        reader.close();
        return lines;
    }

    public static String readText (File file)
        throws IOException
    {
        return readText (new FileReader (file));
    }

    public static String readText (Reader srcReader)
        throws IOException
    {
        BufferedReader reader = new BufferedReader (srcReader);
        StringBuilder buf = new StringBuilder ("");
        for (;;) {
            String line = reader.readLine();
            if (line == null)
                break;
            buf.append (line);
            buf.append ('\n');
        }
        reader.close();
        return buf.toString();
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public static List<String> splitSentences (String text) {
        List<String> ret = new ArrayList<String>();
        Matcher m = Pattern.compile("(.+?(?:[\\.\\?!]+|$))(?:\\s+|$)", Pattern.DOTALL).matcher (text);
        while (m.find())
            ret.add (m.group(1));
        return ret;
    }

    public static List<String> splitSentences (File file)
        throws IOException
    {
        return splitSentences (readText (file));
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
