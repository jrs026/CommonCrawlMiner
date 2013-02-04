// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

import ccparc.structs.Chunk;
import ccparc.structs.TagChunk;
import ccparc.structs.TextChunk;

import ccparc.util.TextUtils;

import java.io.File;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

//---------------------------------------------------------------------------------------------------------------------------------

public class HtmlChunker {

    // Tags that can appear within a sentence
    public static final Pattern RE_SOFT_TAGS = Pattern.compile (
        "< /? (?:[a-z]+:)?"
        + "(?:a|b|strong|i|em|font|span|nobr|sup|sub|meta|link|acronym)"
        + "(?:[\\s/][^>]*)? >",
        Pattern.CASE_INSENSITIVE|Pattern.COMMENTS
    );

    public static final Pattern RE_INLINE_SCRIPT = Pattern.compile (
        // This hairy regex is supposed to avoid matching self-closing tags
        // 
        // NB this backtracks like crazy without the $ part. Doesn't in Perl, though. Not sure why it goes haywire here, but it
        // sure does. Basically never finishes.
        "<(?:script|style) (?:[^>/]+|/\\s*[^\\s>]*)* > .*? (?: $ | < \\s*/\\s* (?:script|style) \\s* > )",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.COMMENTS
    );

    public static final Pattern RE_COMMENT = Pattern.compile (
        "<!-- .*? (?: $ | (?<=--)>)",
        Pattern.DOTALL | Pattern.COMMENTS
    );

    public static final Pattern RE_SHORT_LIST_ITEM = Pattern.compile (
        "<li (?:\\s[^>]*)? > \\s* .{1,100}? (?: $ | </?(?:li|ul|ol)> \\s* )",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.COMMENTS
    );

    public static final Pattern RE_SHORT_TABLE_CELL = Pattern.compile (
        "<td (?:\\s[^>]*)? > \\s* .{1,100}? (?: $ | </td> \\s* )",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL | Pattern.COMMENTS
    );

    public static final Pattern RE_OPTION_TAG = Pattern.compile (
        "<option (?:\\s[^>]*)? > [^<]+",
        Pattern.CASE_INSENSITIVE | Pattern.COMMENTS
    );


    // these are set automatically by iterate-config-and-compute-scores.py
    private static final boolean DO_REMOVE_SHORT_LIST_ITEMS = false;
    private static final boolean DO_REMOVE_SHORT_TABLE_CELLS = false;
    private static final boolean DO_REMOVE_OPTION_TAGS = false;


    public static List<Chunk> splitIntoChunks (String htmlStr) {
        List<Chunk> allChunks = new ArrayList<Chunk> ();

        // remove comments and non-text data
        htmlStr = RE_COMMENT.matcher(htmlStr).replaceAll ("");
        htmlStr = RE_INLINE_SCRIPT.matcher(htmlStr).replaceAll ("");

        // remove soft tags and normalize spaces
        htmlStr = htmlStr.replaceAll ("&nbsp;", " ");
        // htmlStr = RE_SOFT_TAGS.matcher(htmlStr).replaceAll (" ");
        htmlStr = htmlStr.replaceAll ("\\s+", " ");

        // remove drop-down menus and short list items, as they introduce a lot of the noise we see
        if (DO_REMOVE_SHORT_LIST_ITEMS)
            htmlStr = RE_SHORT_LIST_ITEM.matcher(htmlStr).replaceAll (" ");
        if (DO_REMOVE_SHORT_TABLE_CELLS)
            htmlStr = RE_SHORT_TABLE_CELL.matcher(htmlStr).replaceAll (" ");
        if (DO_REMOVE_OPTION_TAGS)
            htmlStr = RE_OPTION_TAG.matcher(htmlStr).replaceAll (" ");

        Matcher tagsMatcher = TagChunk.PATTERN.matcher (htmlStr);
        Matcher textMatcher = TextChunk.PATTERN.matcher (htmlStr);

        // StringBuilder tagSoup = new StringBuilder();
        // Pattern reSoup = Pattern.compile ("(?<=<)\\w+");

        int pos = 0;
        Chunk chunk;
        while (pos < htmlStr.length()) {
            tagsMatcher.region (pos, htmlStr.length());
            textMatcher.region (pos, htmlStr.length());

            if (tagsMatcher.lookingAt()) {
                chunk = new TagChunk (tagsMatcher.group());
                pos = tagsMatcher.end();

                // Matcher soupMatcher = reSoup.matcher (tagsMatcher.group());
                // while (soupMatcher.find()) {
                //     tagSoup.append (soupMatcher.group().toUpperCase());
                //     tagSoup.append ('-');
                // }
            } else if (textMatcher.lookingAt()) {
                chunk = new TextChunk (
                    // tagSoup.toString() +
                    textMatcher.group()
                );
                // tagSoup.delete (0, tagSoup.length());
                pos = textMatcher.end();
            } else {
                throw new Error (
                    "Can't parse: '"
                    + htmlStr.substring (pos, Math.min(htmlStr.length(), pos+100))
                    + "'"
                );
            }

            if (chunk.content.length() > 0)
                allChunks.add (chunk);
        }

        return allChunks;
    }


    //-----------------------------------------------------------------------------------------------------------------------------

    public static List<Chunk> chunkFile (File file)
        throws IOException
    {
        return splitIntoChunks (TextUtils.readText (file));
    }

    public static void main (String[] args) {
        try {
            for (Chunk chunk : chunkFile(new File (args[0])))
                System.out.println (chunk);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

}

//---------------------------------------------------------------------------------------------------------------------------------
