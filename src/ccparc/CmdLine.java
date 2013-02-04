// $Id:$
// Herve Saint-Amand
// Edinburgh

//---------------------------------------------------------------------------------------------------------------------------------

package ccparc;

//---------------------------------------------------------------------------------------------------------------------------------

// This class defines a plain Java app version of the whole CCParc application. It builds on the same core components as the Hadoop
// version does.

import ccparc.exceptions.SkippedDocument;
import ccparc.exceptions.SkippedDocumentPair;

import ccparc.structs.Candidate;
import ccparc.structs.CandidatePair;
import ccparc.structs.DocumentPair;
import ccparc.structs.LanguageIndependentUrl;

import ccparc.util.HashMapOfArrayLists;

import static ccparc.util.IoUtils.println;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.commoncrawl.hadoop.mapred.ArcRecord;

//---------------------------------------------------------------------------------------------------------------------------------

public class CmdLine {

    //-----------------------------------------------------------------------------------------------------------------------------
    // config

    public static final int NUM_THREADS = 12;

    //-----------------------------------------------------------------------------------------------------------------------------
    // Pair up document pair candidates based on URLs. This largely corresponds to the Mapper's role in the Hadoop setup.

    // A thread that goes through Arc files, extracts each record's URL and HTML, extracts any LanguageIndependentUrl's the URL
    // might contain, and indexes the documents by base URL
    // 
    public static class ArcFileReaderThread
        extends Thread
    {

        private Iterator<File> iterInputFiles;
        public final HashMapOfArrayLists<String,Candidate> candidates;
        public boolean hasErrors;

        public ArcFileReaderThread (Iterator<File> iterInputFiles) {
            this.iterInputFiles = iterInputFiles;
            candidates = new HashMapOfArrayLists<String,Candidate>();
            hasErrors = false;
        }

        public void run () {
            try {
                for (;;) {

                    File file;
                    synchronized (iterInputFiles) {
                        if (!iterInputFiles.hasNext())
                            break;
                        file = iterInputFiles.next();
                    }

                    println ("< " + file);
                    for (ArcRecord rec : ArcIO.parseRecords(file)) {

                        String htmlStr;
                        try {
                            htmlStr = CCParc.getHtmlStr (rec);
                        } catch (IOException ex) {
                            println (ex);
                            continue;
                        } catch (SkippedDocument ex) {
                            // println (ex);
                            continue;
                        }

                        for (LanguageIndependentUrl lui : Urls.getLanguageIndependentUrls (rec.getURL()))
                            candidates.add (lui.urlBase.toString(), new Candidate (lui, htmlStr));
                    }
                }
            } catch (Throwable ex) {
                ex.printStackTrace();
                hasErrors = true;
            }
        }
    }


    public static HashMapOfArrayLists<String,Candidate> loadAllCandidatesByUrlBase (Iterable<File> allInputFiles) {

        // Run threads to parse all files
        Iterator<File> iterInputFiles = allInputFiles.iterator();
        List<ArcFileReaderThread> threads = new ArrayList<ArcFileReaderThread> ();
        for (int i = 0; i < NUM_THREADS; i++) {
            ArcFileReaderThread t = new ArcFileReaderThread (iterInputFiles);
            threads.add (t);
            t.start();
        }

        // Wait for all threads to complete
        boolean errorsFound = false;
        for (ArcFileReaderThread t : threads) {
            while (t.isAlive()) {
                try {
                    t.join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
            if (t.hasErrors)
                errorsFound = true;
        }
        if (errorsFound)
            System.exit (1);

        // Collect each thread's results into a single map
        final HashMapOfArrayLists<String,Candidate> candidatesByUrlBase = new HashMapOfArrayLists<String,Candidate>();
        for (ArcFileReaderThread t : threads)
            candidatesByUrlBase.addAll (t.candidates);

        // Prune the structure a bit to save memory
        Iterator<Map.Entry<String,ArrayList<Candidate>>> iterEntries = candidatesByUrlBase.entrySet().iterator();
        while (iterEntries.hasNext()) {
            Map.Entry<String,ArrayList<Candidate>> entry = iterEntries.next();
            if (entry.getValue().size() < 2)
                iterEntries.remove ();
            else
                entry.getValue().trimToSize();
        }

        return candidatesByUrlBase;
    }


    public static List<CandidatePair> allCandidatePairs (Iterable<File> allInputFiles) {

        List<CandidatePair> ret = new ArrayList<CandidatePair>();
        for (Map.Entry<String,ArrayList<Candidate>> e : loadAllCandidatesByUrlBase(allInputFiles).entrySet()) {
            List<Candidate> candidates = e.getValue();

            ret.addAll (CCParc.pairUpCandidates (candidates));
        }

        return ret;
    }


    //-----------------------------------------------------------------------------------------------------------------------------
    // Iterate through CandidatePair objects, extract DocumentPair objects from them, and save the results to disk. This part
    // corresponds to the Reducer part in a Hadoop setup.

    public static void alignAndSaveAllDocPairs (final File outputDir, Iterable<CandidatePair> allCandidatePairs) {
        final Iterator<CandidatePair> iterCandidatePairs = allCandidatePairs.iterator();

        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0 ; i < NUM_THREADS; i++) {
            final int threadId = i;
            threads.add (new Thread (new Runnable () {
                public void run () {
                    for (;;) {
                        CandidatePair cpair;
                        synchronized (iterCandidatePairs) {
                            if (!iterCandidatePairs.hasNext())
                                break;
                            cpair = iterCandidatePairs.next();
                        }

                        
                        DocumentPair docPair;
                        try {
                            docPair = CCParc.alignCandidates (cpair);
                        } catch (SkippedDocumentPair ex) {
                            println (ex);
                            continue;
                        }

                        try {
                            saveParallelSentences (outputDir, docPair);
                        } catch (IOException ex) {
                            ex.printStackTrace();
                            break;
                        }

                    }
                    println ("Thread " + threadId + " done");
                }
            }));
        }

        for (Thread t : threads)
            t.start();
        for (Thread t : threads) {
            while (t.isAlive()) {
                try {
                    t.join();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        }
    }


    //-----------------------------------------------------------------------------------------------------------------------------
    // I/O utils

    /** Returns all the Arc data files we have */
    public static List<File> listDataFiles (File rootDir) {
        List<File> files = new ArrayList<File> ();
        listDataFiles (rootDir, files);
        return files;
    }

    /** Helper for the above */
    private static void listDataFiles (File file, List<File> accum) {
        if (file.isFile() && file.getName().endsWith (".arc.gz")) {
            accum.add (file);
        } else if (file.isDirectory()) {
            for (File child : file.listFiles())
                listDataFiles (child, accum);
        }
    }

    private static final Set<String> openOutputFiles = new HashSet<String>();

    /** Saves the parallel sentences from the given DocumentPair object into simple line-based text files */
    public static void saveParallelSentences (File outputDir, DocumentPair docPair)
        throws IOException
    {
        String hostname = docPair.enLui.hostname();
        String fileId = hostname + "." + docPair.enLui.lang + "-" + docPair.frLui.lang;

        File enFile = new File (outputDir, fileId + "." + docPair.enLui.lang + ".txt");
        File frFile = new File (outputDir, fileId + "." + docPair.frLui.lang + ".txt");

        synchronized (openOutputFiles) {
            while (openOutputFiles.contains(fileId)) {
                try {
                    openOutputFiles.wait();
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
            openOutputFiles.add (fileId);
        }

        if (!enFile.isFile()) {
            println ("> " + enFile);
            println ("> " + frFile);
        }

        try {
            final PrintWriter enWriter = new PrintWriter (new FileWriter (enFile, true));
            final PrintWriter frWriter = new PrintWriter (new FileWriter (frFile, true));

            try {
                CCParc.extractSentencePairs (docPair, new CCParc.SentencePairConsumer () {
                    public void consume (String enSentence, String frSentence) throws IOException {
                        enWriter.println (enSentence);
                        frWriter.println (frSentence);
                    }
                });
            } catch (Exception ex) {
                throw new Error (ex);
            } finally {
                enWriter.close();
                frWriter.close();
            }

        } finally {
            synchronized (openOutputFiles) {
                openOutputFiles.remove (fileId);
                openOutputFiles.notifyAll();
            }

        }
    }

    //-----------------------------------------------------------------------------------------------------------------------------

    public static void main (String[] args) {
        try {

            if (args.length != 2) {
                System.err.println ("usage: java Main <input-dir> <output-dir>");
                System.exit (-1);
            }

            File outputDir = new File (args[1]);
            if (outputDir.exists()) {
                System.err.println (outputDir + " exists, won't overwrite -- aborting");
                System.exit (1);
            }

            File inputDir = new File (args[0]);
            List<File> inputFiles = listDataFiles (inputDir);
            if (inputFiles.isEmpty()) {
                System.err.println ("No arc files found in " + inputDir + " -- aborting");
                System.exit (1);
            }

            outputDir.mkdirs();
            alignAndSaveAllDocPairs (
                outputDir,
                allCandidatePairs (inputFiles)
            );

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


}

//---------------------------------------------------------------------------------------------------------------------------------
