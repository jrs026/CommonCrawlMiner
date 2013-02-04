
package ccparc;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;


public class Tokenizer {
	public static boolean endsabbrev(String s,ArrayList<String> abbrevs)
	{
		for (String elem:abbrevs)
		{
			if (s.endsWith(elem))
			{
				return true;
			}
			
		}
	
		return false;
	}
	

	// Last argument (optional) is the language code. Default is English
	public static ArrayList<String> tokenize(String s, ArrayList<String> abbrevs,ArrayList<String> numabbrevs,String...language) {
		ArrayList<String> result=new ArrayList<String>();
		String[] sentences = s.split("[\\.\\?\\!]");

		for (int i = 0;i<sentences.length;i++){
			sentences[i]=sentences[i].replaceAll("^\\s?(.*)\\s?$", "$1");
			if (endsabbrev(sentences[i],abbrevs))
			{
				int j=i;
				while (endsabbrev(sentences[j],abbrevs)==true || endsabbrev(sentences[j],numabbrevs))
				{
					sentences[j+1]=sentences[j+1].replaceAll("^\\s?(.*)\\s?$", "$1");
					if (endsabbrev(sentences[j],numabbrevs)&&Character.isDigit(sentences[j+1].charAt(0))==false)
					{
						break;
					}
					sentences[i]+=". "+sentences[j+1];
					j++;
				}
				result.add(sentences[i]);
				i = j;	
				continue;
			}
			if (endsabbrev(sentences[i],numabbrevs))
			{
				sentences[i+1]=sentences[i+1].replaceAll("^\\s?(.*)\\s?$", "$1");
				if (Character.isDigit(sentences[i+1].charAt(0)))
				{
				sentences[i]+=". "+sentences[i+1];
				result.add(sentences[i]);
				i++;
				continue;
				}
			}
//			System.out.println(sentences[i]);
			result.add(sentences[i]);

		}
		String lang;
		if (language.length==1){
			lang=language[0];
		}
		else
		{
			lang="en";
		}
		for (String sentence: result){
//			sentence = sentence.replaceAll("([\\`\\,\\-\\:\\;])", " $1 ");
			if (sentence.contains("'")&& (lang.equals("en") || lang.equals("de")) ){
				result.set(result.indexOf(sentence),sentence.replaceAll("(\\')", " $1"));
			}
			if (sentence.contains("'")&& (lang.equals("fr") || lang.equals("it"))){
				result.set(result.indexOf(sentence),sentence.replaceAll("(\\')", "$1 "));
			}
		}
		return result;
	}

public static ArrayList<String> tokenizefromFile(String s, ArrayList<String> abbrevs,ArrayList<String> numabbrevs) {
	
	String content ="";
	try{
		FileInputStream fstream = new FileInputStream(s);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));

		String strLine;
		while ((strLine = br.readLine()) != null)   {
			content+=strLine+" ";
		}

		in.close();
	}catch (Exception e){//Catch exception if any
		System.err.println("Error: " + e.getMessage());

	}
	return tokenize(content,abbrevs,numabbrevs);

}
public static void main(String[] args) {
	
	ArrayList<String> abbrev=new ArrayList<String>();
	ArrayList<String> numberabbrev=new ArrayList<String>(Arrays.asList("No","Nos","Art","Nr","pp"));
	try{
		FileInputStream fstream = new FileInputStream(args[0]);
		DataInputStream in = new DataInputStream(fstream);
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String strLine;

		while ((strLine = br.readLine()) != null)   {
			if (strLine.startsWith("#")==false && strLine.contains("#")==false && strLine.length()!=0){
				abbrev.add(strLine);
			}
		}
		in.close();
		ArrayList<String> output = tokenizefromFile("sample.txt",abbrev,numberabbrev);
		FileWriter foutstream = new FileWriter("out.txt");
		BufferedWriter out = new BufferedWriter(foutstream);
		

		for (String s:output)
		{
			System.out.println(s);
			out.write(s+"\n");
		}
		out.close();

	}catch (Exception e){//Catch exception if any
		System.err.println("Error: " + e.getMessage());
	}
	

	
}

}

