package ccparc;

import ccparc.structs.Language;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class TestLangCodes {

	public static void main(String[] args)
	{
		try{
			FileInputStream fstream = new FileInputStream(args[0]);
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;
			Map<Language,List<String>> languageIds = new HashMap<Language,List<String>> ();
			while ((strLine = br.readLine()) != null)   {
				if (strLine.startsWith("#")==false)
				{
					String[] elems = strLine.split("\t");
					languageIds.put (Language.ALL_LANGS_BY_IDS.get(elems[2]),Collections.unmodifiableList (Arrays.asList(elems)));
//					System.out.println(elems.length);
				}
			}
			System.out.println(languageIds.size());
			Iterator it = languageIds.entrySet().iterator();
		    while (it.hasNext()) {
		        Map.Entry pairs = (Map.Entry)it.next();
		        System.out.println(pairs.getKey() + " = " + pairs.getValue());
		    }

		}
		catch (Exception e){//Catch exception if any
			System.err.println("Error: " + e.getMessage());
		}


	}
}
