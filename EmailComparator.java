package mongoDBClient;

import java.util.Comparator;

class EmailComparator implements Comparator<String> {
	 
    @Override
    public int compare(String str1, String str2) {
    	//System.out.println("str1:" + str1 + " str2:" + str2 + " result:" + str1.toLowerCase().compareTo(str2.toLowerCase()));
        return str1.toLowerCase().compareTo(str2.toLowerCase());
    }
     
}
