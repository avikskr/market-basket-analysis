package mit.mba.hadoop;

import java.util.ArrayList;
import java.util.List;

public class Combination {

	public static List<String> getItemSets(String[] items) {
		List<String> itemsets = new ArrayList<String>();
		int n = items.length;
		int[] masks = new int[n];

		for (int i = 0; i < n; i++)
			masks[i] = (1 << i);

		for (int i = 0; i < (1 << n); i++) {

			List<String> newList = new ArrayList<String>(n);

			for (int j = 0; j < n; j++) {

				if ((masks[j] & i) != 0) {
					newList.add(items[j]);
					// System.out.println("(if) "+items[j]+" , mask["+j+"] : "+masks[j]+ " , i =
					// "+i);
				}

				if (j == n - 1 && newList.size() > 0 && newList.size() <= 5) {
					itemsets.add(newList.toString().replaceAll("\\s+",""));
					// System.out.println("###########");
				}
			}
		}

		return itemsets;
	}
}
