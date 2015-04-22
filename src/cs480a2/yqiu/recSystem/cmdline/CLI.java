package cs480a2.yqiu.recSystem.cmdline;

import java.io.*;
import java.util.*;

/**
 * Created by yqiu on 4/3/15.
 * Command line interface
 * The command line takes book title as input and returns top serveral related books
 */
public class CLI {

    private final int NUMBER_OF_RETURN_BOOKS = 30;

    private HashMap<String, Map<String, Double>> simMatrix = new HashMap<String, Map<String, Double>>();

    private Map<String, Double> getVector(String title) {
        Map<String, Double> simVector = simMatrix.get(title);

        if (simVector == null) {
            simVector = new HashMap<String, Double>();
        }

        return simVector;
    }

    public CLI(String filename) throws IOException {
        File file = new File(filename);
        BufferedReader bufferedReader = new BufferedReader(new FileReader(file));

        try {
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] lineSplit = line.split("\\t");
                String[] titles = lineSplit[0].split("\\|");

                Map<String, Double> book1_Vector = getVector(titles[0]);
                Map<String, Double> book2_Vector = getVector(titles[1]);

                Double similarity = Double.parseDouble(lineSplit[1]);
                book1_Vector.put(titles[1], similarity);
                book2_Vector.put(titles[0], similarity);
                simMatrix.put(titles[0], book1_Vector);
                simMatrix.put(titles[1], book2_Vector);
            }

            System.out.println("Finished reading " + simMatrix.size() + " books from database");
            System.out.println("All available books are listed below:");
            int i = 1;
            for (String title : simMatrix.keySet()) {
                System.out.println(i + ". " + title);
                i++;
            }

        } catch (IOException e) {
            System.out.println("Error happened when reading line");
        } finally {
            bufferedReader.close();
        }
    }

    private void getRecommendation(String targetBook) {
        if (!simMatrix.containsKey(targetBook)) {
            System.out.println("Book is not found. Please try again.");
            return;
        }

        HashSet<String> recommendations = new HashSet<String>();

        Map<String, Double> similartyVector = simMatrix.get(targetBook);

        for (String book : similartyVector.keySet()) {
            if (book.equalsIgnoreCase(targetBook)) {
                continue;
            }

            if (recommendations.size() >= NUMBER_OF_RETURN_BOOKS) {

                recommendations.add(book);
                double leastSimValue = -1;

                String toRemove = book;

                for (String recommendationBookTitle : recommendations) {
                    double bookSimVal = similartyVector.get(recommendationBookTitle);
                    if (bookSimVal > leastSimValue) {
                        leastSimValue = bookSimVal;
                        toRemove = recommendationBookTitle;
                    }
                }
                recommendations.remove(toRemove);
                continue;
            }

            recommendations.add(book);
        }

        int count = 1;

        String[] recommendationBooks = new String[recommendations.size()];
        recommendations.toArray(recommendationBooks);
        Arrays.sort(recommendationBooks);

        for (String book : recommendationBooks) {
            System.out.println("\t" + count + ". " + book);
            count++;
        }
    }

    public static void main(String[] args) {

        try {
            CLI cli = new CLI(args[0]);

            Scanner scan = new Scanner(System.in);
            scan.useDelimiter("\\n");
            while (true) {
                System.out.println("Enter the name of a book: ");
                String bookToSearch = scan.next();
                cli.getRecommendation(bookToSearch);
            }

        } catch (FileNotFoundException e) {
            System.out.println("Input file is not found! " + args[0]);
            System.exit(1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
