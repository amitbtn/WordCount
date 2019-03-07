import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.LongAdder;

/**
 *  @Auther amit
 *  This class takes files and count the number of each word of those filed.
 * */
public class WordCounter {
    //multiple threads increment this fields thus should be volatile
    static volatile int counter=0;

    private ConcurrentHashMap<String,LongAdder> map = new ConcurrentHashMap<String,LongAdder>();


    public static void main (String [] args) {
        WordCounter wc = new WordCounter();
        // load text files in parallel
        wc.load("C:\\devl\\files\\a.txt", "C:\\devl\\files\\b.txt", "C:\\devl\\files\\c.txt");
        // display words statistics
        wc.displayStatus();
    }

    private void load(String ...args) {
        int numOfFiles = args.length;
        ExecutorService executor = Executors.newFixedThreadPool(numOfFiles);
        for (int i = 0; i <numOfFiles ; i++) {
            WordCountParallel wcp = new WordCountParallel(args[i],map);
            executor.execute(wcp);
        }
        executor.shutdown();
    }

    private void displayStatus() {
        map.forEach((k,v)-> System.out.println(k+" "+v)); // O(n)
        System.out.println("\n\n**total: " + counter); //O(1)
    }


    public class WordCountParallel implements Runnable{

        String fileName;
        ConcurrentHashMap map;
        public WordCountParallel(String fileName, ConcurrentHashMap<String,LongAdder> map) {
            this.fileName  = fileName;
            this.map = map;

        }

        @Override
        public void run() {
            //read the file and put it in the map
            FileReader fr = null;
            try {
                fr = new FileReader(fileName);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            BufferedReader br = new BufferedReader (fr);
            try {
                String line = br.readLine();
                while (line != null) {
                    String []parts = line.split(" ");
                    for( String w : parts)
                    {
                        map.putIfAbsent(w, new LongAdder());
                        ((LongAdder) map.get(w)).increment();
                        counter++;
                    }
                    line = br.readLine();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
