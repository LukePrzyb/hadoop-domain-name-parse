/* Domain String Parser
 * By: Luke (luka.a.programmer@gmail.com)
 * Last Updated: 3/1/2019
 *
 * A domain parsing program that takes advantage of Apache Hadoop's MapReduce
 * in order to quickly find all possible domain names in a large file and
 * to see number of occurences of said domain. Then, we parse the SLD of
 * the domain name (the name of the site that comes immediately after the TLD)
 * and make a best-guess string parser to perform the best possible word 
 * outcomes for the domain name, using a pipeline and comma delimiter.
 * 
 * Input: 
 * Any number of separated text files that contains domain
 * names in any format. This provides support for files such as ZONE files
 * used in DNS data dumps. Sadly, almost all up-to-date DNS servers no longer
 * allow zone data dumps.
 *
 * Output:
 * Line delimited results with key-value pairs tab delimited.
 * The Key is a unique single domain name, the value is a pipeline ("|") delimited
 * 3 result set of SLD, Multiple possible spelling parse outputs, and number of
 * occurences. The second result in the result set can be 1 or more proposed parsing outputs.
 */


import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.io.DataOutput;
import java.io.DataInput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DomainParse {

  protected static HashSet<String> dictionary = new HashSet<String>();
  protected static String dictionaryFileName = "dictionary.txt";


  // Map method is dead simple here: extract all domain names out of the files.
  public static class DomainParseMapper extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    // Following the K.I.S.S. style, we keep the regex as simple as possible.
    private Pattern p = Pattern.compile("\\S+(\\.\\S+)+");

    public void map(Object _key, Text _value, Context _context) throws IOException, InterruptedException {

      String line = _value.toString();
      Matcher m = p.matcher(line);

      // For each domain name found on the line, add it to our mapper
      while(m.find()){
        _context.write(new Text(m.group().toLowerCase()), one);
      }

      return;
    }
  }

  // Reduce function combines all the data into a simple key, value pair of domain names with number of occurences.
  // We combine to reduce redundancy on the final reducer step
  public static class DomainParseCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text _key, Iterable<IntWritable> _value, Context _context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : _value) {
        sum += val.get();
      }
      result.set(sum);
      _context.write(_key, result);
    }
  }

  // The real work happens here: with all the values combined, we can start extracting the sld and provide the
  // best fitting word to said format.
  public static class EndParseDomain extends Reducer<Text,IntWritable,Text,DomainOutputWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text _key, Iterable<IntWritable> values, Context _context) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      // result.set(sum);

      String keyString = _key.toString();

      // System.out.println(keyString);

      // SLD is defined as the domain that comes after the Top Level Domain (TLD)
      // Regex demands at least ONE period to be a valid domain name
      String[] domain = keyString.split("\\.");

      // System.out.println(domain.toString() + ", " + domain.length);

      String sld = domain[domain.length - 2];

      int sldLength = sld.length();

      ArrayList<WordSet> possibleWords = new ArrayList<WordSet>();

      for(int i=0; i<sldLength; ++i){
        for(int j=sldLength; j > i; --j){

          String word = sld.substring(i, j);

          if (dictionary.contains(word)) {
            possibleWords.add(new WordSet(word, i, j));
          }

        }
      }

      // List is empty, that means we had jibberish. Treat Jibberish as one word.
      if(possibleWords.isEmpty()){
        _context.write(_key, new DomainOutputWritable(sld, sld, sum));
      } else {

        // Sort by starting position, and length. First occurence to last, longest word to shortest.
        Collections.sort(possibleWords, new Comparator<WordSet>() {
          @Override
          public int compare(WordSet w1, WordSet w2) {
            if(w1.startPosition != w2.startPosition) {
              return w1.startPosition - w2.startPosition;
            }
            return w2.word.length() - w1.word.length();
          }
        });

        // Based on the well-known scheduling problem. 
        // Create the least amount of lists using all the words.
        ArrayList<ArrayList<WordSet>> possibleSet = new ArrayList<ArrayList<WordSet>>();

        for(WordSet wordCandidate : possibleWords) {
          // System.out.println(wordCandidate.word);

          boolean insertedWord = false;

          // Go through scheduled sets
          for(ArrayList<WordSet> set : possibleSet) {


            WordSet holder = set.get(set.size() - 1);

            if(holder.endPosition <= wordCandidate.startPosition) {
              set.add(wordCandidate);
              insertedWord = true;
              break;
            }

          }

          if(!insertedWord) {
            ArrayList<WordSet> newSet = new  ArrayList<WordSet>();
            newSet.add(wordCandidate);
            possibleSet.add(newSet);
          }
        }


        ArrayList<ArrayList<WordSet>> bestCombinations = new ArrayList<ArrayList<WordSet>>();

        int bestSum = 0;

        // Find the best combinations by the amount of words used. If there are
        // Equal as big sized sets. Include them.
        // Best solution is find by how many letters are used in the end parsing result.
        for(ArrayList<WordSet> newSets : possibleSet) {

          // Quick sum
          int newSum = newSets.stream().map(WordSet::wordLength).mapToInt(Integer::intValue).sum();

          if(bestSum < newSum) {
            bestCombinations.clear();
            bestCombinations.add(newSets);
            bestSum = newSum;
          }else if(bestSum == newSum) {
            bestCombinations.add(newSets);
          }

        }

        String bestSets = "";

        // Final string parsing.
        for(ArrayList<WordSet> wordSet : bestCombinations){

          int pos = 0;
          String optionString = "";

          for(WordSet w : wordSet) {
            if(pos != w.startPosition) {
              optionString += sld.substring(pos, w.startPosition) + " ";
            }

            optionString += w.word + " ";
            pos = w.endPosition;
          }

          optionString += sld.substring(pos);

          bestSets += optionString.trim() + ",";
        }

        // Write our new content out as 
        _context.write(_key, new DomainOutputWritable(sld, bestSets.substring(0,bestSets.length() - 1), sum));
      }

    }
  }

  // Word set class to store extra values on parsed string results
  public static class WordSet {
    public String word;
    public int startPosition;
    public int endPosition;

    public WordSet(String _word, int _startPosition, int _endPosition) {
      this.word = _word;
      this.startPosition = _startPosition;
      this.endPosition = _endPosition;
    }

    // Method built for easy and quick stream map expressions
    public int wordLength() {
      return this.word.length();
    }
  }


  // Class overriding the Writable class to provide a more powerful output format
  // Prints out the domain name's SLD, best guessed word parsing(s), and the number of domain name occurences.
  public static class DomainOutputWritable implements Writable {

    private Text sld;
    private Text sldParsed;
    private IntWritable occurences;

    public DomainOutputWritable(String _sld, String _sldParsed, int _occurences) {
      this.sld = new Text(_sld);
      this.sldParsed = new Text(_sldParsed);
      this.occurences = new IntWritable(_occurences);

      return;
    }

    @Override
    public void readFields(DataInput _in) throws IOException {
      // System.out.println("Reading");
      this.sld = new Text(_in.readLine());
      this.sldParsed = new Text(_in.readLine());
      this.occurences = new IntWritable(_in.readInt());

      return;
    }


    @Override
    public void write(DataOutput _out) throws IOException {
      // System.out.println("Writing");
      _out.writeBytes(this.sld.toString());
      _out.writeBytes(this.sldParsed.toString());
      _out.writeInt(this.occurences.get());

      return;
    }

    @Override
    public String toString() {
      return sld.toString() + "|" + sldParsed.toString() + "|" + occurences.toString();
    }
  }


  // Entry Method
  public static void main(String[] _args) throws Exception {
    if(_args.length >= 3){
      dictionaryFileName=_args[2];
    }

    // Prep the dictionary
    try (BufferedReader read = new BufferedReader(new FileReader(dictionaryFileName))) {
      String newLine;
      while((newLine = read.readLine()) != null){
        dictionary.add(newLine.trim().toLowerCase());
      }
    }catch (IOException _e){
      System.out.println("I/O Error when trying to open / read the dictionary. Is it in the same directory?");
      System.out.println("Arguments: Input-File Output-File Dictionary-Location");
      System.out.println(_e.toString());
      System.exit(1);
    }catch (Exception _e){
      System.out.println("Unknown issue occured. Configuration may be wrong?");
      System.out.println(_e.toString());
      System.exit(1);
    }


    Configuration conf = new Configuration();
    Job primaryJob = Job.getInstance(conf, "Domain Parse");
    primaryJob.setJarByClass(DomainParse.class);
    primaryJob.setMapperClass(DomainParseMapper.class);
    primaryJob.setCombinerClass(DomainParseCombiner.class);
    primaryJob.setReducerClass(EndParseDomain.class);

    primaryJob.setMapOutputKeyClass(Text.class);
    primaryJob.setMapOutputValueClass(IntWritable.class);

    // For simplicity's sake: Reduce task is set to 1.
    primaryJob.setNumReduceTasks(1);

    primaryJob.setOutputKeyClass(Text.class);
    // job.setOutputValueClass(IntWritable.class);
    // Overwrite standard output class for reducer to a custom one for more powerful output printing
    primaryJob.setOutputValueClass(DomainOutputWritable.class);

    FileInputFormat.addInputPath(primaryJob, new Path(_args[0]));
    FileOutputFormat.setOutputPath(primaryJob, new Path(_args[1]));

    System.exit(primaryJob.waitForCompletion(true) ? 0 : 1);
  }
}

