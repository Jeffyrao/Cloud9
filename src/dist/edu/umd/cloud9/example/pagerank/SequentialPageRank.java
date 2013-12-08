/*
 * Cloud9: A Hadoop toolkit for working with big data
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */


package edu.umd.cloud9.example.pagerank;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.regex.Pattern;
import java.io.PrintWriter;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;

import edu.uci.ics.jung.algorithms.cluster.WeakComponentClusterer;
import edu.uci.ics.jung.algorithms.importance.Ranking;
import edu.uci.ics.jung.algorithms.scoring.PageRank;
import edu.uci.ics.jung.graph.DirectedSparseGraph;

/**
 * <p>
 * Program that computes PageRank for a graph using the <a
 * href="http://jung.sourceforge.net/">JUNG</a> package (2.0 alpha1). Program takes two command-line
 * arguments: the first is a file containing the graph data, and the second is the random jump
 * factor (a typical setting is 0.15).
 * </p>
 * 
 * <p>
 * The graph should be represented as an adjacency list. Each line should have at least one token;
 * tokens should be tab delimited. The first token represents the unique id of the source node;
 * subsequent tokens represent its link targets (i.e., outlinks from the source node). For
 * completeness, there should be a line representing all nodes, even nodes without outlinks (those
 * lines will simply contain one token, the source node id).
 * </p>
 * 
 * @author Jimmy Lin
 */
public class SequentialPageRank {
  private SequentialPageRank() {
  }

  private static final String INPUT = "input";
  private static final String OUTPUT = "output";
  private static final String JUMP = "jump";

  @SuppressWarnings({ "static-access" })
  public static void main(String[] args) throws IOException {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("input path").create(INPUT));
    options.addOption(OptionBuilder.withArgName("path").hasArg()
        .withDescription("output path").create(OUTPUT));
    options.addOption(OptionBuilder.withArgName("val").hasArg()
        .withDescription("random jump factor").create(JUMP));

    CommandLine cmdline = null;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      System.exit(-1);
    }

    if (!cmdline.hasOption(INPUT)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(SequentialPageRank.class.getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      System.exit(-1);
    }

    String infile = cmdline.getOptionValue(INPUT);
    String outfile = cmdline.getOptionValue(OUTPUT);
    float alpha = cmdline.hasOption(JUMP) ? Float.parseFloat(cmdline.getOptionValue(JUMP)) : 0.15f;

    int edgeCnt = 0;
    DirectedSparseGraph<String, Integer> graph = new DirectedSparseGraph<String, Integer>();
    
    File folder = new File(infile);
    if(folder.isDirectory()){
      for(File file:folder.listFiles()){
        if(!file.getName().startsWith("part"))
          continue;
        String contents = FileUtils.readFileToString(file);
        String[] lines = contents.split("\\n");
        for(String line: lines){
          line = line.replaceAll(Pattern.quote("["), "");
          line = line.replaceAll(Pattern.quote("]"), "");
          line = line.replaceAll(Pattern.quote(",")," ");
          if (!line.equals("")) { // non-empty string 
            line.trim();
            String[] arr = line.split("\\s+");

            for (int i = 1; i < arr.length; i++) {
              graph.addEdge(new Integer(edgeCnt++), arr[0], arr[i]);
            }
          }
      }
    }
    }else{
      String contents = FileUtils.readFileToString(folder);
      String[] lines = contents.split("\\n");
      for(String line: lines){
          line = line.replaceAll(Pattern.quote("["), "");
          line = line.replaceAll(Pattern.quote("]"), "");
          line = line.replaceAll(Pattern.quote(",")," ");
          if (!line.equals("")) { // non-empty string 
            line.trim();
            String[] arr = line.split("\\s+");

          for (int i = 1; i < arr.length; i++) {
            graph.addEdge(new Integer(edgeCnt++), arr[0], arr[i]);
          }
        }
      }
    }
    
    PrintWriter writer = new PrintWriter(outfile,"UTF-8");
    WeakComponentClusterer<String, Integer> clusterer = new WeakComponentClusterer<String, Integer>();

    Set<Set<String>> components = clusterer.transform(graph);
    int numComponents = components.size();
    writer.println("Number of components: " + numComponents);
    writer.println("Number of edges: " + graph.getEdgeCount());
    writer.println("Number of nodes: " + graph.getVertexCount());
    writer.println("Random jump factor: " + alpha);

    // Compute PageRank.
    PageRank<String, Integer> ranker = new PageRank<String, Integer>(graph, alpha);
    ranker.evaluate();

    // Use priority queue to sort vertices by PageRank values.
    PriorityQueue<Ranking<String>> q = new PriorityQueue<Ranking<String>>();
    int i = 0;
    for (String pmid : graph.getVertices()) {
      q.add(new Ranking<String>(i++, ranker.getVertexScore(pmid), pmid));
    }

    // Print PageRank values.
    writer.println("\nPageRank of nodes, in descending order:");
    Ranking<String> r = null;
    while ((r = q.poll()) != null) {
      writer.println(r.rankScore + "\t" + r.getRanked());
    }
    writer.close();
  }
}
