package cn.base.dfs.cli;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.log4j.Logger;

import cn.base.dfs.client.DFSClient;

public class OptionsProcessor {
	
	private static final Logger LOG = Logger.getLogger(OptionsProcessor.class);
	
	private final Options options = new Options();
	private CommandLineParser parser = new DefaultParser();
	private HelpFormatter formatter = new HelpFormatter();
	
	public OptionsProcessor() {
		options.addOption("ls", "lsr", true, "File system list");
		options.addOption("v", "version", false, "Current version information");
		options.addOption("h", "help", false, "dfs Help information");
	}
	
	public void run(String[] args) throws ParseException, InterruptedException {
		CommandLine line = parser.parse(options, args);
		if (line.hasOption("ls") || line.hasOption("lsr")) {
			DFSClient client = new DFSClient();
			client.listFiles(args[3]);
		} else if (line.hasOption("h") || line.hasOption("help")) {
			helpPrint();
		} else if (line.hasOption("v") || line.hasOption("version")) {
			System.out.println("0.10.0");
		}
	}
	
	public void helpPrint() {
		formatter.printHelp("dfs", options);
	}
	
	public static void main(String[] args) throws ParseException, InterruptedException {
//		args = new String[] {"dfs", "fs", "-lsr", "/"};
//		args = new String[] {"dfs", "fs", "-help"};
		args = new String[] {"dfs", "fs", "-v"};
		OptionsProcessor optionsProcessor = new OptionsProcessor();
		optionsProcessor.run(args);
	}
}
