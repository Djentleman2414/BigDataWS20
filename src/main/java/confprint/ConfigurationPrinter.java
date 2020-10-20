package confprint;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ConfigurationPrinter extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		for (Entry<String,String> entry : conf)
			System.out.printf("%s=%s\n",entry.getKey(),entry.getValue());
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int exitcode = ToolRunner.run(new ConfigurationPrinter(), args);
		System.exit(exitcode);
	}

}
