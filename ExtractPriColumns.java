/*
 * @brief introduction
 * @xiang_wang 2020年5月1日
 */
package tools;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.sql.ResultSet;
import java.sql.Statement;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;

/**
 * @author xiang_wang
 *
 */
public class ExtractPriColumns {


	  public static void main(String[] args) throws Exception {
		  if (args.length != 4) {
		      System.out.println("args incorrect. accept 4 params(url db user password)");
		      return;
		    }

		    String url = args[0];
		    String db = args[1];
		    String user = args[2];
		    String password = args[3];

		    System.out.println("connection " + url + " ...");

	    	DruidDataSource dataSource = new DruidDataSource();
	        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
	        dataSource.setUsername(user);
	        dataSource.setPassword(password);
	        dataSource.setUrl("jdbc:mysql://" + url);
	        dataSource.setInitialSize(1);
	        dataSource.setMinIdle(1);
	        dataSource.setMaxActive(5);
	        dataSource.init();
	        System.out.println("connecting " + url + " success");

	        DruidPooledConnection connection = dataSource.getConnection();
	        Statement statement = connection.createStatement();
			ResultSet result = statement.executeQuery("select TABLE_SCHEMA,TABLE_NAME,COLUMN_NAME,COLUMN_KEY from information_schema.COLUMNS where TABLE_SCHEMA = '" + db + "' and COLUMN_KEY = \"PRI\"");
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("result.csv")));
			Statement statementRes = connection.createStatement();
			while (result.next()) {
				String tb = result.getString(2);
				String pri = result.getString(3);
				System.out.println(tb + ":" + pri);
				ResultSet rsRecords = statementRes.executeQuery("select " + pri + " from " + db + "." + tb);
				StringBuilder sb = new StringBuilder();
				while (rsRecords.next()) {
					String priV = rsRecords.getString(1);
					sb.append(db + "\t" + tb + "\t" + pri + "\t" + priV + "\n");
				}
				if (sb.length() > 0) {
					bw.write(sb.toString());
				}
				rsRecords.close();
			}
			result.close();
			bw.close();
			dataSource.close();
	  }
}
