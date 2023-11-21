package test.jdbc.datasource;

import java.io.File;

import javax.sql.DataSource;

import org.apache.derby.jdbc.EmbeddedDataSource;
import org.springframework.beans.factory.config.AbstractFactoryBean;

public class DerbyDataSourceFactoryBean extends AbstractFactoryBean {

	private String dataDirectory = "derby-home";

	public void setDataDirectory(String dataDirectory) {
		this.dataDirectory = dataDirectory;
	}

	protected Object createInstance() throws Exception {
		File directory = new File(dataDirectory);
		System.setProperty("derby.system.home", directory.getCanonicalPath());
		System.setProperty("derby.storage.fileSyncTransactionLog", "true");
		System.setProperty("derby.storage.pageCacheSize", "100");

		final EmbeddedDataSource ds = new EmbeddedDataSource();
		ds.setDatabaseName("derbydb");
		ds.setCreateDatabase("create");
		ds.getConnection().close();
		ds.setCreateDatabase(null);

		return ds;
	}

	public Class getObjectType() {
		return DataSource.class;
	}

}