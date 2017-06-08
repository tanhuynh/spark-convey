package com.adobe.mcdp.campaign;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function2;
import scala.runtime.AbstractFunction1;

//https://unbareablelightness.tumblr.com/post/140531306084/writing-a-spark-dataframe-to-mysql-tips-and
public class Convey implements Serializable {

	private static final String USERS_JSON = "/Users/thuynh/Work/onboard/spark-convey/src/resources/users.json";
	private static final String ADDRESSES_JSON = "/Users/thuynh/Work/onboard/spark-convey/src/resources/addresses.json";

	private static final String MYSQL_PWD = "expertuser123";
	private static final String MYSQL_CONNECTION_URL = "jdbc:sqlserver://cob-sql.database.windows.net:1433;database=cob-sql-dw;user=sqladmin@cob-sql;password=Pa$$w0rd;encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;";
	// campaign-dw.database.windows.net
	// "jdbc:mysql://localhost:3306/employees?user=" + MYSQL_USERNAME +
	// "&password=" + MYSQL_PWD;
	private static final int NUMBER_OF_WORKERS = 2;
	private static final String UPSERT_RECORD_SQL = "IF NOT EXISTS (SELECT * FROM Users WHERE email = ?) BEGIN"
			+ // 1
			" INSERT INTO Users(_id, id, isActive, balance, age, eyeColor, name, email)"
			+ "  VALUES(?, ?, ?, ?, ?, ?, ?, ?)"
			+ // 2-9
			" END "
			+ " ELSE "
			+ " BEGIN "
			+ "  UPDATE Users "
			+ "  SET _id=?, id=?, isActive=?, balance=?, age=?, eyeColor=?, name=?, email=?"
			+ // 10-17
			" WHERE email = ?" + // 18
			" END";

	private static final JavaSparkContext sc = new JavaSparkContext(
			new SparkConf().setMaster("local[*]"));

	private static Connection getDbConnection() throws ClassNotFoundException,
			SQLException {
		// Declare the JDBC objects.
		Connection con = null;

		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		con = DriverManager.getConnection(MYSQL_CONNECTION_URL);
		return con;
	}

	private static void batchUpsertRecordsIntoTable(final Row row)
			throws SQLException, ClassNotFoundException {

		Connection dbConnection = null;
		PreparedStatement preparedStatement = null;
		try {
			dbConnection = getDbConnection();

			preparedStatement = dbConnection
					.prepareStatement(UPSERT_RECORD_SQL);
			dbConnection.setAutoCommit(false);
			/*
			 * preparedStatement.setInt(1, 101); preparedStatement.setString(2,
			 * "mkyong101"); preparedStatement.setString(3, "system");
			 * preparedStatement.setTimestamp(4, getCurrentTimeStamp());
			 * preparedStatement.addBatch();
			 * 
			 * preparedStatement.setInt(1, 102); preparedStatement.setString(2,
			 * "mkyong102"); preparedStatement.setString(3, "system");
			 * preparedStatement.setTimestamp(4, getCurrentTimeStamp());
			 * preparedStatement.addBatch();
			 * 
			 * preparedStatement.setInt(1, 103); preparedStatement.setString(2,
			 * "mkyong103"); preparedStatement.setString(3, "system");
			 * preparedStatement.setTimestamp(4, getCurrentTimeStamp());
			 * preparedStatement.addBatch();
			 */
			preparedStatement.executeBatch();

			dbConnection.commit();

			System.out.println("Record is inserted into DBUSER table!");

		} catch (SQLException e) {

			System.out.println(e.getMessage());
			dbConnection.rollback();

		} finally {

			if (preparedStatement != null) {
				preparedStatement.close();
			}

			if (dbConnection != null) {
				dbConnection.close();
			}

		}

	}

	private static class User implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = -4481999054495346171L;
		private String _id;
		private long id;
		private boolean isActive;
		private String balance;
		private int age;
		private String eyeColor;
		private String name;
		private String email;

		public User() {

		}

		public String get_id() {
			return _id;
		}

		public void set_id(String _id) {
			this._id = _id;
		}

		public long getId() {
			return id;
		}

		public void setId(long id) {
			this.id = id;
		}

		public boolean getIsActive() {
			return isActive;
		}

		public void setIsActive(boolean isActive) {
			this.isActive = isActive;
		}

		public String getBalance() {
			return balance;
		}

		public void setBalance(String balance) {
			this.balance = balance;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getEyeColor() {
			return eyeColor;
		}

		public void setEyeColor(String eyeColor) {
			this.eyeColor = eyeColor;
		}

		public String getEmail() {
			return email;
		}

		public void setEmail(String email) {
			this.email = email;
		}

		public String toString() {
			return "id: " + id + " email:" + email;
		}
	}

	private static void printDataSet1(final Dataset<Row> dataset) {
		dataset.foreach((ForeachFunction<Row>) row -> System.out.println(row));

	}

	private static void printDataSet(final Dataset<Row> dataset) {
		StructType struct = dataset.schema();
		StructField fields[] = struct.fields();// [StructField(_id,StringType,true),
												// StructField(age,LongType,true),
												// StructField(balance,StringType,true),
												// StructField(email,StringType,true),
												// StructField(eyeColor,StringType,true),
												// StructField(id,LongType,true),
												// StructField(isActive,BooleanType,true),
												// StructField(name,StringType,true)]
		StringBuffer sb = new StringBuffer();
		dataset.foreach(new ForeachFunction<Row>() {
			public void call(Row row) throws Exception {
				for (int i = 0; i < fields.length; i++) {
					System.out.print(fields[i].name() + "=");
					String type = fields[i].dataType().typeName();
					if (type.equals("string")) {
						System.out.print(" " + row.getString(i) + "; ");
					} else if (type.equals("long")) {
						System.out.print(" " + row.getLong(i) + "; ");
						;
					} else if (type.equals("boolean")) {
						System.out.print(" " + row.getBoolean(i) + "; ");
					} else {
						System.out.println("Unknown type:" + type);
					}

				}
				System.out.println("\n");
			}
		});
		System.out.println(sb);

	}

	

	public static abstract class SerializableFunction1<T1, R> extends
			AbstractFunction1<T1, R> implements Serializable {
	}

	@SuppressWarnings("unchecked")
	private static void upsertToDB(final Dataset<Row> dataset) {
		SerializableFunction1 sf1 = new SerializableFunction1<scala.collection.Iterator<GenericRowWithSchema>, 
				scala.collection.Iterator<Row>>() {
			List<Row> retVal = new ArrayList<Row>();

			@Override
			public scala.collection.Iterator<Row> apply(scala.collection.Iterator<GenericRowWithSchema> users) {
				System.out.println("****");
				for (; users.hasNext();) {
					GenericRowWithSchema user = users.next();
//					retVal.add(RowFactory.create(user.getId(), user.getEmail(), user.getName(), user.getBalance(),
//							user.get_id(), user.getAge(), user.getIsActive()));
					System.out.println(user);
				}
				return scala.collection.JavaConverters.asScalaIteratorConverter(retVal.iterator()).asScala();
			}

			
			
		};
		
  		Encoder<User> userEncoder = Encoders.bean(User.class);
    	Dataset<User> d = dataset.coalesce(NUMBER_OF_WORKERS).mapPartitions(sf1, userEncoder);
    	d.count();
    }

	
	
	private static void writeToDB(final Dataset<User> dataset) {
		/*
		 * A common naive mistake is to open a connection on the Spark driver
		 * program, and then try to use that connection on the Spark workers.
		 * The connection should be opened on the Spark worker, such as by
		 * calling forEachPartition and opening the connection inside that
		 * function. Use partitioning to control the parallelism for writing to
		 * your data storage. Your data storage may not support too many
		 * concurrent connections. Use batching for writing out multiple objects
		 * at a time if batching is optimal for your data storage. Make sure
		 * your write mechanism is resilient to failures. Writing out a very
		 * large dataset can take a long time, which increases the chance
		 * something can go wrong - a network failure, etc. Consider utilizing a
		 * static pool of database connections on your Spark workers. If you are
		 * writing to a sharded data storage, partition your RDD to match your
		 * sharding strategy. That way each of your Spark workers only connects
		 * to one database shard, rather than each Spark worker connecting to
		 * every database shard.
		 */
		/*
		 * dataframe.coalesce("NUMBER OF WORKERS").mapPartitions((d) =>
		 * Iterator(d)).foreach { batch => val dbc: Connection =
		 * DriverManager.getConnection("JDBCURL") val st: PreparedStatement =
		 * dbc.prepareStatement("YOUR PREPARED STATEMENT")
		 * 
		 * batch.grouped("# Of Rows you want per batch").foreach { session =>
		 * session.foreach { x => st.setDouble(1, x.getDouble(1)) st.addBatch()
		 * } st.executeBatch() } dbc.close() }
		 */

		dataset.show();
		Function2 iterateUsers = new Function2<Integer, Iterator<User>, Iterator<User>>() {
			@Override
			public Iterator<User> call(Integer ind, Iterator<User> iterator)
					throws Exception {
				System.out.println("Function2 iterator" + ind);

				for (User user = iterator.next(); iterator.hasNext();) {
					System.out.println(user.getEmail());
				}
				return iterator;
			}
		};

		// dataset.coalesce(NUMBER_OF_WORKERS).foreach((ForeachFunction<Row>)
		// row -> System.out.println(row));
		Encoder<User> userEncoder = Encoders.bean(User.class);
		JavaRDD<User> javaRdd = JavaRDD.fromRDD(dataset./*
														 * .coalesce(
														 * NUMBER_OF_WORKERS)
														 */rdd(), null);
		System.out.println(javaRdd.toDebugString());
		System.out.println("Num Partitions: " + javaRdd.partitions().size());
		javaRdd.mapPartitionsWithIndex(iterateUsers, false);

	}

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder()
				.appName("Java Spark SQL basic")
				.config("spark.some.config.option", "some-value")
				.sparkContext(sc.sc()).getOrCreate();
		Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
		logger.setLevel(Level.FINE);
		PropertyDescriptor[] props;
		/*
		try {
			props = Introspector.getBeanInfo(User.class)
					.getPropertyDescriptors();
			for (PropertyDescriptor prop : props) {
				System.out.println(prop.getDisplayName());
				System.out.println("\t" + prop.getReadMethod());
				System.out.println("\t" + prop.getWriteMethod());
			}
		} catch (IntrospectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
*/
		// Sample data-frame loaded from a JSON file
		Dataset<User> dfUsers = spark.read().json(USERS_JSON)
				.as(Encoders.bean(User.class));
		// Displays the content of the DataFrame to stdout
		System.out.println("Users");
		dfUsers.show(20, false);
		// Print the schema in a tree format
		dfUsers.printSchema();

		// Sorting
		// Sort user. TODO: I don't know how to express asc/desc in java :(
		Dataset<User> dfSortedUsers = dfUsers.orderBy("age");
		// Displays the content of the DataFrame to stdout
		System.out.println("Sorted users");
		dfSortedUsers.show(20, false);

		// Loading addresses json
		Dataset<Row> dfAddresses = spark.read().json(ADDRESSES_JSON);
		System.out.println("Addresses");
		// Displays the content of the DataFrame to stdout
		dfAddresses.show(20, false);
		dfAddresses.printSchema();

		// Joining
		Dataset<Row> dfJoined = dfSortedUsers.join(dfAddresses, "email");
		dfJoined.show(20, false);
		dfJoined.printSchema();

		// Save data-frame to Datawarehouse using JDBC
		// Choose one of 2 options depending on your requirement (Not both).
		Properties connectionProperties = new Properties();
		// connectionProperties.put("isolationLevel", "READ_COMMITTED");
		//df.write().partitionBy("email").saveAsTable("users");
		// Saving data to a JDBC source
		
		dfUsers.write(). mode(SaveMode.Append). jdbc(MYSQL_CONNECTION_URL,
		 "Users", connectionProperties);
		 

		//writeToDB3(spark.read().json(USERS_JSON));
	}
}
