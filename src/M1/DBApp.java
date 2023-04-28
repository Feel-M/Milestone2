package M1;

import exceptions.DBAppException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.io.*;

public class DBApp {

	public DBApp() {

	}

	public void init() {
	}

	public void createTable(String strTableName,
			String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax)
			throws IOException, DBAppException {
		DBApp dbApp = new DBApp();

		// metadata file
		File meta = new File("src" + File.separator + "MetaData.csv");
		boolean metafile;
		metafile = meta.createNewFile();

		if (metafile == false) {
			FileWriter fileWriter = new FileWriter("src" + File.separator + "MetaData.csv", true);
			Enumeration<String> type = htblColNameType.keys();
			Enumeration<String> min = htblColNameMin.keys();
			Enumeration<String> max = htblColNameMax.keys();
			for (int i = 0; i < htblColNameType.size(); i++) {

				String keytype = type.nextElement();
				String keymin = min.nextElement();
				String keymax = max.nextElement();
				fileWriter.write(strTableName + ",");
				fileWriter.write(keytype + ",");
				try {
					String[] types = { "java.lang.Integer", "java.lang.Double", "java,lang.Date", "java.lang.String" };

					if (check(types, htblColNameType.get(keytype))) {
						fileWriter.write(htblColNameType.get(keytype) + ",");
					} else {
						throw new DBAppException("Unsupported Data Types");
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}

				if (strClusteringKeyColumn.equals(keytype)) {
					fileWriter.write("True,");
					fileWriter.write("Null,");
					fileWriter.write("Null,");
					fileWriter.write(htblColNameMin.get(keymin) + ",");
					fileWriter.write(htblColNameMax.get(keymax) + "\n");

				} else {

					fileWriter.write("False,");
					fileWriter.write("Null,");
					fileWriter.write("Null,");
					fileWriter.write(htblColNameMin.get(keymin) + ",");
					fileWriter.write(htblColNameMax.get(keymax) + "\n");

				}

			}
			fileWriter.flush();
			fileWriter.close();
		} else {
			FileWriter fileWriter = new FileWriter("src" + File.separator + "MetaData.csv");
			fileWriter.write("Table Name,");
			fileWriter.write("Colum Name,");
			fileWriter.write("Colum Type,");
			fileWriter.write("ClusteringKey,");
			fileWriter.write("IndexName,");
			fileWriter.write("IndexType,");
			fileWriter.write("min,");
			fileWriter.write("max \n");
			Enumeration<String> type = htblColNameType.keys();
			Enumeration<String> min = htblColNameMin.keys();
			Enumeration<String> max = htblColNameMax.keys();
			for (int i = 0; i < htblColNameType.size(); i++) {

				String keytype = type.nextElement();
				String keymin = min.nextElement();
				String keymax = max.nextElement();
				fileWriter.write(strTableName + ",");
				fileWriter.write(keytype + ",");
				try {
					if ((htblColNameType.get(keytype).getClass().getName()).equals("java.lang.Integer")
							|| (htblColNameType.get(keytype).getClass().getName()).equals("java.lang.String")
							|| (htblColNameType.get(keytype).getClass().getName()).equals("java.lang.Double")
							|| (htblColNameType.get(keytype).getClass().getName()).equals("java.lang.Date")) {
						fileWriter.write(htblColNameType.get(keytype) + ",");
					} else {
						throw new DBAppException("Unsupported Data Types");
					}
				} catch (Exception e) {
					e.printStackTrace();
					System.exit(0);
				}

				if (strClusteringKeyColumn.equals(keytype)) {
					fileWriter.write("True,");
					fileWriter.write("Null,");
					fileWriter.write("Null,");
					fileWriter.write(htblColNameMin.get(keymin) + ",");
					fileWriter.write(htblColNameMax.get(keymax) + "\n");

				} else {

					fileWriter.write("False,");
					fileWriter.write("Null,");
					fileWriter.write("Null,");
					fileWriter.write(htblColNameMin.get(keymin) + ",");
					fileWriter.write(htblColNameMax.get(keymax) + "\n");

				}

			}
			fileWriter.flush();

			fileWriter.close();

		}
		// table
		new File("src" + File.separator + "Tables").mkdir();
		File f = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		boolean f1;
		f1 = f.createNewFile();

		// files file
		// data file
		File Files = new File("src" + File.separator + "Files.csv");
		boolean datafile;
		datafile = Files.createNewFile();

		if (datafile == false) {
			FileWriter fileWriter = new FileWriter("src" + File.separator + "Files.csv", true);
			fileWriter.write(strTableName + ",");
			fileWriter.write("Table" + ",");
			fileWriter.write(strTableName + "1.csv,");
			fileWriter.write(f.getAbsolutePath() + "\n");
			fileWriter.close();

		} else {

			FileWriter fileWriter = new FileWriter("src" + File.separator + "Files.csv", true);
			fileWriter.write("Name" + ",");
			fileWriter.write("Type" + ",");
			fileWriter.write("FileName" + ",");
			fileWriter.write("FileLocationonHarddisk" + "\n");
			fileWriter.write(strTableName + ",");
			fileWriter.write("Table" + ",");
			fileWriter.write(strTableName + "1.csv,");
			fileWriter.write(f.getAbsolutePath() + "\n");
			fileWriter.close();
		}

		if (f1 == false) {
			throw new DBAppException("TABLE ALREADY EXISTS!");
		} else {

		}

		// MetaData file

	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {

		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		long count = 0;
		String clusterKey = "";
		String line = "";
		int clusterkeyindex= 0;
		boolean flag = false;
		String path = "";

		try {
			if (file.exists()) {

				try {

					Enumeration<String> hash = htblColNameValue.keys();

					while (hash.hasMoreElements()) {
						String key = hash.nextElement();

						BufferedReader maxreader = new BufferedReader(
								new FileReader("src" + File.separator + "MetaData.csv"));
						line = maxreader.readLine();
						while (((line = maxreader.readLine()) != null)) {

							if ((line).equals("")) {
								break;
							}
							String[] indexed = line.split(",");
							if (indexed[3].equals("True") && indexed[0].equals(strTableName)) {
								clusterKey = (htblColNameValue.get(indexed[1])).toString();
							}
							Double min = (Double.parseDouble(indexed[6]));
							Double max = (Double.parseDouble(indexed[7]));
							if (indexed[0].equals(strTableName) && flag == false) {
								clusterkeyindex++;
								if (indexed[3].equals("True") ) {
									flag = true;
									clusterkeyindex -=1;
								}
							}

							if ((htblColNameValue.get(key).getClass().getName()).equals("java.lang.String")
									&& indexed[1].equals(key) && indexed[0].equals(strTableName)) {
								if (((String) htblColNameValue.get(key)).length() < min
										|| ((String) htblColNameValue.get(key)).length() > max) {
									System.out.println(((String) htblColNameValue.get(key)).length());
									throw new DBAppException(key + " is not within Range!");

								}

							}

							else if (indexed[1].equals(key) && indexed[0].equals(strTableName)) {
								Object obj = htblColNameValue.get(key);
								String str = obj.toString();
								double value = Double.valueOf(str).doubleValue();
								if (value < min || value > max) {
									throw new DBAppException(key + " is not within Range!");
								}

								if ((htblColNameValue.get(key).getClass().getName()).equals(indexed[2])
										&& indexed[0].equals(strTableName)) {
									continue;

								} else {
									maxreader.close();
									throw new DBAppException("invalid Data Types");

								}
							}

						}

					}
				} catch (Exception e) {
					e.printStackTrace();
					return;
				}

				for (int i = 1; i <= 10; i++) {
					path = "src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv";
					File check = new File(
							"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
					if (check.exists()) {
						Path filecount = Paths
								.get("src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
						BufferedReader br = new BufferedReader(
								new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i
										+ ".csv"));
						count = Files.lines(filecount).count();

						Properties prop = new Properties();
						String propFileName = "DBApp.config";
						InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
						if (inputStream != null) {
							prop.load(inputStream);
						} else {
							throw new FileNotFoundException(
									"property file '" + propFileName + "' not found in the classpath");
						}

						while ((line = br.readLine()) != null) {
							if ((line).equals("")) {
								break;
							}
							String[] indexed = line.split(",");
							if (check(indexed, clusterKey)) {
								br.close();
								throw new DBAppException("Duplicates of clustering key values detected!");
							}
						}
						int max = Integer.parseInt(prop.getProperty("Max"));
						if (count < max) {
							FileWriter fileWriter = new FileWriter(
									"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv",
									true);
							BufferedWriter bw = new BufferedWriter(fileWriter);

							Enumeration<String> e = htblColNameValue.keys();

							while (e.hasMoreElements()) {
								String key = e.nextElement();
								fileWriter.write(htblColNameValue.get(key) + ",");
							}
							fileWriter.write("\n");
							fileWriter.flush();
							fileWriter.close();
							sorter(clusterkeyindex , path);
							break;

						} else {
							continue;

						}
					} else {
						Boolean create = check.createNewFile();
						FileWriter fileWriter = new FileWriter(
								"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv", true);
						BufferedWriter bw = new BufferedWriter(fileWriter);

						Enumeration<String> e = htblColNameValue.keys();

						while (e.hasMoreElements()) {
							String key = e.nextElement();
							bw.write(htblColNameValue.get(key) + ",");
						}
						bw.write("\n");
						bw.flush();
						bw.close();
						File Files = new File("src" + File.separator + "Files.csv");
						FileWriter datafile = new FileWriter("src" + File.separator + "Files.csv", true);
						datafile.write(strTableName + ",");
						datafile.write("Table" + ",");
						datafile.write(strTableName + i + ".csv,");
						datafile.write(check.getAbsolutePath() + "\n");
						datafile.close();
						break;

					}
				}
			} else {
				throw new DBAppException("Table Doesnt exist!");
			}
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

	}

	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		if (file.exists()) {

			String newfile = "new.csv";

			String line = "";
			for (int i = 1; i <= 10; i++) {
				try (BufferedReader br = new BufferedReader(
						new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv"));
						FileWriter fw = new FileWriter("src" + File.separator + "Tables" + File.separator + newfile)) {
					String current = strTableName + i + ".csv";
					File f = new File("src" + File.separator + "Tables" + File.separator + current);
					File nf = new File("src" + File.separator + "Tables" + File.separator + newfile);
					boolean f1;
					f1 = nf.createNewFile();
					while ((line = br.readLine()) != null) {
						String indexed[] = line.split(",");

						if (check(indexed, strClusteringKeyValue)) {
							Enumeration<String> e = htblColNameValue.keys();

							while (e.hasMoreElements()) {
								String key = e.nextElement();
								fw.write(htblColNameValue.get(key) + ",");

							}
							fw.write("\n");

						} else {
							fw.write(line + "\n");
						}
					}
					fw.close();
					br.close();
					File deletFile = new File("src" + File.separator + "Tables" + File.separator + current);
					deletFile.canWrite();
					deletFile.delete();

					File renamefile = new File("src" + File.separator + "Tables" + File.separator + newfile);
					renamefile.canWrite();
					renamefile.renameTo(new File("src" + File.separator + "Tables" + File.separator + current));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

		} else {
			throw new DBAppException("FILE DOESNT EXIST!");

		}
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");

		if (file.exists()) {

			String newfile = "new.csv";
			String key = "";
			long count = 0;
			String line = new String("");
			Enumeration<String> e = htblColNameValue.keys();

			key = e.nextElement();

			for (int i = 1; i <= 10; i++) {
				count = 0;
				try (BufferedReader br = new BufferedReader(
						new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv"));
						FileWriter fw = new FileWriter("src" + File.separator + "Tables" + File.separator + newfile)) {

					Path filecount = Paths
							.get("src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");

					String current = strTableName + i + ".csv";
					File nf = new File("src" + File.separator + "Tables" + File.separator + newfile);

					while ((line = br.readLine()) != null) {

						String[] indexed = line.split(",");
						if (line.equals("")) {
							break;
						}
						boolean f1 = nf.createNewFile();
						if (!htblColNameValue.containsValue(indexed[1])) {
							fw.write(line + "\n");
							count = Files.lines(filecount).count();

						}

					}
					fw.close();
					br.close();
					new File("src" + File.separator + "Tables" + File.separator + current).delete();

					File endfile = new File("src" + File.separator + "Tables" + File.separator + newfile);
					endfile.renameTo(new File("src" + File.separator + "Tables" + File.separator + current));

					if (count == 0 && i != 1) {
						new File("src" + File.separator + "Tables" + File.separator + current).delete();
					}

				} catch (IOException x) {
					// TODO Auto-generated catch block
					x.printStackTrace();
				}

			}

		} else {
			throw new DBAppException("FILE DOESNT EXIST!");

		}
	}

	private static Boolean check(String[] arr, String toCheckValue) {

		boolean result = false;
		for (String element : arr) {
			if (element.equals(toCheckValue)) {
				result = true;
				break;
			}
		}
		return result;
	}

	public static void sorter(int index,String path) throws IOException {
		Hashtable<Object, String> rows = new Hashtable<Object, String>();


		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line;
		String newfile = "new.csv";
		FileWriter fw = new FileWriter("src" + File.separator + "Tables" + File.separator + newfile);


		while ((line = reader.readLine()) != null) {
		   String[] indexed = line.split(",");
		   rows.put(indexed[index], line);
		  
		}
		reader.close();
		
		
		TreeMap<Object, String> tm
		= new TreeMap<Object, String>(rows);

	// create a keyset
	Set<Object> keys = tm.keySet();
	Iterator<Object> itr = keys.iterator();

	// traverse the TreeMap using iterator
	while (itr.hasNext()) {
		Object i = itr.next();
		fw.write(rows.get(i) + "\n");
		
	}
	fw.close();
	new File(path).delete();

					File endfile = new File("src" + File.separator + "Tables" + File.separator + newfile);
					endfile.renameTo(new File(path));
		
	 }
  
  
	 
  


	public static void main(String[] args) throws IOException, DBAppException {

		DBApp dbApp = new DBApp();
		Hashtable<String, String> map = new Hashtable<String, String>();
		map.put("same", new String("mostafa"));
		map.put("gender", new String("m"));
		map.put("age", new String("19"));
		Hashtable<String, Object> map2 = new Hashtable<String, Object>();
		map2.put("name", new String("mostafa"));
		map2.put("gender", new String("m"));
		map2.put("age", new String("30"));

		Enumeration<String> e = map.keys();

		while (e.hasMoreElements()) {

			String key = e.nextElement();

			// System.out.print(map.get(key) +",");

		}
		// dbApp.Tables.put("table", 1);

		// dbApp.createTable("table", "id",map,map, map) ;
		// dbApp.add();
		// System.out.println(DBApp.Tables);
		// dbApp.updateTable("table", "age", map2);
		// dbApp.deleteFromTable("table", map2);
		String strTableName = "Student";
		Hashtable htblColNameType = new Hashtable();
		Hashtable max = new Hashtable();
		Hashtable min = new Hashtable();
		Hashtable htblColNameValue = new Hashtable();
		Hashtable htblColmin = new Hashtable();
		Hashtable htblColmax = new Hashtable();

		htblColmin.put("id", "3");
		htblColmin.put("gpa", "0");
		htblColmin.put("name", "0");
		htblColmax.put("id", "10");
		htblColmax.put("gpa", "10");
		htblColmax.put("name", "10");
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		//dbApp.createTable(strTableName, "id", htblColNameType, htblColmin,htblColmax);

		htblColNameValue.put("id", new Integer(2343432));
		htblColNameValue.put("name", new String("Ahmed Noor"));
		htblColNameValue.put("gpa", new Double(0.95));
		// dbApp.insertIntoTable( strTableName , htblColNameValue );
		// htblColNameValue.clear();
		// htblColNameValue.put("id", new Integer( 453455 ));
		// htblColNameValue.put("name", new String("Ahmed Noor" ) );
		// htblColNameValue.put("gpa", new Double( 0.95 ) ); dbApp.insertIntoTable(
		// strTableName , htblColNameValue );
		// htblColNameValue.clear( );
		htblColNameValue.put("id", new Integer(13));
		htblColNameValue.put("name", new String("Dalia Noor"));
		htblColNameValue.put("gpa", new Double(1.76)); //
		// htblColNameValue.put("id", new Integer(20));
		// htblColNameValue.put("name", new String("mostafa"));
		// htblColNameValue.put("gpa", new Double(1.40)); //
		htblColNameValue.put("id", new Integer(1));
		htblColNameValue.put("name", new String("Zaky Noor"));
		htblColNameValue.put("gpa", new Double(0.78));

		dbApp.insertIntoTable(strTableName, htblColNameValue);
		// System.out.println(htblColNameValue);
		// dbApp.deleteFromTable("Man", htblColNameValue);
		// dbApp.updateTable("Student", "20", htblColNameValue);

		System.out.println(File.separator);
		//sorter(3, "src" + File.separator + "Tables" + File.separator + strTableName +   "1.csv");
	}
}