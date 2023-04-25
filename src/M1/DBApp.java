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
		// data file
		File Files = new File("src" + File.separator + "Files.csv");
		boolean datafile;
		datafile = Files.createNewFile();

		// metadata file
		File meta = new File("src" + File.separator + "MetaData.csv");
		boolean metafile;
		metafile = meta.createNewFile();

		if (metafile == false) {
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
				fileWriter.write(htblColNameType.get(keytype) + ",");
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
				fileWriter.write(htblColNameType.get(keytype) + ",");
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
		// files file
		if (datafile == false) {
			FileWriter fileWriter = new FileWriter("src" + File.separator + "Files.csv");
			fileWriter.write(strTableName + ",");
			fileWriter.write("Table" + ",");
			fileWriter.write(strTableName + "1.csv,");
			fileWriter.write(Files.getAbsolutePath() + "\n");
			fileWriter.close();

		} else {

			FileWriter fileWriter = new FileWriter("src" + File.separator + "Files.csv");
			fileWriter.write("Name" + ",");
			fileWriter.write("Type" + ",");
			fileWriter.write("FileName" + ",");
			fileWriter.write("FileLocationonHarddisk" + "\n");
			fileWriter.write(strTableName + ",");
			fileWriter.write("Table" + ",");
			fileWriter.write(strTableName + "1.csv,");
			fileWriter.write(Files.getAbsolutePath() + "\n");
			fileWriter.close();
		}

		File f = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		boolean f1;
		f1 = f.createNewFile();

		if (f1 == false) {
			throw new DBAppException("TABLE ALREADY EXISTS!");
		} else {

			FileWriter fileWriter = new FileWriter(
					"src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");

			Enumeration<String> e = htblColNameType.keys();

			while (e.hasMoreElements()) {

				String key = e.nextElement();

				fileWriter.write(key + ",");
			}
			fileWriter.write("\n");
			fileWriter.close();

		}

		// MetaData file

	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {

		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		long count = 0;
		int fileindex = 0;
		int max = 0;
		String line = "";

		try {
			if (file.exists()) {

				try {
					Enumeration<String> hash = htblColNameValue.keys();

					while (hash.hasMoreElements()) {
						String key = hash.nextElement();

						BufferedReader maxreader = new BufferedReader(
								new FileReader("src" + File.separator + "MetaData.csv"));
						while ((line = maxreader.readLine()) != null) {

							String[] indexed = line.split(",");

							if (indexed[1].equals(key)) {
								Object str = indexed[2];

								if ((htblColNameValue.get(key).getClass().getName()).equals(indexed[2])) {
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

				BufferedReader maxreader = new BufferedReader(new FileReader("src" + File.separator + "MetaData.csv"));
				while ((line = maxreader.readLine()) != null) {

					String[] indexed = line.split(",");
					int loc = indexed.length - 1;
					if (indexed.equals(null)) {
						break;
					}

					if (indexed[0].equals(strTableName)) {

						max = Integer.parseInt(indexed[loc]);

					}

				}

				for (int i = 1; i <= 10; i++) {
					File check = new File(
							"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
					if (check.exists()) {
						Path filecount = Paths
								.get("src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
						BufferedReader br = new BufferedReader(
								new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i
										+ ".csv"));
						count = Files.lines(filecount).count();
						if (count < max) {
							FileWriter fileWriter = new FileWriter(
									"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv",
									true);
							BufferedWriter bw = new BufferedWriter(fileWriter);

							Enumeration<String> e = htblColNameValue.keys();

							while (e.hasMoreElements()) {
								String key = e.nextElement();
								bw.write(htblColNameValue.get(key) + ",");
							}
							bw.write("\n");
							bw.flush();
							bw.close();
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
						FileWriter datafile = new FileWriter("src" + File.separator + "Files.csv");
						datafile.write(strTableName + ",");
						datafile.write("Table" + ",");
						datafile.write(strTableName + i + ".csv,");
						datafile.write(Files.getAbsolutePath() + "\n");
						datafile.close();
						break;

					}
				}
			} else {
				throw new DBAppException("Table Doesnt exist!");
			}
		} catch (Exception a) {
			if (file.exists()) {
				System.out.print("Max not found!");
				System.out.print("Reverting to Default '3' ");
				try {
					Enumeration<String> hash = htblColNameValue.keys();

					while (hash.hasMoreElements()) {
						String key = hash.nextElement();

						BufferedReader maxreader = new BufferedReader(
								new FileReader("src" + File.separator + "MetaData.csv"));
						while ((line = maxreader.readLine()) != null) {

							String[] indexed = line.split(",");

							if (indexed[1].equals(key)) {
								Object str = indexed[2];

								if ((htblColNameValue.get(key).getClass().getName()).equals(indexed[2])) {
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
					File check = new File(
							"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
					if (check.exists()) {
						Path filecount = Paths
								.get("src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
						BufferedReader br = new BufferedReader(
								new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i
										+ ".csv"));

						count = Files.lines(filecount).count();
						if (count < 3) {
							FileWriter fileWriter = new FileWriter(
									"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv",
									true);
							BufferedWriter bw = new BufferedWriter(fileWriter);

							Enumeration<String> e = htblColNameValue.keys();

							while (e.hasMoreElements()) {
								String key = e.nextElement();
								bw.write(htblColNameValue.get(key) + ",");
							}
							bw.write("\n");
							bw.flush();
							bw.close();
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
						FileWriter datafile = new FileWriter("src" + File.separator + "Files.csv");
						datafile.write(strTableName + ",");
						datafile.write("Table" + ",");
						datafile.write(strTableName + i + ".csv,");
						datafile.write(Files.getAbsolutePath() + "\n");
						datafile.close();
						break;

					}
				}
			} else {
				throw new DBAppException("Table Doesnt exist!");
			}
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
					File deletFile = 	new File("src" + File.separator + "Tables" + File.separator + current);
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

		htblColmin.put("id", "0");
		htblColmin.put("gpa", "0");
		htblColmin.put("name", "0");
		htblColmax.put("id", "10");
		htblColmax.put("gpa", "10");
		htblColmax.put("name", "10");
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		 //dbApp.createTable(strTableName, "id", htblColNameType, htblColmin, htblColmax);

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
		 //htblColNameValue.put("name", new String("mostafa"));
		 //htblColNameValue.put("gpa", new Double(1.40)); //

		 //dbApp.insertIntoTable( strTableName , htblColNameValue );
		// System.out.println(htblColNameValue);
		dbApp.deleteFromTable("Student", htblColNameValue);
	//	 dbApp.updateTable("Student", "20", htblColNameValue);

		System.out.println();
	}
}