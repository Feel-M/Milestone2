package M2;

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

	public void createIndex(String strTableName,
			String strColName) throws DBAppException {
		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		File indexfile = new File(
				"src" + File.separator + "Indices" + File.separator + strTableName + strColName + ".csv");
		String line = "";
		String[] columns;
		String[] Sparserow;
		String[] Denserow;
		String SparseColinfo = "";
		String DenseColinfo = "";
		List<String> coulmnlist = new ArrayList<>();
		boolean isClusterkey = false;
		boolean flag = false;
		int clusterkeyindex = 0;
		boolean Nonclusterflag = false;
		int Nonclusterkeyindex = 0;
		ArrayList<String> pages = new ArrayList<>();
		String NonClusterType = "";
		File DenseIndexfile = new File("src" + File.separator + "Indices" + File.separator + strTableName + "."
				+ strColName + "." + "Dense.csv");

		try {
			if (file.exists()) {
				if (indexfile.exists()) {
					throw new DBAppException("Index Already Exists!");
				}

				BufferedReader maxreader = new BufferedReader(
						new FileReader("src" + File.separator + "MetaData.csv"));
				line = maxreader.readLine();
				while (((line = maxreader.readLine()) != null)) {

					if ((line).equals("")) {
						break;
					}
					String[] indexed = line.split(",");
					// Gets all columns in requested table
					if (indexed[0].equals(strTableName)) {
						coulmnlist.add(indexed[1]);
					}

					// gets clusterkey index
					if (indexed[0].equals(strTableName) && flag == false && !indexed[5].equals("SparseIndex")
							&& !indexed[5].equals("DenseIndex")) {

						clusterkeyindex++;
						if (indexed[3].equals("True")) {
							flag = true;
							clusterkeyindex -= 1;
						}
					}
					// gets nonclusterkey index
					if (indexed[0].equals(strTableName) && Nonclusterflag == false
							&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {

						Nonclusterkeyindex++;
						if (indexed[1].equals(strColName)) {
							Nonclusterflag = true;
							NonClusterType = indexed[2];
							Nonclusterkeyindex -= 1;
						}
					}
					// creates metadata info
					if (indexed[0].equals(strTableName) && indexed[1].equals(strColName)
							&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {
						Sparserow = indexed;
						Denserow = indexed;
						Denserow[4] = strColName + "Index";
						Denserow[5] = "DenseIndex";
						DenseColinfo = String.join(",", Denserow);

						Sparserow[4] = strColName + "Index";
						Sparserow[5] = "SparseIndex";

						SparseColinfo = String.join(",", Sparserow);

					}

					// checks if column is clusterkey or not
					if (indexed[0].equals(strTableName) && indexed[1].equals(strColName)
							&& indexed[3].equals("True")) {
						isClusterkey = true;
					}

				}
				// checks if coulmn exists in requested table
				columns = coulmnlist.toArray(new String[0]);
				if (!check(columns, strColName)) {
					throw new DBAppException("Column not found in specified Table!");
				}

				// TODO: create index and add values
				if (isClusterkey) {
					Boolean c = indexfile.createNewFile();
					FileWriter fileWriter = new FileWriter(indexfile, true);
					for (int i = 1; i < 10; i++) {
						File check = new File(
								"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");
						if (!check.exists()) {
							continue;
						}
						maxreader = new BufferedReader(
								new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i
										+ ".csv"));
						line = maxreader.readLine();
						if (line == null || line.equals("")) {
							break;
						}
						String[] indexed = line.split(",");
						fileWriter.write(indexed[clusterkeyindex] + "," + strTableName + i + ".csv" + "\n");

					}
					fileWriter.close();

				} else {
					// TODO: create dense file
					DenseIndexfile.createNewFile();
					FileWriter fileWriter = new FileWriter(DenseIndexfile, true);
					for (int i = 1; i < 10; i++) {
						File check = new File(
								"src" + File.separator + "Tables" + File.separator + strTableName + i + ".csv");

						if (!check.exists()) {
							continue;
						}
						maxreader = new BufferedReader(
								new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + i
										+ ".csv"));
						while ((line = maxreader.readLine()) != null) {
							if (line == null || line.equals("")) {
								break;
							}
							String[] indexed = line.split(",");
							fileWriter.write(indexed[Nonclusterkeyindex] + "," + strTableName + i + ".csv" + "\n");

						}
					}
					String DensePath = "src" + File.separator + "Indices" + File.separator + strTableName + "."
							+ strColName + "." + "Dense.csv";
					fileWriter.close();
					sorter(Nonclusterkeyindex, DensePath, NonClusterType);

					// TODO: Create sparse file
					Boolean c = indexfile.createNewFile();
					fileWriter = new FileWriter(indexfile, true);
					maxreader = new BufferedReader(
							new FileReader("src" + File.separator + "Indices" + File.separator + strTableName + "."
									+ strColName + "." + "Dense.csv"));
					while ((line = maxreader.readLine()) != null) {
						if (line == null || line.equals("")) {
							break;
						}
						String[] indexed = line.split(",");
						if (!pages.contains(indexed[1])) {
							fileWriter.write(indexed[0] + "," + indexed[1] + "\n");
							pages.add(indexed[1]);
						}

					}
					fileWriter.close();

				}

				// TODO: Update Metadata File
				FileWriter fileWriter = new FileWriter("src" + File.separator + "MetaData.csv", true);
				fileWriter.write(DenseColinfo + "\n");
				fileWriter.write(SparseColinfo + "\n");
				fileWriter.close();

				// TODO: Update Data File
				fileWriter = new FileWriter("src" + File.separator + "Files.csv", true);
				fileWriter.write(strTableName + "_" + strColName + "," + "DenseIndex" + "," + strTableName + "."
						+ strColName + "." + "Dense.csv" + "," + DenseIndexfile.getAbsolutePath() + "\n");

				fileWriter.write(strTableName + "_" + strColName + "," + "SparseIndex" + "," + strTableName + strColName
						+ ".csv" + "," + indexfile.getAbsolutePath() + "\n");
				fileWriter.close();

			} else {
				throw new DBAppException("TABLE DOESN'T EXIST!");
			}

		} catch (Exception e) {
			// if (!(e instanceof FileNotFoundException)) {
			e.printStackTrace();
			return;
			// }
		}

	}

	public void insertIntoTable(String strTableName,
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {

		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		long count = 0;
		String clusterKey = "";
		String line = "";
		int clusterkeyindex = 0;
		boolean flag = false;
		String path = "";
		String clusterType = "";

		try {
			if (file.exists()) {

				try {
					// start
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
							// gets clusterkeyindex
							if (indexed[0].equals(strTableName) && flag == false && !indexed[5].equals("SparseIndex")
									&& !indexed[5].equals("DenseIndex")) {
								clusterkeyindex++;
								if (indexed[3].equals("True")) {
									flag = true;
									clusterType = indexed[2];
									clusterkeyindex -= 1;
								}
							}

							if ((htblColNameValue.get(key).getClass().getName()).equals("java.lang.String")
									&& indexed[1].equals(key) && indexed[0].equals(strTableName)
									&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {
								Double minString = (Double.parseDouble(String.valueOf(indexed[6].length())));
								Double maxString = (Double.parseDouble(String.valueOf(indexed[7].length())));

								if (((String) htblColNameValue.get(key)).length() < minString
										|| ((String) htblColNameValue.get(key)).length() > maxString) {
									System.out.println(((String) htblColNameValue.get(key)).length());

									throw new DBAppException(key + " is not within Range!");

								}

							}

							else if (indexed[1].equals(key) && indexed[0].equals(strTableName)
									&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {
								Double minNum = (Double.parseDouble(indexed[6]));
								Double maxNum = (Double.parseDouble(indexed[7]));
								Object obj = htblColNameValue.get(key);
								String str = obj.toString();
								double value = Double.valueOf(str).doubleValue();
								if (value < minNum || value > maxNum) {

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
					// end
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
							br.close();
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
						br.close();
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
							bw.close();
							sorter(clusterkeyindex, path, clusterType);
							UpdateIndex(strTableName);
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
			Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		File file = new File("src" + File.separator + "Tables" + File.separator + strTableName + "1.csv");
		Enumeration<String> hash = htblColNameValue.keys();
		boolean flag = false;
		String line = "";
		int clusterkeyindex = 0;
		boolean Clusterflag = false;
		String clusterType = "";

		if (file.exists()) {
			// here\
			try {
				while (hash.hasMoreElements()) {
					String hashkey = hash.nextElement();

					BufferedReader maxreader = new BufferedReader(
							new FileReader("src" + File.separator + "MetaData.csv"));
					line = maxreader.readLine();
					while (((line = maxreader.readLine()) != null)) {

						if ((line).equals("")) {
							break;
						}
						String[] indexed = line.split(",");
						if (indexed[0].equals(strTableName) && Clusterflag == false && !indexed[5].equals("SparseIndex")
								&& !indexed[5].equals("DenseIndex")) {

							clusterkeyindex++;
							if (indexed[3].equals("True")) {
								Clusterflag = true;
								clusterType = indexed[2];
								clusterkeyindex -= 1;
							}
						}
						if ((htblColNameValue.get(hashkey).getClass().getName()).equals("java.lang.String")
								&& indexed[1].equals(hashkey) && indexed[0].equals(strTableName)
								&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {
							Double minString = (Double.parseDouble(String.valueOf(indexed[6].length())));
							Double maxString = (Double.parseDouble(String.valueOf(indexed[7].length())));

							if (((String) htblColNameValue.get(hashkey)).length() < minString
									|| ((String) htblColNameValue.get(hashkey)).length() > maxString) {
								System.out.println(((String) htblColNameValue.get(hashkey)).length());
								throw new DBAppException(hashkey + " is not within Range!");

							}

						} else if (indexed[1].equals(hashkey) && indexed[0].equals(strTableName)
								&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {
							Double minNum = (Double.parseDouble(indexed[6]));
							Double maxNum = (Double.parseDouble(indexed[7]));
							Object obj = htblColNameValue.get(hashkey);
							String str = obj.toString();
							double value = Double.valueOf(str).doubleValue();
							if (value < minNum || value > maxNum) {
								throw new DBAppException(hashkey + " is not within Range!");

							}

							if ((htblColNameValue.get(hashkey).getClass().getName()).equals(indexed[2])
									&& indexed[0].equals(strTableName)) {
								continue;

							} else {
								maxreader.close();
								throw new DBAppException("invalid Data Types");

							}

						}
					}
					maxreader.close();
				}
			} catch (Exception e) {

				e.printStackTrace();
				return;

			}

			String newfile = "new.csv";

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
						String[] indexed = line.split(",");

						if (check(indexed, strClusteringKeyValue)) {
							Enumeration<String> e = htblColNameValue.keys();

							while (e.hasMoreElements()) {
								String key = e.nextElement();
								fw.write(htblColNameValue.get(key) + ",");

							}
							fw.write("\n");
							flag = true;

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
					String path = "src" + File.separator + "Tables" + File.separator + current;
					sorter(clusterkeyindex, path, clusterType);
					UpdateIndex(strTableName);

				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();

				}

				if (flag) {

					return;
				}

			}

		} else

		{
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
					UpdateIndex(strTableName);

				} catch (IOException x) {
					if (e instanceof FileNotFoundException) {

					} else {
						x.printStackTrace();
					}
				}

			}

		} else {
			throw new DBAppException("FILE DOESNT EXIST!");

		}
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators)
			throws DBAppException, FileNotFoundException, IOException {
		if (arrSQLTerms.length == 0) {
			return null;
		}
		Iterator result = null;
		ArrayList data = new ArrayList<String>();

		File file = new File(
				"src" + File.separator + "Tables" + File.separator + arrSQLTerms[0]._strTableName + "1.csv");
		String line = "";
		String[] indexed = null;
		Double[] SparseValues = null;
		Double closestValue = 0.0;
		ArrayList<Double> sparseValueList = new ArrayList<Double>();

		if (file.exists()) {
			for (int i = 0; i < arrSQLTerms.length; i++) {
				file = new File("src" + File.separator + "Indices" + File.separator + arrSQLTerms[i]._strTableName
						+ arrSQLTerms[i]._strColumnName + ".csv");

				if (file.exists()) {
					BufferedReader br = new BufferedReader(
							new FileReader(file));
					while ((line = br.readLine()) != null) {
						if (line == null || line.equals("")) {
							break;
						}
						indexed = line.split(",");
						sparseValueList.add(Double.parseDouble(indexed[0]));

					}
					br.close();
					SparseValues = sparseValueList.toArray(new Double[0]);
					closestValue = (double) getClosestValue(SparseValues,
							Double.parseDouble(arrSQLTerms[i]._objValue.toString()));

					br = new BufferedReader(
							new FileReader(file));
					while ((line = br.readLine()) != null) {
						if (line == null || line.equals("")) {
							break;
						}
						indexed = line.split(",");
						if (Double.parseDouble(indexed[0]) == closestValue) {
							file = new File("src" + File.separator + "Tables" + File.separator + indexed[1]);
							br = new BufferedReader(
									new FileReader(file));
							int columnIndex = GetColIndex(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName);
							while ((line = br.readLine()) != null) {
								if (line == null || line.equals("")) {
									break;
								}
								indexed = line.split(",");
								if (evaluate(indexed[columnIndex], arrSQLTerms[i]._strOperator,
										(arrSQLTerms[i]._objValue).toString(), arrSQLTerms[i]._strTableName,
										arrSQLTerms[i]._strColumnName)) {

									data.add(line);

								}

							}
							// ! WE NOW HAVE THE DATA NEXT STEP IS TO COMPARE THE DATA (AND,OR,XOR) MAYBE
							// LOOP OVER DATA ARRAYLIST?

						}
					}

				} else {

					for (int j = 0; j < 10; j++) {
						file = new File("src" + File.separator + "Tables" + File.separator
								+ arrSQLTerms[0]._strTableName + j + ".csv");
						if (!file.exists()) {
							continue;
						}
						BufferedReader br = new BufferedReader(
								new FileReader(file));
						while ((line = br.readLine()) != null) {
							if (line == null || line.equals("")) {
								break;
							}
							indexed = line.split(",");
							int columnIndex = GetColIndex(arrSQLTerms[i]._strTableName, arrSQLTerms[i]._strColumnName);
							if (evaluate(indexed[columnIndex], arrSQLTerms[i]._strOperator,
									(arrSQLTerms[i]._objValue).toString(), arrSQLTerms[i]._strTableName,
									arrSQLTerms[i]._strColumnName)) {
								data.add(line);
							}

						}
					}
				}

			}
			// delete from here

			if (strarrOperators.length == 0) {
				result = data.iterator();
			} else {
				for (int i = 0; i < arrSQLTerms.length; i++) {
					for (int j = 0; j < arrSQLTerms.length - 1; j++) {
						for (int k = 0; k < data.size(); k++) {
							line = data.get(k).toString();
							indexed = line.split(",");
							int colindex = GetColIndex(arrSQLTerms[j]._strTableName, arrSQLTerms[j]._strColumnName);
							int colindex2 = GetColIndex(arrSQLTerms[j + 1]._strTableName,
									arrSQLTerms[j + 1]._strColumnName);
							if (strarrOperators[j].equals("AND")) {
								if (evaluate(indexed[colindex], arrSQLTerms[j]._strOperator,
										arrSQLTerms[j]._objValue.toString(), arrSQLTerms[j]._strTableName,
										arrSQLTerms[j]._strColumnName)
										&& evaluate(indexed[colindex2], arrSQLTerms[j + 1]._strOperator,
												arrSQLTerms[j + 1]._objValue.toString(),
												arrSQLTerms[j + 1]._strTableName, arrSQLTerms[j + 1]._strColumnName)) {
									continue;
								} else {
									data.remove(k);
									k--;
								}
							} else if (strarrOperators[j].equals("OR")) {
								if (evaluate(indexed[colindex], arrSQLTerms[j]._strOperator,
										arrSQLTerms[j]._objValue.toString(), arrSQLTerms[j]._strTableName,
										arrSQLTerms[j]._strColumnName)
										|| evaluate(indexed[colindex2], arrSQLTerms[j + 1]._strOperator,
												arrSQLTerms[j + 1]._objValue.toString(),
												arrSQLTerms[j + 1]._strTableName, arrSQLTerms[j + 1]._strColumnName)) {
									continue;
								} else {
									data.remove(k);
									k--;
								}
							} else {
								if (evaluate(indexed[colindex], arrSQLTerms[j]._strOperator,
										arrSQLTerms[j]._objValue.toString(), arrSQLTerms[j]._strTableName,
										arrSQLTerms[j]._strColumnName) != evaluate(indexed[colindex2],
												arrSQLTerms[+1]._strOperator, arrSQLTerms[j + 1]._objValue.toString(),
												arrSQLTerms[j + 1]._strTableName, arrSQLTerms[j + 1]._strColumnName)) {
									continue;
								} else {
									data.remove(k);
									k--;
								}
							}
						}

					}
				}
			}

			// till here

			result = data.iterator();
			return result;
		} else {
			throw new DBAppException("Table Doesn't Exist!");
		}

		// TODO: DELETE THIS LINE LATER

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

	public static void sorter(int index, String path, String type) throws IOException {

		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line;
		String newfile = "new.csv";
		Hashtable rows = new Hashtable<>();

		FileWriter fw = new FileWriter("src" + File.separator + "Tables" + File.separator + newfile);
		if (type.equals("java.lang.Integer")) {
			rows = new Hashtable<Integer, String>();

		} else if (type.equals("java.lang.Double")) {
			rows = new Hashtable<Double, String>();

		} else {
			rows = new Hashtable<Object, String>();

		}

		while ((line = reader.readLine()) != null) {
			String[] indexed = line.split(",");
			if (type.equals("java.lang.Integer")) {
				int key = Integer.valueOf(indexed[index].toString());
				rows.put(key, line);

			} else if (type.equals("java.lang.Double")) {
				double key = Double.valueOf(indexed[index].toString());
				rows.put(key, line);

			} else {
				rows.put(indexed[index], line);

			}

		}
		reader.close();

		TreeMap<Object, String> tmap = new TreeMap<Object, String>(rows);

		Set<Object> keys = tmap.keySet();
		Iterator<Object> itr = keys.iterator();

		while (itr.hasNext()) {
			Object i = itr.next();
			fw.write(rows.get(i) + "\n");

		}
		fw.close();
		new File(path).delete();

		File endfile = new File("src" + File.separator + "Tables" + File.separator + newfile);
		endfile.renameTo(new File(path));

	}

	public static void UpdateIndex(String strTableName) throws IOException {
		ArrayList denseColumns = new ArrayList<>();
		ArrayList SparseColumns = new ArrayList<>();
		String line = "";
		ArrayList<String> pages = new ArrayList<>();
		String Clustertype = "";
		String NonClustertype = "";

		try {
			BufferedReader maxreader = new BufferedReader(
					new FileReader("src" + File.separator + "MetaData.csv"));
			line = maxreader.readLine();
			int Nonclusterkeyindex = 0;
			boolean Nonclusterflag = false;
			int clusterkeyindex = 0;
			boolean flag = false;

			while (((line = maxreader.readLine()) != null)) {

				if ((line).equals("")) {
					break;
				}
				String[] indexed = line.split(",");
				if (indexed[5].equals("DenseIndex") && indexed[0].equals(strTableName)) {
					denseColumns.add(indexed[1]);
				} else if (indexed[5].equals("SparseIndex") && indexed[0].equals(strTableName)) {
					SparseColumns.add(indexed[1]);
				} // gets clusterkey index
				if (indexed[0].equals(strTableName) && flag == false && !indexed[5].equals("SparseIndex")
						&& !indexed[5].equals("DenseIndex")) {

					clusterkeyindex++;
					if (indexed[3].equals("True")) {
						flag = true;
						clusterkeyindex -= 1;
					}
				}

				for (int i = 0; i < denseColumns.size(); i++) {
					File DenseIndexfile = new File(
							"src" + File.separator + "Indices" + File.separator + strTableName + "."
									+ denseColumns.get(i) + "." + "Dense.csv");
					if (!DenseIndexfile.exists()) {
						continue;
					}
					FileWriter fileWriter = new FileWriter(DenseIndexfile);
					// gets nonclusterkey index
					if (indexed[0].equals(strTableName) && Nonclusterflag == false
							&& !indexed[5].equals("SparseIndex") && !indexed[5].equals("DenseIndex")) {

						Nonclusterkeyindex++;
						if (indexed[1].equals(denseColumns.get(i))) {
							Nonclusterflag = true;
							NonClustertype = indexed[2];
							Nonclusterkeyindex -= 1;
						}
					}
				}
			}

			for (int i = 0; i < denseColumns.size(); i++) {
				if (!SparseColumns.contains(denseColumns.get(i))) {
					continue;
				}
				File DenseIndexfile = new File("src" + File.separator + "Indices" + File.separator + strTableName + "."
						+ denseColumns.get(i) + "." + "Dense.csv");
				FileWriter fileWriter = new FileWriter(DenseIndexfile);

				for (int j = 1; j < 10; j++) {
					File check = new File(
							"src" + File.separator + "Tables" + File.separator + strTableName + j + ".csv");

					if (!check.exists()) {
						continue;
					}
					maxreader = new BufferedReader(
							new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + j
									+ ".csv"));
					while ((line = maxreader.readLine()) != null) {
						if (line == null || line.equals("")) {
							break;
						}
						String[] indexed = line.split(",");
						fileWriter.write(indexed[Nonclusterkeyindex] + "," + strTableName + j + ".csv" + "\n");

					}
				}
				String DensePath = "src" + File.separator + "Indices" + File.separator + strTableName + "."
						+ denseColumns.get(i) + "." + "Dense.csv";
				fileWriter.close();
				sorter(Nonclusterkeyindex, DensePath, NonClustertype);
				File indexfile = new File(
						"src" + File.separator + "Indices" + File.separator + strTableName + denseColumns.get(i)
								+ ".csv");
				fileWriter = new FileWriter(indexfile);
				maxreader = new BufferedReader(
						new FileReader("src" + File.separator + "Indices" + File.separator + strTableName + "."
								+ denseColumns.get(i) + "." + "Dense.csv"));
				while ((line = maxreader.readLine()) != null) {
					if (line == null || line.equals("")) {
						break;
					}
					String[] indexed = line.split(",");
					if (!pages.contains(indexed[1])) {
						fileWriter.write(indexed[0] + "," + indexed[1] + "\n");
						pages.add(indexed[1]);
					}

				}
				fileWriter.close();

			}
			for (int i = 0; i < SparseColumns.size(); i++) {
				File indexfile = new File("src" + File.separator + "Indices" + File.separator + strTableName
						+ SparseColumns.get(i) + ".csv");
				if (!indexfile.exists()) {
					continue;
				}
				if (denseColumns.contains(SparseColumns.get(i))) {
					continue;
				}
				FileWriter fileWriter = new FileWriter(indexfile);
				for (int j = 1; j < 10; j++) {
					File check = new File(
							"src" + File.separator + "Tables" + File.separator + strTableName + j + ".csv");
					if (!check.exists()) {
						continue;
					}
					maxreader = new BufferedReader(
							new FileReader("src" + File.separator + "Tables" + File.separator + strTableName + j
									+ ".csv"));
					line = maxreader.readLine();
					if (line == null || line.equals("")) {
						break;
					}
					String[] indexed = line.split(",");
					fileWriter.write(indexed[clusterkeyindex] + "," + strTableName + j + ".csv" + "\n");

				}
				fileWriter.close();
			}

		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public static int GetColIndex(String tableName, String column) throws FileNotFoundException, IOException {
		Boolean flag = false;
		int index = 0;

		try (BufferedReader maxreader = new BufferedReader(
				new FileReader("src" + File.separator + "MetaData.csv"))) {
			String line = maxreader.readLine();
			while (((line = maxreader.readLine()) != null)) {

				if ((line).equals("")) {
					break;
				}
				String[] indexed = line.split(",");

				// gets clusterkey index
				if (indexed[0].equals(tableName) && flag == false && !indexed[5].equals("SparseIndex")
						&& !indexed[5].equals("DenseIndex")) {

					index++;
					if (indexed[1].equals(column)) {
						flag = true;
						index -= 1;

					}

				}
			}
		}
		return index;
	}

	public static String getColType(String tableName, String colname) throws IOException {
		String result = "";

		try (BufferedReader maxreader = new BufferedReader(
				new FileReader("src" + File.separator + "MetaData.csv"))) {
			String line = maxreader.readLine();
			while (((line = maxreader.readLine()) != null)) {

				if ((line).equals("")) {
					break;
				}
				String[] indexed = line.split(",");

				if (indexed[0].equals(tableName) && indexed[1].equals(colname) && !indexed[5].equals("SparseIndex")
						&& !indexed[5].equals("DenseIndex")) {

					result = indexed[2];

				}
			}
		}

		return result;
	}

	public static Double getClosestValue(Double[] sparseValues, Double _objValue) {
		int left = 0;
		int right = sparseValues.length - 1;
		double floor = -1;

		while (left <= right) {
			int mid = left + (right - left) / 2;

			if (sparseValues[mid] == _objValue) {
				return sparseValues[mid];
			} else if (sparseValues[mid] < _objValue) {
				floor = sparseValues[mid];
				left = mid + 1;
			} else {
				right = mid - 1;
			}
		}

		return floor;
	}

	public static boolean evaluate(String dataValue, String operator, String queryValue, String tableName,
			String column) throws IOException {

		String type = getColType(tableName, column);
		if (type.equals("java.lang.Double")) {

		}
		switch (operator) {
			case ">":
				return Double.parseDouble(dataValue) > Double.parseDouble(queryValue);
			case "<":
				return Double.parseDouble(dataValue) < Double.parseDouble(queryValue);
			case ">=":
				return Double.parseDouble(dataValue) >= Double.parseDouble(queryValue);
			case "<=":
				return Double.parseDouble(dataValue) <= Double.parseDouble(queryValue);
			case "=":
				return queryValue.equals(dataValue);
			case "!=":
				return !queryValue.equals(dataValue);
			default:
				return false;
		}
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
		htblColmin.put("name", "A");
		htblColmax.put("id", "10");
		htblColmax.put("gpa", "10");
		htblColmax.put("name", "ZZZZZZZZZZZ");
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("gpa", "java.lang.Double");
		// dbApp.createTable(strTableName, "id", htblColNameType,htblColmin,
		// htblColmax);

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
		htblColNameValue.put("id", new Integer(10));
		htblColNameValue.put("name", new String("zed"));
		htblColNameValue.put("gpa", new Double(9));

		// dbApp.insertIntoTable(strTableName, htblColNameValue);
		// System.out.println(htblColNameValue);
		// dbApp.deleteFromTable(strTableName, htblColNameValue);
		// dbApp.updateTable(strTableName, "9", htblColNameValue);

		System.out.println(File.separator);

		// dbApp.createIndex(strTableName, new String("gpa"));
		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[3];
		System.out.println(arrSQLTerms.length);
		arrSQLTerms[0] = new SQLTerm("Student", "gpa", "=", "6.0");
		arrSQLTerms[1] = new SQLTerm("Student", "id", "=", "4");
		arrSQLTerms[2] = new SQLTerm("Student", "id", "=", "4");

		String[] strarrOperators = new String[2];
		strarrOperators[0] = "OR";
		//strarrOperators[1] = "AND";

		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);

		while (resultSet.hasNext()) {
			System.out.print(resultSet.next() + "\n");
		}
	}
}