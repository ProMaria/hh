//sudo apt-get install libpostgresql-jdbc-java libpostgresql-jdbc-java-doc

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import javax.net.ssl.HttpsURLConnection;

import java.io.FileWriter;
import java.io.IOException;
 
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.json.simple.JSONObject;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.FileWriter;
import au.com.bytecode.opencsv.CSVWriter;
import java.nio.charset.Charset;
import java.io.File;




class hhapi {
    private final String USER_AGENT = "Mozilla/5.0";
    private final String DATABASE_NAME = "testdb";
    private final String DATABASE_USERNAME = "test";
    private final String DATABASE_PASSWD = "test";
    

    public static void main(String[] args) throws Exception {        
		
		int resultSalary; 
		 
		hhapi http = new hhapi();
		//System.out.println(args.length);
        if (args.length<1)
        {			
			System.out.print("Please, enter arguments \n");
		}
		else if (args.length==1)
		{
			if (args[0].equals("showData")){
				resultSalary = http.averageSalary("1859");
				int resultVacancies = http.countVacancies("1859");
				System.out.println("Средняя заработная плата по региону составляет "+resultSalary+" рублей.\nКоличество вакансий по региону: "+resultVacancies+".");
			}
			else if (args[0].equals("saveData")){
				//Поиск вакансий выше средней зп по региону
				resultSalary = http.averageSalary("1859");
				//Запись этих вакансий в csv файл
				http.csvSave (resultSalary, 43);
				System.out.println("Файл сформирован");
			}
			
			else {
				System.out.print("Please, enter other arguments \n");	
			}
		}
		else if (args.length==2)
		{
			//Обрабатываем запрос с 2 параметрами (getData, Название страны)
			if (args[0].equals("getData")){
				String countryName = args[1];
				//System.out.println("countryName="+countryName);
				String code="";
				//Поиск кода страны из справочника
				code = http.countryCode(countryName);
				if (!code.equals(""))				
				{
					String selectQuery = "SELECT count(*) FROM public.areas where id="+code;
					String insertQuery = "INSERT INTO public.areas VALUES('"+code+"','"+countryName+"')";
					http.connectPostgres(selectQuery,insertQuery, "country");
					//System.out.println("code equal"+code);
					http.getData(code,"100", "0");
				}
			}
		}
		else if (args.length==3)
		{
			//Обрабатываем запрос с 3 параметрами (showRange, min, max)
			if (args[0].equals("showRange")){				
				http.csvSaveFromDb (Integer.valueOf(args[1]), Integer.valueOf(args[2]));
			}
		}

		else {
			System.out.print("Please, enter other arguments \n");			
		
		};
   

        
    }
    //Получение 100 вакансий для страны и запись в БД
    private void getData(String code, String per_page, String page) throws Exception {
        //System.out.println(code);

		String url = "https://api.hh.ru/vacancies?describe_arguments=true&&area="+code+"&&per_page="+per_page+"&&page="+page;
		StringBuffer response;
		hhapi http = new hhapi();  
		//Вызов функции подключения к api hh
		response = http.connectToApi(url);
		
		JSONParser parser = new JSONParser();
		if(response.toString().length()>0)
		{	Object objk  = parser.parse(response.toString());
			
			JSONObject jsonObject = (JSONObject) objk;
			JSONArray jsonArray = (JSONArray) jsonObject.get("items");
			if (jsonArray != null) { 
			   int len = jsonArray.size();
			   for (int i=0;i<len;i++){ 
				
				JSONObject jo = (JSONObject) jsonArray.get(i);
				JSONObject jsonSnipped = (JSONObject) jo.get("snippet");
				JSONObject jsonSalary = (JSONObject) jo.get("salary");
				
				//System.out.println(jsonSalary);
				//Готовим данные для записи в БД
				String resp="";
				int from=0;
				int to=0;
				String cur="";
				String name=""+jo.get("name");
				String altUrl=""+jo.get("alternate_url");
				
				
				if(jsonSnipped !=null){
					resp=""+jsonSnipped.get("responsibility");				
				}
				
				if(jsonSalary !=null){
					
					if(jsonSalary.get("from")!=null)
					{
						from = Integer.valueOf(""+jsonSalary.get("from"));
					}

					if(jsonSalary.get("to")!=null){
						to = Integer.valueOf(""+jsonSalary.get("to"));
					}

					if(jsonSalary.get("currency")!=null){
						cur= ""+ jsonSalary.get("currency");
					}

				}
				
				//Создаем запросы к БД
				String selectQuery = "SELECT count(*) FROM public.areas where id="+code;
				String insertQuery = "INSERT INTO vacancies (name,area_id, responsibility, salary_from, salary_to, currency, url) VALUES('"+name+"','"+code+"','"+resp+"','"+from+"','"+to+"','"+cur+"','"+altUrl+"')";
				//Вызов функции записи данных в БД
				http.connectPostgres(selectQuery,insertQuery, "vacancy");
				
			   }		    
			}
		}


	}
	
	//Функция поиска средней зарплаты по региону с кодом code	
	private int averageSalary(String code) throws Exception { 	
		StringBuffer response;
		int vacanciesCounter = 0;		
		int salaryCounter = 0;
		hhapi http = new hhapi();
		JSONParser parser = new JSONParser();

	    String urlAll = "https://api.hh.ru/vacancies?clusters=true&only_with_salary=true&area="+code;		    
		
		response = http.connectToApi(urlAll);
		if(response.toString().length()>0)
			{
				Object objk  = parser.parse(response.toString());
				JSONObject jsonObject = (JSONObject) objk;
				//Находим раздел кластеров, где приведен хэш с кол-вом вакансий для диапазона зарплатами
				JSONArray jsonArray = (JSONArray) jsonObject.get("clusters");
				if (jsonArray != null) 
				{ 
				   int len = jsonArray.size();
				   for (int i=0;i<len;i++)
				   {
					JSONObject jo = (JSONObject) jsonArray.get(i);
					
					//System.out.println("JO="+jo.get("id"));
					if(jo.get("id").equals("salary"))
					{
						//System.out.println("JO="+jo);
						JSONArray jsonSalary = (JSONArray) jo.get("items");
						if (jsonSalary != null) 
						{ 
						   int lenSalary = jsonSalary.size();						  
						   for (int j=0;j<lenSalary;j++)
							{
								JSONObject joSalary = (JSONObject) jsonSalary.get(j);
								//преобразуем строку в число рублей
								int rur = Integer.valueOf((""+joSalary.get("name")).replaceAll("\\D+",""));
								//чилос вакансий для зарплаты
								int count = Integer.valueOf(""+joSalary.get("count"));
								//суммируем все зарплаты
								salaryCounter = salaryCounter+rur*count;
								//суммируем вакансии
								vacanciesCounter = vacanciesCounter + count;
							}
						}
					 }					
					}
				}
			}
		
		return salaryCounter/vacanciesCounter;

	}
	
	
	
	
	//Кол-во вакансий по реону с кодом code	
	private int countVacancies(String codeRegion) throws Exception {
 	
		StringBuffer response;
		
		int RegionCounter = 0;
		
		hhapi http = new hhapi();
		
		
		JSONParser parser = new JSONParser();
		String urlAll = "https://api.hh.ru/vacancies?clusters=true&only_with_salary=true&area="+codeRegion;	
			
		response = http.connectToApi(urlAll);
		if(response.toString().length()>0)
			{
				Object objk  = parser.parse(response.toString());
				JSONObject jsonObject = (JSONObject) objk;
				//System.out.println(jsonObject);
				JSONArray jsonArray = (JSONArray) jsonObject.get("clusters");
				//System.out.println(response.toString());
				if (jsonArray != null) 
				{ 
				   int len = jsonArray.size();
				   for (int i=0;i<len;i++)
				   {					
					JSONObject jo = (JSONObject) jsonArray.get(i);					
					//System.out.println("JO="+jo.get("id"));
					//Находим кластер, в кором описаны кол-во вакансий по районам и городам региона
					if(jo.get("id").equals("area"))
					{
						//System.out.println("JO="+jo);
						JSONArray jsonRegion = (JSONArray) jo.get("items");						
						//System.out.println("ITEMS="+jsonRegion);
						if (jsonRegion != null) 
						{ 
						   int lenRegion = jsonRegion.size();						  
						   for (int j=0;j<lenRegion;j++)
							{
								//System.out.println(jsonRegion.get(j));
								JSONObject joRegion = (JSONObject) jsonRegion.get(j);								
								int count = Integer.valueOf(""+joRegion.get("count"));								
								RegionCounter = RegionCounter+count;					

							}
						}
					}
					
						
				   }
				}
			}		
		return RegionCounter;
	}
	//Функция записи вакансий в файл
	private void csvSave (int salaryAverage, int code) throws Exception {
 	
		StringBuffer response;
		String urlPage ="";
		int pages;
		JSONObject jsonObject;
		Object objk ;
		String [] record;
		String csv = "vacancies.csv";		
		
		hhapi http = new hhapi();
		//Создаем новый файл для записи вакансий
		http.checkFileExists(csv);	    
		FileWriter file = new FileWriter(csv, true);
        CSVWriter writer = new CSVWriter(file);
  	
		JSONParser parser = new JSONParser();
	   
		String urlAll = "https://api.hh.ru/vacancies?area="+code+"&clusters=true&enable_snippets=true&only_with_salary=true&salary="+salaryAverage+"&from=cluster_compensation";	
		
		
		response = http.connectToApi(urlAll);
		if(response.toString().length()>0)
			{
				objk  = parser.parse(response.toString());
				jsonObject = (JSONObject) objk;
				pages = Integer.valueOf(""+jsonObject.get("pages"));
							
				String [] csvRecordHead = {"name","responsibility","requirement","currency","from","to"};
				writer.writeNext(csvRecordHead);
				for (int page=0;page<pages;page++)
				{		   
					urlPage = "https://api.hh.ru/vacancies?area="+code+"&clusters=true&enable_snippets=true&only_with_salary=true&salary="+salaryAverage+"&page="+page;	
					response = http.connectToApi(urlPage);	
					if(response.toString().length()>0)
					{
						objk  = parser.parse(response.toString());
						jsonObject = (JSONObject) objk;
						JSONArray jsonArray = (JSONArray) jsonObject.get("items");				
						/*System.out.println(jsonArray);
						System.out.println(page);*/
						if (jsonArray != null) 
						{ 
						for (int j=0;j<jsonArray.size();j++)
							{
								
								//Строки для записи в файл
							   JSONObject jo = (JSONObject) jsonArray.get(j);
							   //System.out.println(jo);
							   JSONObject jsonSnipped = (JSONObject) jo.get("snippet");
							   String responsibility = ""+jsonSnipped.get("responsibility");
							   String requirement = ""+jsonSnipped.get("requirement");
							   
							   JSONObject jsonSalary = (JSONObject) jo.get("salary");
							   String from = ""+jsonSalary.get("from");
							   String to = ""+jsonSalary.get("to");
							   String currency = ""+jsonSalary.get("currency");
							   
							   //JSONObject alternateUrl = (JSONObject) jo.get("alternate_url");
							   String name = ""+ jo.get("name");
							   String alternateUrl = ""+ jo.get("alternate_url");
							   //record = (name+";"+responsibility+";"+requirement+";"+currency+";"+from+";"+to).split(";"); 
								String [] csvRecord = {name,responsibility,requirement,currency,from,to};
								//Записываем
								writer.writeNext(csvRecord);
							   
							   /*System.out.println("responsibility="+responsibility);
							   System.out.println("requirement="+requirement);
							   System.out.println("from="+from);
							   System.out.println("to="+to);
							   System.out.println("name="+name);
							   System.out.println("alternateUrl="+alternateUrl);*/
						   
						   }
						   
						}
					}			
				}
			writer.close();
		}	
	}
	//Функция создания файла
	private void checkFileExists(String filename) throws Exception { 
		File fileCsv = new File(filename);
		if(fileCsv.isFile())
		{			
			//System.out.println("FILE EXISTS");
			fileCsv.delete();
		}
	};
	
	//Функция сохранения записей из БД в файле
	private void csvSaveFromDb (int salaryMin, int salaryMax) throws Exception { 	
		hhapi http = new hhapi();  
		String selectQuery = "SELECT v.name as name, v.responsibility as resp, v.salary_from, v.salary_to, v.currency, v.url, a.name as area_name FROM public.vacancies v inner join public.areas a on a.id=v.area_id  where salary_from >="+salaryMin+" and salary_to<="+salaryMax+" and salary_from<="+salaryMax;
		String name, resp, salaryFrom, salaryTo, currency, url, areaName;
		String csv = "vacancies_salary.csv";		
		http.checkFileExists(csv);
		FileWriter file = new FileWriter(csv, true);
        CSVWriter writer = new CSVWriter(file);
        
		Connection connection = null;
		try {
			connection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/"+DATABASE_NAME, DATABASE_USERNAME,DATABASE_PASSWD);
			
				
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(selectQuery);
			//Шапка
			String [] csvRecordHead = {"name","responsibility","salary_from","salary_to", "currency","url","area_name"};
			writer.writeNext(csvRecordHead);
			while (resultSet.next()) {
				
				
				name = resultSet.getString("name");
				resp = resultSet.getString("resp");
				salaryFrom = resultSet.getString("salary_from");
				salaryTo = resultSet.getString("salary_to");
				currency = resultSet.getString("currency");
				url = resultSet.getString("url");
				areaName = resultSet.getString("area_name");
				 
				String [] csvRecord = {name,resp,salaryFrom,salaryTo,currency,url,areaName};
				writer.writeNext(csvRecord);
				
						
			}	
			writer.close();		

		} catch (SQLException e) {

			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;

		}

		
	}
	
	
	

	//Поиск кода страны по ее названию
	private String countryCode(String countryName) throws Exception 
	{
		String url = "https://api.hh.ru/areas/countries";
		String name = "";
		String resultId = "";
		StringBuffer response;
		hhapi http = new hhapi();  
		//Вызов функции подключения к api hh
		response = http.connectToApi(url);
		
		JSONParser parser = new JSONParser();
		Object objk  = parser.parse(response.toString());
		JSONArray jsonArray = (JSONArray) objk;
		//Обработка ответа
		if (jsonArray != null) 
		{ 
		   int len = jsonArray.size();
		   for (int i=0;i<len;i++)
		   { 			
			JSONObject jo = (JSONObject) jsonArray.get(i);
			name = (String) jo.get("name"); 			
			if(name.equals(countryName))
			{				
				resultId = (String) jo.get("id");
				//System.out.println(resultId);
				break;
			}
		   }		    
		}

		return resultId;
	}
	
	//Функция подключения к api hh и формирования ответа
	private StringBuffer connectToApi(String url) throws Exception 
	{
		URL obj = new URL(url);
		HttpURLConnection con = (HttpURLConnection) obj.openConnection();
		StringBuffer response = new StringBuffer();
		if(response ==null)
		{
		System.out.println("TotalResponse");
		}
		
		con.setRequestMethod("GET");

		con.setRequestProperty("User-Agent", USER_AGENT);

		int responseCode = con.getResponseCode();
		if(responseCode==200)
		{
			//System.out.println("\nSending 'GET' request to URL : " + url);
			//System.out.println("Response Code : " + responseCode);

			BufferedReader in = new BufferedReader(
			new InputStreamReader(con.getInputStream()));
			String inputLine;
			
			
			while ((inputLine = in.readLine()) != null) 
			{
				response.append(inputLine);
			}
			in.close();
		}		
		return response;
	}
	
	//Функция записи данных в БД
	 private void connectPostgres(String SelectQuery, String InsertQuery, String requestName) throws Exception {
		
		 /* Запрос на создание таблиц БД 
		  
		  CREATE SEQUENCE area_ids;
			CREATE TABLE areas (
			  id INTEGER PRIMARY KEY DEFAULT NEXTVAL('area_ids'),
			  name CHAR(64) UNIQUE
			  );
			  
			  CREATE SEQUENCE vacancy_ids;
			CREATE TABLE vacancies (
			  id integer NOT NULL DEFAULT nextval('vacancy_ids'),
			  name CHAR(512),
			  area_id integer REFERENCES areas,
			  responsibility CHAr(512),
			  salary_from integer,
			  salary_to integer,
			  currency CHAR(64),
			  url CHAR(128)
			  );
		 
		 System.out.println("-------- PostgreSQL "
				+ "JDBC Connection Testing ------------");*/

		try {

			Class.forName("org.postgresql.Driver");

		} catch (ClassNotFoundException e) {

			System.out.println("Where is your PostgreSQL JDBC Driver? "
					+ "Include in your library path!");
			e.printStackTrace();
			return;

		}

		//System.out.println("PostgreSQL JDBC Driver Registered!");

		Connection connection = null;

		try {

			connection = DriverManager.getConnection(
					"jdbc:postgresql://127.0.0.1:5432/"+DATABASE_NAME, DATABASE_USERNAME,DATABASE_PASSWD);
							
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(SelectQuery);
			/*System.out.println("SelectQuery="+SelectQuery);
			System.out.println("InsertQuery="+InsertQuery);
			System.out.println("resultSet"+resultSet);*/

			while (resultSet.next()) {
				//System.out.println("resultSet.getString('count')"+resultSet.getString("count"));
				if(requestName=="country"&&Integer.valueOf(resultSet.getString("count"))==0 || requestName=="vacancy"&&Integer.valueOf(resultSet.getString("count"))==1) 
					{
						
					statement.executeQuery(InsertQuery);
					statement.close();
					connection.close();
				}							
			}			

		} catch (SQLException e) {

			//System.out.println("Connection Failed! Check output console");
			//e.printStackTrace();
			return;

		}

		if (connection != null) {
			System.out.println("Поключение установлено. Записываем данные в БД");
		} else {
			System.out.println("Failed to make connection!");
		}
	}	
	
 };
	
	
	
	

