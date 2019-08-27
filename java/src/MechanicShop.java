/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

import java.util.regex.*; 
import java.util.Date; 
import java.util.*; 
import java.text.SimpleDateFormat; 

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class MechanicShop{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public MechanicShop(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + MechanicShop.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		MechanicShop esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new MechanicShop (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. AddCustomer");
				System.out.println("2. AddMechanic");
				System.out.println("3. AddCar");
				System.out.println("4. InsertServiceRequest");
				System.out.println("5. CloseServiceRequest");
				System.out.println("6. ListCustomersWithBillLessThan100");
				System.out.println("7. ListCustomersWithMoreThan20Cars");
				System.out.println("8. ListCarsBefore1995With50000Milles");
				System.out.println("9. ListKCarsWithTheMostServices");
				System.out.println("10. ListCustomersInDescendingOrderOfTheirTotalBill");
				System.out.println("11. < EXIT");
				
				/*
				 * FOLLOW THE SPECIFICATION IN THE PROJECT DESCRIPTION
				 */
				switch (readChoice()){
					case 1: AddCustomer(esql); break;
					case 2: AddMechanic(esql); break;
					case 3: AddCar(esql); break;
					case 4: InsertServiceRequest(esql); break;
					case 5: CloseServiceRequest(esql); break;
					case 6: ListCustomersWithBillLessThan100(esql); break;
					case 7: ListCustomersWithMoreThan20Cars(esql); break;
					case 8: ListCarsBefore1995With50000Milles(esql); break;
					case 9: ListKCarsWithTheMostServices(esql); break;
					case 10: ListCustomersInDescendingOrderOfTheirTotalBill(esql); break;
					case 11: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

    public static void AddCustomer(MechanicShop esql) {//1
        // Customer attributes
        int newID;      // integer
        String fname;   // char(32)
        String lname;   // char(32)
        String phone;   // char(13)
        String address; // char(256)

	    try{
            // create new customer ID
            String ID_query = "SELECT MAX(id) FROM Customer";
            List<List<String>> rs = esql.executeQueryAndReturnResult(ID_query);
            newID = Integer.parseInt(rs.get(0).get(0)) + 1;

            // get customer information
            System.out.println("\tEnter first name: ");
            fname = in.readLine();

            System.out.println("\tEnter last name: ");
            lname = in.readLine();

            System.out.println("\tEnter phone number: ");
            phone = in.readLine();

            System.out.println("\tEnter address: ");
            address = in.readLine();

            // concatenate query string and run
            String query = "INSERT INTO Customer VALUES ";
            query += "( " + Integer.toString(newID) + ", ";
            query += "\'" +  fname + "\', ";
            query += "\'" +  lname + "\', ";
            query += "\'" +  phone + "\', ";
            query += "\'" +  address + "\')";

            // execute the query and update the DB
            esql.executeUpdate(query);
            // test update (sanity check)
            String test_query = "SELECT * FROM Customer WHERE id = ";
            test_query += Integer.toString(newID);
            esql.executeQueryAndPrintResult(test_query);
           
	    }catch(Exception e)
	    {
            System.err.println(e.getMessage ());
	    }		
	}
	
    public static void AddMechanic(MechanicShop esql) {//2
        // Mechanic attributes
        int newID;    // integer
        String fname; // char(32)
        String lname; // char(32)
        String exp;   // integer, 0 <= years < 100
        try{
            // create new mechanic ID
            String ID_query = "SELECT MAX(id) FROM Mechanic";
            List<List<String>> rs = esql.executeQueryAndReturnResult(ID_query);
            newID = Integer.parseInt(rs.get(0).get(0)) + 1;

            // get mechanic information
            System.out.println("\tEnter first name: ");
            fname = in.readLine();

            System.out.println("\tEnter last name: ");
            lname = in.readLine();

            System.out.println("\tEnter years experience: ");
            exp = in.readLine();

            // concatenate query string and run
            String query = "INSERT INTO Mechanic VALUES ";
            query += "( " + Integer.toString(newID) + ", ";
            query += "\'" +  fname + "\', ";
            query += "\'" +  lname + "\', ";
            query += exp  + ")";

            // execute the query and update the DB
            esql.executeUpdate(query);
            // test update (sanity check)
            String test_query = "SELECT * FROM Mechanic WHERE id = ";
            test_query += Integer.toString(newID);
            esql.executeQueryAndPrintResult(test_query);
           
	    }catch(Exception e)
	    {
            System.err.println(e.getMessage ());
	    }		
	}
	
	public static void AddCar(MechanicShop esql){//3
        String vin;   // varchar(16)
        String make;  // varchar(32)
        String model; // varchar(32)
        String year;  // integer, year >= 1970
	    try{
            // get car information
            System.out.println("\tEnter VIN: ");
            vin = in.readLine();

            System.out.println("\tEnter make: ");
            make = in.readLine();

            System.out.println("\tEnter model: ");
            model = in.readLine();

            System.out.println("\tEnter year: ");
            year = in.readLine();

            // concatenate query string and run
            String query = "INSERT INTO Car VALUES ";
            query += "(\'" + vin   + "\', ";
            query += "\'"  + make  + "\', ";
            query += "\'"  + model + "\', ";
            query += year  + ")";

            // execute the query and update the DB
            esql.executeUpdate(query);
            // test update (sanity check)
            String test_query = "SELECT * FROM Car WHERE vin = \'" +vin+ "\'";
            esql.executeQueryAndPrintResult(test_query);
       
	    }catch(Exception e)
	    {
            System.err.println (e.getMessage ());
	    }		
	}
	
	public static void InsertServiceRequest(MechanicShop esql){//4
        int newRID;      // integer
        int cust_id;     // integer, from Customer(id)
        String vin;      // varchar(16), from Car(vin)
        String date;     // date
        String odo;      // integer, val > 0
        String complain; // text
        try{
            // create new user ID
            String ID_query = "SELECT MAX(rid) FROM Service_Request";
            List<List<String>> rs = esql.executeQueryAndReturnResult(ID_query);
            newRID = Integer.parseInt(rs.get(0).get(0)) + 1;
            System.out.println(newRID);

            // get customer information
            System.out.println("\tEnter customer ID: ");
            cust_id = Integer.parseInt(in.readLine());

            System.out.println("\tEnter car VIN: ");
            vin = in.readLine();

            date = new SimpleDateFormat("MM/dd/yyyy").format(Calendar.getInstance().getTime());  

            System.out.println("\tEnter milage: ");
            odo = in.readLine();

            System.out.println("\tEnter complaint: ");
            complain = in.readLine();
            // concatenate query string and run
            String query = "INSERT INTO Customer VALUES (";
            query += Integer.toString(newRID) + ", ";
            query += cust_id + ", ";
            query += "\'" + vin + "\', ";
            query += "\'" + date + "\', ";
            query += "\'" + odo + "\')";
            query += "\'" + complain + "\')";

            // execute the query and update the DB
            esql.executeUpdate(query);
       
	    }catch(Exception e)
	    {
            System.err.println (e.getMessage ());
	    }		
	}
	
	public static void CloseServiceRequest(MechanicShop esql) throws Exception{//5
	int mid = 0;	//mechanic ID reference 
	int rid = 0; 	//service request ID reference
	int wid = 0;	//new ID for the closed service request (wid + 1)
	int bill; 	//amount billed for this service request 
	String comment; //comment for the closed service request
	String date;
	String ID_query; 
	List<List<String>> rs;  
	try {
		//get current date 
		date = new SimpleDateFormat("MM/dd/yyyy").format(Calendar.getInstance().getTime());  

		//get mechanic ID
		System.out.println("\tEnter mechanic ID: "); 
		mid = Integer.parseInt(in.readLine());
		ID_query = String.format("SELECT * FROM Mechanic WHERE %d = id", mid); 
		rs = esql.executeQueryAndReturnResult(ID_query); 
		if (rs.size() == 0) {
			System.out.println("Invalid mechanic ID"); 
			return; 
		}
		 
		//get service request ID
		System.out.println("\tEnter service request ID: "); 
		rid = Integer.parseInt(in.readLine());
		//check whether service request actually exists before closing
		ID_query = String.format("SELECT * FROM Service_Request WHERE %d = rid", rid); 
		rs = esql.executeQueryAndReturnResult(ID_query); 
		if (rs.size() == 0) {
			System.out.println("Service Request does not exist with that ID"); 
			return; 
		} 
		//check whether the service request has been closed already
		ID_query = String.format("SELECT * FROM Closed_Request WHERE %d = rid", rid); 
		rs = esql.executeQueryAndReturnResult(ID_query); 
		if (rs.size() > 0) {
			System.out.println("Service request with that ID has already been closed"); 
			return; 
		} 	
		
		//get bill amount for this service request
		System.out.println("\tEnter bill amount: "); 
		bill = Integer.parseInt(in.readLine()); 

		//grab comment for closed service request
		System.out.println("\tEnter any comments: "); 
		comment = in.readLine(); 

		//create new closed request ID (WID) 
		ID_query = "SELECT MAX(wid) FROM Closed_Request"; 
		rs = esql.executeQueryAndReturnResult(ID_query); 
		wid = Integer.parseInt(rs.get(0).get(0)) + 1; 
		System.out.println(wid);

		//create final query 
		String query = "INSERT INTO Closed_Request VALUES ("; 
		query += Integer.toString(wid) + ", "; 
		query += Integer.toString(rid) + ", "; 
		query += Integer.toString(mid) + ", "; 
		query += "\'" + date + "\', "; 
		query += "\'" + comment + "\', "; 
		query += Integer.toString(bill) + ")"; 
		
		//execute query 
		esql.executeUpdate(query); 
		
		// test update (sanity check)
        String test_query = "SELECT * FROM Closed_Request WHERE rid = ";
        test_query += Integer.toString(rid);
        esql.executeQueryAndPrintResult(test_query);
	}
	catch(Exception e) {
		System.err.println(e.getMessage()); 
	}
	}
	
	public static void ListCustomersWithBillLessThan100(MechanicShop esql){//6
		String query = ""; 
		String result = ""; 
		String total_msg = ""; 
		List<List<String>> rs; 
		try {
			query = "SELECT Customer.fname, Customer.lname, Closed_Request.bill FROM Customer, Service_Request, Closed_Request WHERE Customer.id = Service_Request.customer_id AND Service_Request.rid = Closed_Request.rid AND bill < 100 ORDER BY fname;"; 
			rs = esql.executeQueryAndReturnResult(query); 
			for (int i = 0; i < rs.size(); ++i) {
				result += "\nName: " + rs.get(i).get(0) + rs.get(i).get(1) + "\n"; 
				result += "Bill: " + rs.get(i).get(2) + "\n"; 	
			}
			total_msg = "Total customers with bill less than 100: " + rs.size(); 
			System.out.println(total_msg); 
			System.out.println(result); 
		}
		catch (java.sql.SQLException e) {
			System.out.println(e.getMessage()); 
		}

	}
	
	public static void ListCustomersWithMoreThan20Cars(MechanicShop esql){//7
		String query = "";
		String result = "";
		String total_msg = "";   
		List<List<String>> rs; 
		try {
			query = "SELECT Customer.fname, Customer.lname, COUNT(*) FROM Customer, Owns, Car WHERE Customer.id = Owns.customer_id AND Owns.car_vin = Car.vin GROUP BY Customer.id HAVING COUNT(*) > 20;"; 
			rs = esql.executeQueryAndReturnResult(query); 
			for (int i = 0; i < rs.size(); ++i) {
				result += "\nName: " + rs.get(i).get(0) + rs.get(i).get(1) + "\n"; 
				result += "Number of cars: " + rs.get(i).get(2) + "\n"; 
			}
			total_msg = "Total customers owning more than 20 cars: " + rs.size(); 
			System.out.println(total_msg); 
			System.out.println(result); 
		}
		catch (java.sql.SQLException e) {
			System.out.println(e.getMessage()); 
		}
	}
	
	public static void ListCarsBefore1995With50000Milles(MechanicShop esql){//8
		String query = ""; 
		String result = ""; 
		String total_msg = ""; 
		List<List<String>> rs; 
		try {
			query = "SELECT DISTINCT Car.make, Car.model, Car.year FROM Car, Service_Request WHERE Car.vin = Service_Request.car_vin AND Car.year < 1995 AND Service_Request.odometer < 50001 ORDER BY Car.year;"; 
			rs = esql.executeQueryAndReturnResult(query); 
			for (int i = 0; i < rs.size(); ++i) {
				result += "\nMake: " + rs.get(i).get(0) + " "; 
				result += "Model: " + rs.get(i).get(1) + " "; 
				result += "Year: " + rs.get(i).get(2) + "\n"; 
			}
			total_msg = "Total cars made before 1995 with less than or equal to 50000 miles: " + rs.size(); 
			System.out.println(total_msg); 
			System.out.println(result); 
		}
		catch (java.sql.SQLException e) {
			System.out.println(e.getMessage()); 
		}
	}
	
	public static void ListKCarsWithTheMostServices(MechanicShop esql){//9
		int k;
		String output = "";
		String query;
		List<List<String>> rs;  

		try {
			System.out.print("\tEnter k value (k > 0): "); 
			k = Integer.parseInt(in.readLine());
			

			query = String.format("Select make, model, year, COUNT(*) FROM Car, Service_Request WHERE vin = car_vin GROUP BY vin ORDER BY COUNT(*) DESC LIMIT %d", k); 
			rs = esql.executeQueryAndReturnResult(query); 

			for (int i = 0; i < rs.size(); ++i) {
				output += "Pos: "  + Integer.toString(i+1) + "\n";
				output += "Make: "  + rs.get(i).get(0) + "\n";
				output += "Model: " + rs.get(i).get(1) + "\n";
				output += "Year: "  + rs.get(i).get(2) + "\n";
				output += "Count: " + rs.get(i).get(3) + "\n";
				output += "\n";
			}

			System.out.println(output);

		}
		catch(Exception e) {
			System.err.println(e.getMessage()); 
		}
		
	}
	
	public static void ListCustomersInDescendingOrderOfTheirTotalBill(MechanicShop esql){//9
		String query = ""; 
		String result = ""; 
		List<List<String>> rs; 
		try {
			query = "SELECT Customer.fname, Customer.lname, SUM(Closed_Request.bill) FROM Customer, Service_Request, Closed_Request WHERE Customer.id = Service_Request.customer_id AND Service_Request.rid = Closed_Request.rid GROUP BY Customer.id ORDER BY SUM(bill) DESC;"; 
			rs = esql.executeQueryAndReturnResult(query); 
			for (int i = 0; i < rs.size(); ++i) {
				result += "\nName: " + rs.get(i).get(0) + rs.get(i).get(1) + "\n"; 
				result += "Total bill: " + rs.get(i).get(2) + "\n"; 
			} 
			System.out.println(result); 
		}
		catch (java.sql.SQLException e) {
			System.out.println(e.getMessage()); 
		}
	}
	
}
