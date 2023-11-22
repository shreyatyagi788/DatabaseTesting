package storedFunctionsTesting;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class SFTesting {
	
	Connection con = null;
	Statement stmt = null;
	CallableStatement cstmt;
	ResultSet rs;
	ResultSet rs1;
	ResultSet rs2;

	
	@BeforeClass
	void setup() throws SQLException {
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}
	
	@AfterClass
	void tearDown() throws SQLException {
		
		con.close();
	}
	
	//@Test(priority=1)
	void test_storedFunctionExist() throws SQLException
	{
		rs=con.createStatement().executeQuery("show function status where name = 'customerLevel'");
		rs.next();
		Assert.assertEquals(rs.getString("Name"),"customerLevel");
		
	}
	
	//@Test(priority=2)
	void test_customerLevelWithStatement() throws SQLException {
		rs1=con.createStatement().executeQuery("select customerName,customerLevel(creditLimit) from customers");
		rs2=con.createStatement().executeQuery("select customerName, case when creditLimit>50000 then 'platinum' when creditLimit>=10000 and creditLimit<=50000 then 'gold' when creditLimit<10000 then 'silver' end as customerlevel from customers");
		Assert.assertEquals(compareResultSets(rs1, rs2),true);
		
	}
	
	@Test(priority=3)
	void test_customerLeveWithStoredProcedure() throws SQLException {
		cstmt = con.prepareCall("{call getCustomerLevel(?,?)}");
		cstmt.setInt(1, 131);
		cstmt.registerOutParameter(2, Types.VARCHAR);
		cstmt.executeQuery();
		String custLevel=cstmt.getString(2);
		rs=con.createStatement().executeQuery("select customerName, case when creditLimit>50000 then 'platinum' when creditLimit>=10000 and creditLimit<=50000 then 'gold' when creditLimit<10000 then 'silver' end as customerlevel from customers where customerNumber=131");
		rs.next();
		String exp_custLevel=rs.getString("customerLevel");
		Assert.assertEquals(custLevel, exp_custLevel);
		
		
	}
	
	public boolean compareResultSets(ResultSet resultset1, ResultSet resultset2) throws SQLException {
		while(resultset1.next())
		{
			resultset2.next();
			int count = resultset1.getMetaData().getColumnCount();
			for(int i=1;i<=count;i++)
			{
				if(!StringUtils.equals(resultset1.getString(i),resultset2.getString(i)))
				{
					return false;
				}
			}
		}
		return true;
	}
}
