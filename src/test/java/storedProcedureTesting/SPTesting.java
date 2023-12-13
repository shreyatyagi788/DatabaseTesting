package storedProcedureTesting;

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

/*syntax                                 stored procedures
 {call procedure_name() }         accept no parameter and returns no value
 {call procedure_name(?,?) }      accept two parameters and return no value
 {?= call procedure_name()}       accept no parameter and return  value
 {?= call procedure_name(?)}      accept one parameter and return value
 */

public class SPTesting {
	
	Connection con = null;
	Statement stmt = null;
	CallableStatement cstmt; //when we call stored procedures from jdbc we need to use 'CallableStatement' class and then prepare a call.
	ResultSet rs;
	
	@BeforeClass
	void setup() throws SQLException {
		con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels","root","root");
	}
	
	@AfterClass
	void tearDown() throws SQLException {
		
		con.close();
	}
	
	//@Test(priority=1)
	void test_storedProcedureExists() throws SQLException {
		stmt = con.createStatement();
		rs= stmt.executeQuery("show procedure status where name = 'selectAllCustomers'");
		rs.next();
		Assert.assertEquals(rs.getString("Name"), "selectAllCustomers");
	}
	
	//@Test(priority=2)
	void test_selectAllCustomers() throws SQLException {
		
		cstmt = con.prepareCall("{call selectAllCustomers()}");
		ResultSet rs1=cstmt.executeQuery();
		stmt = con.createStatement();
		ResultSet rs2=stmt.executeQuery("select * from customers");
		Assert.assertEquals(compareResultSets(rs1,rs2), true);
	}
	
	//@Test(priority=3)
	void test_selectAllCustomerByCity() throws SQLException {
		cstmt = con.prepareCall("{call selectAllCustomerByCity(?)}");
		cstmt.setString(1, "Singapore");
		ResultSet rs1=cstmt.executeQuery();
		stmt=con.createStatement();
		ResultSet rs2=stmt.executeQuery("select * from customers where city ='Singapore'");
		Assert.assertEquals(compareResultSets(rs1, rs2),true);
	}
	
	//@Test(priority=4)
	void test_selectAllCustomerByCityAndPin() throws SQLException {
		cstmt = con.prepareCall("{call selectCustomersByCityAndPin(?,?)}");
		cstmt.setString(1, "Singapore");
		cstmt.setString(2, "079903");
		ResultSet rs1=cstmt.executeQuery();
		stmt=con.createStatement();
		ResultSet rs2=stmt.executeQuery("select * from customers where city ='Singapore' and pin = '079903'");
		Assert.assertEquals(compareResultSets(rs1, rs2),true);
	}
	
	//@Test(priority=5)
	void test_getOrderByCustomer() throws SQLException
	{
		cstmt = con.prepareCall("{call getOrderByCustomer(?,?,?,?,?)}");
		cstmt.setInt(1, 141);
		cstmt.registerOutParameter(2, Types.INTEGER);
		cstmt.registerOutParameter(3, Types.INTEGER);
		cstmt.registerOutParameter(4, Types.INTEGER);
		cstmt.registerOutParameter(5, Types.INTEGER);
		cstmt.executeQuery();
		int shipped = cstmt.getInt(2);
		int canceled = cstmt.getInt(3);
		int resolved = cstmt.getInt(4);
		int disputed = cstmt.getInt(5);
		rs = con.createStatement().executeQuery("select(select count(*) as 'shipped' from orders where customerNumber=141 and status='Shipped') as shipped,(select count(*) as 'canceled' from orders where customerNumber=141 and status='Canceled') as canceled,(select count(*) as 'resolved' from orders where customerNumber=141 and status='Resolved') as resolved,(select count(*) as 'disputed' from orders where customerNumber=141 and status='Disputed') as disputed");
		rs.next();
		int exp_shpd = rs.getInt("shipped");
		int exp_cncld = rs.getInt("canceled");
		int exp_rslvd = rs.getInt("resolved");
		int exp_dsptd = rs.getInt("disputed");
		
		if(shipped == exp_shpd && canceled == exp_cncld && resolved == exp_rslvd && disputed == exp_dsptd)
			Assert.assertTrue(true);
		else
			Assert.assertTrue(false);

	}

	@Test(priority=6)
	void test_getCustomerShipping() throws SQLException {
		cstmt = con.prepareCall("{call getCustomerShipping(?,?)}");
		cstmt.setInt(1, 112);
		cstmt.registerOutParameter(2, Types.VARCHAR);
		cstmt.executeQuery();
		String shippingTime = cstmt.getString(2);
		
		rs=con.createStatement().executeQuery("select country, case when country = 'USA' then '2-day-shipping' when country = 'Canada' then '3-day-shipping' else '5-day-shipping' end as ShippingTime from customers where customerNumber=112");
		rs.next();
		String exp_shpTime = rs.getString("ShippingTime");
		Assert.assertEquals(shippingTime, exp_shpTime);
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
