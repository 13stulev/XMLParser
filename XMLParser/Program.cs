// See https://aka.ms/new-console-template for more information

using Microsoft.Data.SqlClient;
using System.Xml.Linq;

public class Program
{
    public static void Main()
    {
        XElement document = XElement.Load("data.xml");
        string connectionString = "Server=localhost;Database=master;Trusted_Connection=True;Encrypt=False;";
        SqlConnection connection = new SqlConnection(connectionString);

        connection.Open();

        insertUsers(document, connection);
        insertProducts(document, connection);
        insertOrders(document, connection);
        insertPurchases(document, connection);

        connection.Close();
    }
    public static void insertUsers(XElement document, SqlConnection connection)
    {
        var users = document.Descendants("user").Select(element => new
        {
            name = element.Element("fio").Value,
            mail = element.Element("email").Value
        }).Distinct();
        if (users != null) { 
        foreach (var user in users)
            {
                string sqlQuery = "INSERT INTO \"user\" (fio, mail) SELECT @fio, @email WHERE NOT EXISTS(select fio, mail FROM \"user\" WHERE fio = @fio AND mail = @email)";
                SqlCommand command = new SqlCommand(sqlQuery, connection);
                SqlParameter fioParameter = new SqlParameter("@fio", user.name);
                command.Parameters.Add(fioParameter);
                SqlParameter emailParameter = new SqlParameter("@email", user.mail);
                command.Parameters.Add(emailParameter);
                command.ExecuteNonQuery();
            }
        }
    }

    public static void insertProducts(XElement document, SqlConnection connection)
    {
        var products = document.Descendants("product").Select(element => new
        {
            name = element.Element("name").Value,
            price = element.Element("price").Value
        }).Distinct();
        if(products != null) {
            foreach (var product in products)
            {
                string sqlQuery = "INSERT INTO product (productName, price) SELECT @productName, @price WHERE NOT EXISTS(SELECT productName, price FROM product WHERE productName = @productName AND price = @price)";
                SqlCommand command = new SqlCommand(sqlQuery, connection);
                SqlParameter nameParameter = new SqlParameter("@productName", product.name);
                command.Parameters.Add(nameParameter);
                SqlParameter priceParameter = new SqlParameter("@price", Double.Parse(product.price.Replace('.', ',')));
                command.Parameters.Add(priceParameter);
                command.ExecuteNonQuery();
            }
        }
        
    }

    public static void insertOrders(XElement document, SqlConnection connection)
    {
        var orders = document.Descendants("order").Select(element => new
        {
            num = element.Element("no").Value,
            sum = element.Element("sum").Value,
            regDate = element.Element("reg_date").Value,
            name = element.Element("user").Element("fio").Value,
            mail = element.Element("user").Element("email").Value,
            products = element.Elements("product").Select(element => new
            {
                name = element.Element("name").Value,
                price = element.Element("price").Value,
                quantity = element.Element("quantity").Value
            })
        });
        if (orders != null)
        {
            foreach (var order in orders)
            {
                string sqlQuery = "INSERT INTO \"order\" (num, regdate, userId, \"sum\") SELECT @num, @reg_date, (SELECT userId FROM \"user\" WHERE fio = @name AND mail = @email), @sum WHERE NOT EXISTS(SELECT num FROM \"order\" WHERE num = @num)";
                SqlCommand command = new SqlCommand(sqlQuery, connection);
                SqlParameter numParameter = new SqlParameter("@num", Int32.Parse(order.num));
                command.Parameters.Add(numParameter);
                SqlParameter regDateParameter = new SqlParameter("@reg_date", DateOnly.Parse(order.regDate));
                command.Parameters.Add(regDateParameter);
                SqlParameter fioParameter = new SqlParameter("@name", order.name);
                command.Parameters.Add(fioParameter);
                SqlParameter emailParameter = new SqlParameter("@email", order.mail);
                command.Parameters.Add(emailParameter);
                SqlParameter sumParameter = new SqlParameter("@sum", Double.Parse(order.sum.Replace('.', ',')));
                command.Parameters.Add(sumParameter);
                command.ExecuteNonQuery();
            }
        }
    }

    public static void insertPurchases(XElement document, SqlConnection connection)
    {
        var orders = document.Descendants("order").Select(element => new
        {
            num = element.Element("no").Value,
            products = element.Elements("product").Select(element => new
            {
                name = element.Element("name").Value,
                price = element.Element("price").Value,
                quantity = element.Element("quantity").Value
            })
        }).Distinct();

        if(orders != null) {
            
                foreach (var order in orders)
                {
                    foreach (var product in order.products)
                    {
                    if (order.products != null)
                        {
                            string sqlQuery = "INSERT INTO purchase(quantity, productId, num) SELECT @quantity, (SELECT productId FROM product WHERE productName = @productName and price = @price) id, @num";
                            SqlCommand command = new SqlCommand(sqlQuery, connection);
                            SqlParameter nameParameter = new SqlParameter("@productName", product.name);
                            command.Parameters.Add(nameParameter);
                            SqlParameter priceParameter = new SqlParameter("@price", Double.Parse(product.price.Replace('.', ',')));
                            command.Parameters.Add(priceParameter);
                            SqlParameter quantityParameter = new SqlParameter("@quantity", Int32.Parse(product.quantity));
                            command.Parameters.Add(quantityParameter);
                            SqlParameter numParameter = new SqlParameter("@num", Int32.Parse(order.num));
                            command.Parameters.Add(numParameter);
                            command.ExecuteNonQuery();
                        }
                    }
                }
            
            
        }
        
    }
}


