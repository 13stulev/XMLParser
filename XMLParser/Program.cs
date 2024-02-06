// See https://aka.ms/new-console-template for more information

using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Data.Common;
using System.Transactions;
using System.Xml.Linq;

public class Program
{
    public static void Main()
    {
        //Обязательно прописать путь до считываемого файла
        XElement document = XElement.Load("data.xml");
        string connectionString = "Server=localhost;Database=master;Trusted_Connection=True;Encrypt=False;";
        SqlConnection connection = new SqlConnection(connectionString);

        connection.Open();

        Console.WriteLine("Добавление пользователей");
        insertUsers(document, connection);
        Console.WriteLine("Добавление товара");
        insertProducts(document, connection);
        Console.WriteLine("Добавление заказов");
        insertOrders(document, connection);
        Console.WriteLine("Добавление покупок");
        insertPurchases(document, connection);
        Console.WriteLine("Готово!");
        connection.Close();
    }
    public static void insertUsers(XElement document, SqlConnection connection)
    {
        var users = document.Descendants("user").Select(element => new
        {
            name = element.Element("fio")?.Value,
            mail = element.Element("email")?.Value
        }).Distinct();

        if (users != null)
        {
            foreach (var user in users)
            {
                SqlTransaction transaction = connection.BeginTransaction();
                try {
                    // В запросе есть проверка на дубликаты
                    string sqlQuery = "INSERT INTO \"user\" (fio, mail) SELECT @fio, @email WHERE NOT EXISTS(select fio, mail FROM \"user\" WHERE fio = @fio AND mail = @email)";
                    SqlCommand command = new SqlCommand(sqlQuery, connection, transaction); 
                    command.Transaction = transaction;
                    SqlParameter fioParameter = new SqlParameter("@fio", user.name is null ? DBNull.Value : user.name);
                    command.Parameters.Add(fioParameter);
                    SqlParameter emailParameter = new SqlParameter("@email", user.mail is null ? DBNull.Value : user.mail);
                    command.Parameters.Add(emailParameter);
                    int rows = command.ExecuteNonQuery();
                    if (rows == 0) Console.WriteLine($"Строка {user.ToString()} не была добавлена");
                    transaction.Commit();
                } catch(Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine(ex.Message);
                }
                
            }
        }
    }

    public static void insertProducts(XElement document, SqlConnection connection)
    {
        var products = document.Descendants("product").Select(element => new
        {
            name = element.Element("name")?.Value,
            price = element.Element("price")?.Value
        }).Distinct();
        if (products != null)
        {
            
            foreach (var product in products)
            {
                SqlTransaction transaction = connection.BeginTransaction();
                try {
                    double res;
                    // В запросе есть проверка на дубликаты
                    string sqlQuery = "INSERT INTO product (productName, price) SELECT @productName, @price WHERE NOT EXISTS(SELECT productName, price FROM product WHERE productName = @productName AND price = @price)";
                    SqlCommand command = new SqlCommand(sqlQuery, connection, transaction);
                    SqlParameter nameParameter = new SqlParameter("@productName", (product.name is null ? DBNull.Value : product.name));
                    command.Parameters.Add(nameParameter);
                    SqlParameter priceParameter = new SqlParameter("@price", Double.TryParse(product.price?.Replace(".", ","), out res) ? res : DBNull.Value);
                    command.Parameters.Add(priceParameter);
                    int rows = command.ExecuteNonQuery();
                    if (rows == 0) Console.WriteLine($"Строка {product.ToString()} не была добавлена");
                    transaction.Commit();
                } catch(Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine(ex.Message);
                }
                
            }
        }
    }

    public static void insertOrders(XElement document, SqlConnection connection)
    {
        var orders = document.Descendants("order").Select(element => new
        {
            num = element.Element("no")?.Value,
            sum = element.Element("sum")?.Value,
            regDate = element.Element("reg_date")?.Value,
            name = element.Element("user")?.Element("fio")?.Value,
            mail = element.Element("user")?.Element("email")?.Value,
            products = element.Elements("product").Select(element => new
            {
                name = element.Element("name")?.Value,
                price = element.Element("price")?.Value,
                quantity = element.Element("quantity")?.Value
            })
        });
        if (orders != null)
        {
            foreach (var order in orders)
            {
                SqlTransaction transaction = connection.BeginTransaction();
                try {
                    // В запросе есть проверка на дубликаты
                    string sqlQuery = "INSERT INTO \"order\" (num, regdate, userId, \"sum\") SELECT @num, @reg_date, (SELECT userId FROM \"user\" WHERE fio = @name AND mail = @email), @sum WHERE NOT EXISTS(SELECT num FROM \"order\" WHERE num = @num)";
                    SqlCommand command = new SqlCommand(sqlQuery, connection, transaction);
                    int num;
                    DateOnly date;
                    double sum;
                    SqlParameter numParameter = new SqlParameter("@num", Int32.TryParse(order.num, out num) ? num : DBNull.Value);
                    command.Parameters.Add(numParameter);
                    SqlParameter regDateParameter = new SqlParameter("@reg_date", DateOnly.TryParse(order.regDate, out date) ? date : DBNull.Value);
                    command.Parameters.Add(regDateParameter);
                    SqlParameter fioParameter = new SqlParameter("@name", order.name is null ? DBNull.Value : order.name);
                    command.Parameters.Add(fioParameter);
                    SqlParameter emailParameter = new SqlParameter("@email", order.mail is null ? DBNull.Value : order.mail);
                    command.Parameters.Add(emailParameter);
                    SqlParameter sumParameter = new SqlParameter("@sum", Double.TryParse(order.sum?.Replace('.', ','), out sum) ? sum : DBNull.Value);
                    command.Parameters.Add(sumParameter);
                    int rows = command.ExecuteNonQuery();
                    if (rows == 0) Console.WriteLine($"Строка {order.ToString()} не была добавлена");
                    transaction.Commit();
                    
                } catch(Exception ex)
                {
                    transaction.Rollback();
                    Console.WriteLine(ex.Message);
                }


            }
        }

    }

    public static void insertPurchases(XElement document, SqlConnection connection)
    {
        var orders = document.Descendants("order").Select(element => new
        {
            num = element.Element("no")?.Value,
            products = element.Elements("product")?.Select(element => new
            {
                name = element.Element("name")?.Value,
                price = element.Element("price")?.Value,
                quantity = element.Element("quantity")?.Value
            })
        }).Distinct();

        if (orders != null)
        {
            foreach (var order in orders)
            {
                foreach (var product in order.products)
                {
                    if (order.products != null)
                    {
                        SqlTransaction transaction = connection.BeginTransaction();
                        try {
                            string sqlQuery = "INSERT INTO purchase(quantity, productId, num) SELECT @quantity, (SELECT productId FROM product WHERE productName = @productName and price = @price) id, @num where not exists(select productId, num from purchase where productId = (SELECT productId FROM product WHERE productName = @productName and price = @price) and num = @num)";
                            SqlCommand command = new SqlCommand(sqlQuery, connection, transaction);
                            int num, quantity;
                            double price;
                            SqlParameter nameParameter = new SqlParameter("@productName", product.name is null ? DBNull.Value : product.name);
                            command.Parameters.Add(nameParameter);
                            SqlParameter priceParameter = new SqlParameter("@price", Double.TryParse(product.price?.Replace('.', ','), out price) ? price : DBNull.Value);
                            command.Parameters.Add(priceParameter);
                            SqlParameter quantityParameter = new SqlParameter("@quantity", Int32.TryParse(product.quantity, out quantity) ? quantity : DBNull.Value);
                            command.Parameters.Add(quantityParameter);
                            SqlParameter numParameter = new SqlParameter("@num", Int32.TryParse(order.num, out num) ? num : DBNull.Value);
                            command.Parameters.Add(numParameter);
                            int rows = command.ExecuteNonQuery();
                            if (rows == 0) Console.WriteLine($"Строка {product.ToString()} не была добавлена");
                            transaction.Commit();
                        } catch(Exception ex)
                        {
                            transaction.Rollback();
                            Console.WriteLine(ex.Message);
                        }
                        
                    }
                }
            }
        }

    }
}