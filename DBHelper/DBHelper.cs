using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using HmExtension.Commons.Extensions;
using HmExtension.Commons.utils;

namespace Helper;

/// <summary>
/// 数据库操作类
/// </summary>
public class DBHelper
{
    private Func<DbConnection> _connectionFactory;

    private Func<string, object, DbParameter> _parameterFactory;

    /// <summary>
    /// 打印SQL事件,<see cref="IsPrintSql"/> 为True时可用
    /// </summary>
    private event Action<string> OnPrintSql;

    /// <summary>
    /// 打印参数事件,<see cref="IsPrintParameters"/>为True时可用
    /// </summary>
    private event Action<DbParameter[]> OnPrintParameters;

    /// <summary>
    /// 打印结果事件,<see cref="IsPrintResult"/>为True时可用
    /// </summary>
    private event Action<object> OnPrintResult;

    /// <summary>
    /// 是否打印SQL
    /// </summary>
    public bool IsPrintSql { get; set; } = false;

    /// <summary>
    /// 是否打印参数
    /// </summary>
    public bool IsPrintParameters { get; set; } = false;

    /// <summary>
    /// 是否打印结果
    /// </summary>
    public bool IsPrintResult { get; set; } = false;

    private void PrintSqlAndParameters(DbCommand command)
    {
        if (IsPrintSql)
        {
            OnPrintSql?.Invoke(command.CommandText);
        }

        if (IsPrintParameters)
        {
            OnPrintParameters?.Invoke(command.Parameters.Cast<DbParameter>().ToArray());
        }
    }

    private DataTable PrintResult(DataTable dataTable)
    {
        if (IsPrintResult)
        {
            try
            {
                OnPrintResult?.Invoke(dataTable);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        return dataTable;
    }

    private int PrintResult(int result)
    {
        if (IsPrintResult)
        {
            try
            {
                OnPrintResult?.Invoke(result);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        return result;
    }

    private object PrintResult(object result)
    {
        if (IsPrintResult)
        {
            try
            {
                OnPrintResult?.Invoke(result);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
            }
        }

        return result;
    }

    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="connectionFactory"></param>
    /// <param name="parameterFactory">参数对象创建工厂,可空</param>
    /// <exception cref="ArgumentNullException"></exception>
    public DBHelper(Func<DbConnection> connectionFactory, Func<string, object, DbParameter> parameterFactory = null)
    {
        _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));

        OnPrintSql += sql => Console.WriteLine($"SQL: {sql}");
        OnPrintParameters += parameters =>
        {
            Console.WriteLine(
                $"Parameters: {string.Join(",", parameters.Select(p => $"{p.ParameterName}={p.Value}").ToArray())}");
        };
        OnPrintResult += result =>
        {
            Console.Write("Result:\t");
            if (result is DataTable dataTable)
            {
                Console.WriteLine();
                // 输出列名
                foreach (DataColumn column in dataTable.Columns)
                {
                    Console.Write($"{column.ColumnName}\t");
                }

                Console.WriteLine();
                // 输出数据
                foreach (DataRow row in dataTable.Rows)
                {
                    foreach (var item in row.ItemArray)
                    {
                        Console.Write($"{item}\t");
                    }

                    Console.WriteLine();
                }
            }
            else
            {
                Console.WriteLine(result);
            }
        };
        _parameterFactory = parameterFactory;
    }

    /// <summary>
    /// 获得数据库连接对象
    /// </summary>
    /// <param name="autoConnected">是否自动连接数据库</param>
    /// <returns></returns>
    public DbConnection GetConnection(bool autoConnected = true)
    {
        var connection = _connectionFactory();
        if (autoConnected)
        {
            connection.Open();
        }

        return connection;
    }

    private DbParameter CreateParameter(string name, object value)
    {
        if (_parameterFactory == null) throw new InvalidExpressionException("要进行简化参数传参,parameterFactory不能为空,请在构造方法中指定");
        return _parameterFactory(name, value);
    }

    ///  <summary>
    ///  执行非查询语句
    ///  <example>
    /// var user = new UserInfo
    ///  {
    ///      Username = "admin",
    ///      Password = "123"
    ///  };
    ///  helper.IsPrintSql = true;
    ///  helper.IsPrintParameters = true;
    ///  helper.IsPrintResult = true;
    ///  // 传对象
    ///  "传对象".Println();
    ///  helper.ExecuteNonQuery("insert into user_info(username,password) values(@Username,@Password)", user);
    ///  
    ///  // 传字典
    ///  "传字典".Println();
    ///  var dict = new Dictionary&lt;string, object&gt;
    ///  {
    ///      { "Username", "root" },
    ///      { "Password", "root" }
    ///  };
    ///  helper.ExecuteNonQuery("insert into user_info(username,password) values(@Username,@Password)", dict);
    ///  
    ///  </example>
    ///  </summary>
    ///  <param name="connection">数据库连接对象</param>
    ///  <param name="sql">sql语句</param>
    ///  <param name="parameter">
    ///  简化参数对象,支持普通对象和字典,如果是普通对象,对象的属性名作为参数名,如:
    ///  select * from user_info where id = @Id => 其中参数为@Id,那么在对象中的属性名为Id,字典中的Key也为Id
    ///  </param>
    ///  <returns></returns>
    public int ExecuteNonQuery(DbConnection connection, string sql, object parameter)
    {
        var parameters = patternSql<object>(sql, parameter);

        return ExecuteNonQuery(connection, sql, parameters);
    }

    ///  <summary>
    ///  执行非查询语句
    ///  <example>
    /// var user = new UserInfo
    ///  {
    ///      Username = "admin",
    ///      Password = "123"
    ///  };
    ///  helper.IsPrintSql = true;
    ///  helper.IsPrintParameters = true;
    ///  helper.IsPrintResult = true;
    ///  // 传对象
    ///  "传对象".Println();
    ///  helper.ExecuteNonQuery("insert into user_info(username,password) values(@Username,@Password)", user);
    ///  
    ///  // 传字典
    ///  "传字典".Println();
    ///  var dict = new Dictionary&lt;string, object&gt;
    ///  {
    ///      { "Username", "root" },
    ///      { "Password", "root" }
    ///  };
    ///  helper.ExecuteNonQuery("insert into user_info(username,password) values(@Username,@Password)", dict);
    ///  
    ///  </example>
    ///  </summary>
    ///  <param name="sql">sql语句</param>
    ///  <param name="parameter">
    ///  简化参数对象,支持普通对象和字典,如果是普通对象,对象的属性名作为参数名,如:
    ///  select * from user_info where id = @Id => 其中参数为@Id,那么在对象中的属性名为Id,字典中的Key也为Id
    ///  </param>
    ///  <returns></returns>
    public int ExecuteNonQuery(string sql, object parameter)
    {
        using var connection = GetConnection();
        return ExecuteNonQuery(connection, sql, parameter);
    }

    /// <summary>
    /// 执行非查询SQL语句
    /// </summary>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">SQL参数</param>
    /// <returns>受影响行</returns>
    public int ExecuteNonQuery(string sql, params DbParameter[] parameters)
    {
        using var connection = GetConnection();
        return ExecuteNonQuery(connection, sql, parameters);
    }

    /// <summary>
    /// 执行非查询SQL语句
    /// </summary>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">SQL参数</param>
    /// <returns>受影响行</returns>
    public int ExecuteNonQuery(DbConnection connection, string sql, params DbParameter[] parameters)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddRange(parameters);
        PrintSqlAndParameters(command);
        return PrintResult(command.ExecuteNonQuery());
    }

    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public T ExecuteQuery<T>(string sql, Func<DataTable, T> resultHandler, params DbParameter[] parameters)
    {
        using var connection = GetConnection();
        return ExecuteQuery<T>(connection, sql, resultHandler, parameters);
    }

    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameter">简化参数对象,支持普通对象和字典,如果是普通对象,对象的属性名作为参数名,如:
    ///  select * from user_info where id = @Id => 其中参数为@Id,那么在对象中的属性名为Id,字典中的Key也为Id
    ///  </param>
    /// <returns>查询结果</returns>
    public T ExecuteQuery<T>(string sql, Func<DataTable, T> resultHandler,
        object parameter)
    {
        using var connection = GetConnection();
        return ExecuteQuery(connection, sql, resultHandler, parameter);
    }

    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameter">简化参数对象,支持普通对象和字典,如果是普通对象,对象的属性名作为参数名,如:
    ///  select * from user_info where id = @Id => 其中参数为@Id,那么在对象中的属性名为Id,字典中的Key也为Id
    ///  </param>
    /// <returns>查询结果</returns>
    public T ExecuteQuery<T>(DbConnection connection, string sql, Func<DataTable, T> resultHandler,
        object parameter)
    {
        var parameters = patternSql<T>(sql, parameter);
        return ExecuteQuery(connection, sql, resultHandler, parameters);
    }

    private DbParameter[] patternSql<T>(string sql, object parameter)
    {
        var matches = Regex.Matches(sql, @"@(\w+)");
        var parameters = new DbParameter[matches.Count];
        for (var i = 0; i < matches.Count; i++)
        {
            var match = matches[i];
            var name = match.Groups[1].Value;
            object value = null;
            if (parameter is Dictionary<string, object> dict)
            {
                value = dict[name];
            }
            else
            {
                value = parameter.GetPropertyValue<object>(name);
            }

            parameters[i] = CreateParameter(name, value);
        }

        return parameters;
    }

    /// <summary>
    /// 执行查询SQL语句
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public T ExecuteQuery<T>(DbConnection connection, string sql, Func<DataTable, T> resultHandler,
        params DbParameter[] parameters)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddRange(parameters);
        PrintSqlAndParameters(command);
        using var reader = command.ExecuteReader();
        var dataTable = new DataTable();
        dataTable.Load(reader);
        return resultHandler(PrintResult(dataTable));
    }

    /// <summary>
    /// 执行查询SQL语句,返回List
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameter">参数对象,支持普通对象和字典</param>
    /// <returns>查询结果</returns>
    public List<T> ExecuteQuery<T>(DbConnection connection, string sql, Func<DataRow, T> resultHandler,
        object parameter)
    {
        var dbParameters = patternSql<T>(sql, parameter);
        return ExecuteQuery(connection, sql, resultHandler, dbParameters);
    }

    /// <summary>
    /// 执行查询SQL语句,返回List
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public List<T> ExecuteQuery<T>(DbConnection connection, string sql, Func<DataRow, T> resultHandler,
        params DbParameter[] parameters)
    {
        return ExecuteQuery<List<T>>(connection, sql, dataTable =>
        {
            var list = new List<T>();
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add(resultHandler(row));
            }

            return list;
        }, parameters);
    }

    /// <summary>
    /// 执行查询SQL语句,返回List
    /// </summary>
    /// <typeparam name="T">返回结果类型</typeparam>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public List<T> ExecuteQuery<T>(string sql, Func<DataRow, T> resultHandler, params DbParameter[] parameters)
    {
        using var connection = GetConnection();
        return ExecuteQuery(connection, sql, resultHandler, parameters);
    }

    public List<T> ExecuteQuery<T>(string sql, Func<DataRow, T> resultHandler, object parameter)
    {
        using var connection = GetConnection();
        var dbParameters = patternSql<T>(sql, parameter);
        return ExecuteQuery(connection, sql, resultHandler, dbParameters);
    }

    /// <summary>
    /// 执行查询SQL语句,返回第一行第一列
    /// </summary>
    /// <typeparam name="T">数据类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public T ExecuteScalar<T>(DbConnection connection, string sql, Func<object, T> resultHandler,
        params DbParameter[] parameters)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddRange(parameters);
        PrintSqlAndParameters(command);
        return resultHandler(PrintResult(command.ExecuteScalar()));
    }


    /// <summary>
    /// 执行查询SQL语句,返回第一行第一列
    /// </summary>
    /// <typeparam name="T">数据类型</typeparam>
    /// <param name="sql">SQL语句</param>
    /// <param name="resultHandler">结果处理委托</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public T ExecuteScalar<T>(string sql, Func<object, T> resultHandler, params DbParameter[] parameters)
    {
        using var connection = GetConnection();
        return ExecuteScalar(connection, sql, resultHandler, parameters);
    }

    /// <summary>
    /// 执行查询SQL语句,返回第一行第一列
    /// </summary>
    /// <typeparam name="T">数据类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public T ExecuteScalar<T>(DbConnection connection, string sql,
        params DbParameter[] parameters)
    {
        return ExecuteScalar(connection, sql, obj => (T)obj, parameters);
    }

    /// <summary>
    /// 执行查询SQL语句,返回第一行第一列
    /// </summary>
    /// <typeparam name="T">数据类型</typeparam>
    /// <param name="sql">SQL语句</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>查询结果</returns>
    public T ExecuteScalar<T>(string sql, params DbParameter[] parameters)
    {
        using var connection = GetConnection();
        return ExecuteScalar<T>(connection, sql, parameters);
    }

    /// <summary>
    /// 执行事务
    /// </summary>
    /// <param name="queryHandler">
    /// 事务处理范围,在此范围内必须调用带有<see cref="DbConnection"/>参数的方法,如:
    /// <list type="list">
    /// <item><see cref="ExecuteNonQuery(DbConnection, string, DbParameter[])"/></item>
    /// <item><see cref="ExecuteQuery{T}(DbConnection, string, Func{DataTable, T}, DbParameter[])"/></item>
    /// <item><see cref="ExecuteScalar{T}(DbConnection, string, Func{object, T}, DbParameter[])"/></item>
    /// <item><see cref="ExecuteScalar{T}(DbConnection, string, DbParameter[])"/></item>
    /// <item><see cref="ExecuteQuery{T}(DbConnection, string, Func{DataRow, T}, DbParameter[])"/></item>
    /// </list>
    /// </param>
    public void ExecuteTransaction(Action<DbConnection, DbTransaction> queryHandler)
    {
        using var dbConnection = GetConnection();
        using var transaction = dbConnection.BeginTransaction();
        try
        {
            queryHandler(dbConnection, transaction);
            transaction.Commit();
        }
        catch
        {
            transaction.Rollback();
            throw;
        }
    }

    /// <summary>
    /// 将对象插入到数据库中,该方法需要满足以下条件才可使用:
    /// <list type="table">
    ///     <listheader>
    ///         <term>类别</term>
    ///         <description>内容</description>
    ///     </listheader>
    ///     <item>
    ///         <term>类名</term>
    ///         <description>
    ///             满足以下条件之一:
    ///             <list type="table">
    ///                 <item>
    ///                     <term>1</term>
    ///                     <description>类名与表名一致</description>
    ///                 </item>
    ///                 <item>
    ///                     <term>2</term>
    ///                     <description>类名进行下划线命名法转换后与表名一致,如: UserInfo(类名)=> user_info(表名)</description>
    ///                 </item>
    ///             </list>
    ///         </description>
    ///     </item>
    ///     <item>
    ///         <term>属性</term>
    ///         <description>
    ///             满足以下条件之一:
    ///             <list type="table">
    ///                 <item>
    ///                     <term>1</term>
    ///                     <description>属性名与字段名一致</description>
    ///                 </item>
    ///                 <item>
    ///                     <term>2</term>
    ///                     <description>属性名进行下划线命名法转换后与字段名一致,如: UserName(属性名)=> user_name(字段名)</description>
    ///                 </item>
    ///             </list>
    ///         </description>
    ///     </item>
    /// </list>
    /// </summary>
    /// <param name="obj">待插入对象</param>
    /// <returns></returns>
    public int InsertSelective(object obj)
    {
        using var connection = GetConnection();
        return InsertSelective(connection, obj);
    }

    /// <summary>
    /// 将对象插入到数据库中,该方法需要满足以下条件才可使用:
    /// <list type="table">
    ///     <listheader>
    ///         <term>类别</term>
    ///         <description>内容</description>
    ///     </listheader>
    ///     <item>
    ///         <term>类名</term>
    ///         <description>
    ///             满足以下条件之一:
    ///             <list type="table">
    ///                 <item>
    ///                     <term>1</term>
    ///                     <description>类名与表名一致</description>
    ///                 </item>
    ///                 <item>
    ///                     <term>2</term>
    ///                     <description>类名进行下划线命名法转换后与表名一致,如: UserInfo(类名)=> user_info(表名)</description>
    ///                 </item>
    ///             </list>
    ///         </description>
    ///     </item>
    ///     <item>
    ///         <term>属性</term>
    ///         <description>
    ///             满足以下条件之一:
    ///             <list type="table">
    ///                 <item>
    ///                     <term>1</term>
    ///                     <description>属性名与字段名一致</description>
    ///                 </item>
    ///                 <item>
    ///                     <term>2</term>
    ///                     <description>属性名进行下划线命名法转换后与字段名一致,如: UserName(属性名)=> user_name(字段名)</description>
    ///                 </item>
    ///             </list>
    ///         </description>
    ///     </item>
    /// </list>
    /// </summary>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="obj">待插入对象</param>
    /// <returns></returns>
    public int InsertSelective(DbConnection connection, object obj)
    {
        if (obj == null)
        {
            throw new NullReferenceException("需要插入的对象不能为null");
        }

        var type = obj.GetType();
        var propertyInfos = TypeHelper.GetProperties(type);
        var tb = SqlBuilder.CreateBuilder(ToUnderlineName(type.Name));
        foreach (var propertyInfo in propertyInfos)
        {
            var value = TypeHelper.GetPropertyValue<object>(obj, propertyInfo.Name);
            if (value != null)
            {
                tb.Insert(ToUnderlineName(propertyInfo.Name), value);
            }
        }

        var sql = tb.ToInsert(out var parameters,true);
        return ExecuteNonQuery(connection, sql, parameters);
    }

    /// <summary>
    /// 查询数据
    /// </summary>
    /// <typeparam name="T">返回的结果类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">sql语句</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>结果集合</returns>
    public List<T> ExecuteQuery<T>(DbConnection connection,string sql,params DbParameter[] parameters)
    {
        return ExecuteQuery(connection,sql, (DataTable table) =>
        {
            var list = new List<T>();
            foreach (DataRow row in table.Rows)
            {
                var item = Activator.CreateInstance<T>();
                var properties = TypeHelper.GetProperties(typeof(T),true);
                foreach (var property in properties)
                {
                    var columnName = ToUnderlineName(property.Name);
                    // 如果列名不存在,则跳过
                    if (!table.Columns.Contains(columnName))
                    {
                        continue;
                    }
                    if (!row.IsNull(columnName))
                    {
                        item.SetPropertyValue(property.Name, row[columnName]);
                    }
                }
                list.Add(item);
            }
            return list;
        },parameters);
    }
    /// <summary>
    /// 查询数据
    /// </summary>
    /// <typeparam name="T">返回的结果类型</typeparam>
    /// <param name="connection">数据库连接对象</param>
    /// <param name="sql">sql语句</param>
    /// <param name="parameter">参数对象,目前仅支持普通对象及字典</param>
    /// <returns>结果集合</returns>
    public List<T> ExecuteQuery<T>(DbConnection connection,string sql, object parameter)
    {
        var parameters = patternSql<T>(sql, parameter);
        return ExecuteQuery<T>(connection, sql, parameters);
    }

    /// <summary>
    /// 查询数据
    /// </summary>
    /// <typeparam name="T">返回的结果类型</typeparam>
    /// <param name="sql">sql语句</param>
    /// <param name="parameters">参数列表</param>
    /// <returns>结果集合</returns>
    public List<T> ExecuteQuery<T>(string sql, params DbParameter[] parameters)
    {
        using var connection = GetConnection();
        return ExecuteQuery<T>(connection, sql, parameters);
    }
    /// <summary>
    /// 查询数据
    /// </summary>
    /// <typeparam name="T">返回的结果类型</typeparam>
    /// <param name="sql">sql语句</param>
    /// <param name="parameter">参数对象,目前仅支持普通对象及字典</param>
    /// <returns>结果集合</returns>
    public List<T> ExecuteQuery<T>(string sql, object parameter)
    {
        using var connection = GetConnection();
        return ExecuteQuery<T>(connection, sql, parameter);
    }

    private string ToUnderlineName(string name)
    {
        var charArray = name.ToCharArray();
        var underlineName = $"{charArray[0]}";
        for (var i = 1; i < charArray.Length; i++)
        {
            if (char.IsUpper(charArray[i]))
            {
                underlineName += "_";
            }

            underlineName += charArray[i];
        }

        return underlineName.ToUpper();
    }
}