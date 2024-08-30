using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Helper;

/// <summary>
/// SQL构建器
/// </summary>
/// <example>
/// var tb = SqlBuilder.CreateBuilder("user_info");
/// // 指定要查询的列
/// // 方式1 =&gt; select id, username from user_info
/// //tb.Column().Add("id", "username");
/// // 方式2 =&gt; select id,username from user_info
/// //tb.Select("id", "username");
/// // 方式3 =&gt; select * from user_info
/// //tb.Select("*"); // 或不指定
/// // 指定查询条件
/// var whereBuild = tb.Where();
/// // 方式1
/// whereBuild.And("id", "=", 1);
/// // 方式2
/// string username = "admin";
/// // 仅当username不为空时才添加查询条件
/// whereBuild.And(!string.IsNullOrWhiteSpace(username),"username", "=", "admin");
/// // 方式3
/// var condition = whereBuild.And();
/// condition.And("password", "=", 1);
/// condition.Or("username", "like", "a%");
/// /// 指定排序
/// tb.OrderBy("id", OrderType.ASC);
/// // 指定分组
/// tb.GroupBy("id");
/// // 指定分组条件
/// var having = tb.Having();
/// having.And("id", "&gt;", 1);
/// // 生成查询SQL语句
/// "查询SQL语句".Println("===============", "===============");
/// tb.ToSelect(out var p).Println("SQL ");
/// p.ToList().ForEach(x =&gt; x.Println());
/// 
/// // 生成统计SQL语句
/// "统计SQL语句".Println("===============", "===============");
/// tb.ToCount(out var p2).Println("SQL ");
/// p2.ToList().ForEach(x =&gt; x.Println());
/// </example>
/// <example>
/// // 生成插入SQL语句
/// "插入SQL语句".Println("===============", "===============");
/// var tb2 = SqlBuilder.CreateBuilder("user_info");
/// tb2.Insert("id", 1);
/// tb2.Insert("username", "admin");
/// tb2.ToInsert(out var p3).Println("SQL ");
/// p3.ToList().ForEach(x =&gt; x.Println());
/// </example>
/// <example>
/// // 生成更新SQL语句
/// "更新SQL语句".Println("===============", "===============");
/// var tb3 = SqlBuilder.CreateBuilder("user_info");
/// tb3.Set("username", "admin");
/// tb3.Set("password", "123456");
/// tb3.Where().And("id", "=", 1);
/// tb3.ToUpdate(out var p4).Println("SQL ");
/// p4.ToList().ForEach(x =&gt; x.Println());
/// </example>
/// <example>
/// // 生成删除SQL语句
/// "删除SQL语句".Println("===============", "===============");
/// var tb4 = SqlBuilder.CreateBuilder("user_info");
/// tb4.Where().And("id", "=", 1);
/// tb4.ToDelete(out var p5).Println("SQL ");
/// Console.ReadKey();
/// </example>
public class SqlBuilder
{
    public static TableBuilder CreateBuilder(string table, Func<string, object, DbParameter> createParameter = null)
    {
        return new TableBuilder(table, createParameter);
    }
}

public class ColumnBuilder(TableBuilder tableBuilder)
{
    public List<string> Column = new();

    public TableBuilder TableBuilder { get; set; } = tableBuilder;

    public ColumnBuilder Add(params string[] columns)
    {
        if (columns.ToList().Contains("*"))
        {
            if (columns.Length > 1 || Column.Any())
            {
                Column.AddRange(columns);
                throw new ArgumentException("查询所有列时,不应该再指定其他列:" + string.Join(",", Column));
            }
        }

        Column.AddRange(columns);
        // 去重
        Column = Column.Distinct().ToList();
        return this;
    }

    public TableBuilder BuildEnd()
    {
        return TableBuilder;
    }
}

public class TableBuilder
{
    internal string Table { get; set; }

    internal List<JoinTable> Joins = new();

    internal ColumnBuilder ColumnBuild;

    internal List<Condition> Sets = new();

    internal List<Condition> Values = new();

    internal WhereBuild WhereBuild;

    internal List<OrderByInfo> OrderByInfos = new();

    internal WhereBuild HavingBuild;

    internal List<string> GroupByColumns = new();

    private Func<string, object, DbParameter> _createParameter;

    public TableBuilder(string table, Func<string, object, DbParameter> createParameter = null)
    {
        Table = table;
        WhereBuild = new WhereBuild(this);
        HavingBuild = new WhereBuild(this);
        ColumnBuild = new ColumnBuilder(this);
        _createParameter = createParameter;
    }

    /// <summary>
    /// 添加表连接
    /// </summary>
    /// <param name="table">从表名称</param>
    /// <param name="columnLeft">主表列名</param>
    /// <param name="columnRight">从表列名</param>
    /// <param name="joinType">连接类型</param>
    /// <returns></returns>
    public TableBuilder Join(string table, string columnLeft, string columnRight, JoinType joinType = JoinType.Inner)
    {
        Joins.Add(new JoinTable(Table, table, columnLeft, columnRight, joinType.ToString().ToUpper()));
        return this;
    }

    /// <summary>
    /// 添加表连接
    /// </summary>
    /// <param name="condition">是否进行表连接</param>
    /// <param name="table">从表名称</param>
    /// <param name="columnLeft">主表列名</param>
    /// <param name="columnRight">从表列名</param>
    /// <param name="joinType">连接类型</param>
    /// <returns></returns>
    public TableBuilder Join(bool condition, string table, string columnLeft, string columnRight,
        JoinType joinType = JoinType.Inner)
    {
        if (condition) Join(table, columnLeft, columnRight, joinType);
        return this;
    }


    /// <summary>
    /// 添加查询列
    /// </summary>
    /// <returns></returns>
    public ColumnBuilder Column()
    {
        return ColumnBuild;
    }

    /// <summary>
    /// 添加查询列
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public TableBuilder Select(params string[] columns)
    {
        ColumnBuild.Add(columns);
        return this;
    }

    /// <summary>
    /// 创建查询条件构建器
    /// </summary>
    /// <returns></returns>
    public WhereBuild Where()
    {
        return WhereBuild;
    }

    /// <summary>
    /// 创建查询条件构建器
    /// </summary>
    /// <returns></returns>
    public WhereBuild Having()
    {
        return HavingBuild;
    }

    /// <summary>
    /// 添加排序
    /// </summary>
    /// <param name="column"></param>
    /// <param name="orderType"></param>
    /// <returns></returns>
    public TableBuilder OrderBy(string column, OrderType orderType)
    {
        OrderByInfos.Add(new OrderByInfo { Column = column, OrderType = orderType });
        return this;
    }

    /// <summary>
    /// 添加排序
    /// </summary>
    /// <param name="condition">是否添加排序</param>
    /// <param name="column"></param>
    /// <param name="orderType"></param>
    /// <returns></returns>
    public TableBuilder OrderBy(bool condition, string column, OrderType orderType)
    {
        if (condition) OrderBy(column, orderType);
        return this;
    }

    /// <summary>
    /// 添加分组
    /// </summary>
    /// <param name="column"></param>
    /// <returns></returns>
    public TableBuilder GroupBy(string column)
    {
        GroupByColumns.Add(column);
        return this;
    }

    /// <summary>
    /// 添加分组
    /// </summary>
    /// <param name="condition">是否添加分组</param>
    /// <param name="column"></param>
    /// <returns></returns>
    public TableBuilder GroupBy(bool condition, string column)
    {
        if (condition) GroupBy(column);
        return this;
    }

    /// <summary>
    /// 添加设置
    /// </summary>
    /// <param name="column"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public TableBuilder Set(string column, object value)
    {
        Sets.Add(Condition.Create(column, "=", value));
        return this;
    }

    /// <summary>
    /// 添加设置
    /// </summary>
    /// <param name="condition">是否添加设置</param>
    /// <param name="column"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public TableBuilder Set(bool condition, string column, object value)
    {
        if (condition) Set(column, value);
        return this;
    }

    /// <summary>
    /// 添加新增
    /// </summary>
    /// <param name="column"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public TableBuilder Insert(string column, object value)
    {
        Values.Add(Condition.Create(column, "", value));
        return this;
    }

    /// <summary>
    /// 添加新增
    /// </summary>
    /// <param name="condition"></param>
    /// <param name="column"></param>
    /// <param name="value"></param>
    /// <returns></returns>
    public TableBuilder Insert(bool condition, string column, object value)
    {
        if (condition) Insert(column, value);
        return this;
    }

    /// <summary>
    /// 生成查询SQL
    /// </summary>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public string ToSelect()
    {
        var sql = ToSelect<Dictionary<string, object>>(out var p);
        if (p.Any())
        {
            throw new ArgumentException("查询语句不应该有参数,或者SQL中有参数需要接收");
        }

        return sql;
    }

    private T CreateParameters<T>(Dictionary<string, object> param)
    {
        T parameters;
        if (typeof(T) == typeof(Dictionary<string, object>))
        {
            parameters = (T)(object)param;
        }
        else if (typeof(T) == typeof(DbParameter[]))
        {
            if (_createParameter == null) throw new ArgumentException("未指定参数构建器");
            parameters = (T)(object)param.Select(x => _createParameter(x.Key, x.Value)).ToArray();
        }
        else if (typeof(T) == typeof(List<DbParameter>))
        {
            if (_createParameter == null) throw new ArgumentException("未指定参数构建器");
            parameters = (T)(object)param.Select(x => _createParameter(x.Key, x.Value)).ToList();
        }
        else
        {
            throw new ArgumentException("不支持的参数类型");
        }

        return parameters;
    }

    /// <summary>
    /// 生成查询SQL
    /// </summary>
    /// <param name="parameters"></param>
    /// <typeparam name="T"></typeparam>
    /// <returns></returns>
    public string ToSelect<T>(out T parameters)
    {
        var sql = "";
        var columns = string.Join(",", ColumnBuild.Column);
        // 如果没有指定列,则默认查询所有列
        if (string.IsNullOrEmpty(columns))
        {
            columns = "*";
        }

        sql += $"SELECT {columns} FROM {Table}";
        foreach (var join in Joins)
        {
            sql += $" {join}";
        }

        var param = new Dictionary<string, object>();
        // 检查是否有条件
        if (WhereBuild.Any())
        {
            sql += $" WHERE {WhereBuild.ToWhere(out var p)}";
            param.AddAll(p);
        }

        if (GroupByColumns.Any())
        {
            sql += $" GROUP BY {string.Join(",", GroupByColumns)}";
        }

        if (HavingBuild.Any())
        {
            sql += $" HAVING {HavingBuild.ToWhere(out var p2)}";
            param.AddAll(p2);
        }

        if (OrderByInfos.Any())
        {
            sql += $" ORDER BY {string.Join(",", OrderByInfos)}";
        }

        parameters = CreateParameters<T>(param);
        return sql;
    }

    /// <summary>
    /// 生成新增SQL
    /// </summary>
    /// <param name="parameters"></param>
    /// <param name="isPropertyName">生成的参数名称是否不带@前缀,默认为false</param>
    /// <returns></returns>
    public string ToInsert<T>(out T parameters, bool isPropertyName = false)
    {
        var sql =
            $"INSERT INTO {Table} ({string.Join(",", Values.Select(c => c.Column))}) VALUES ({string.Join(",", Values.Select(x => $"@{x.Column}"))})";
        var param = new Dictionary<string, object>();
        foreach (var t in Values)
        {
            param[$"{(isPropertyName ? "" : "@")}{t.Column}"] = t.Value;
        }

        parameters = CreateParameters<T>(param);
        return sql;
    }

    /// <summary>
    /// 生成更新SQL
    /// </summary>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public string ToUpdate<T>(out T parameters)
    {
        var param = new Dictionary<string, object>();
        var sql = $"UPDATE {Table} SET {string.Join(",", Sets.Select(x => {
            var sql = x.Build(out var p);
            param.AddAll(p);
            return sql;
        }))} WHERE {WhereBuild.ToWhere(out var p2)}";
        param.AddAll(p2);
        parameters = CreateParameters<T>(param);
        return sql;
    }

    /// <summary>
    /// 生成删除SQL
    /// </summary>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public string ToDelete<T>(out T parameters)
    {
        var sql = $"DELETE FROM {Table} WHERE {WhereBuild.ToWhere(out var p)}";
        parameters = CreateParameters<T>(p);
        return sql;
    }

    /// <summary>
    /// 生成统计SQL
    /// </summary>
    /// <param name="parameters"></param>
    /// <returns></returns>
    public string ToCount<T>(out T parameters)
    {
        var sql = $"SELECT COUNT(1) FROM {Table}";
        var param = new Dictionary<string, object>();
        if (WhereBuild.Any())
        {
            sql += $" WHERE {WhereBuild.ToWhere(out var p)}";
            param.AddAll(p);
        }

        parameters = CreateParameters<T>(param);
        return sql;
    }
}

public class OrderByInfo
{
    public string Column { get; set; }

    public string Table { get; set; }

    public OrderType OrderType { get; set; } = OrderType.ASC;

    public override string ToString()
    {
        var result = "";
        if (!string.IsNullOrEmpty(Table))
        {
            result += $"{Table}.";
        }

        result += $"{Column} {OrderType}";
        return result;
    }
}

public enum OrderType
{
    ASC,
    DESC
}

public class WhereBuild(TableBuilder tableBuilder)
{
    public List<Condition> Conditions = new();

    public TableBuilder TableBuilder { get; set; } = tableBuilder;

    /// <summary>
    /// 添加条件
    /// </summary>
    /// <param name="condition">条件验证,只有值为true时,条件才会真正添加到SQL中参与过滤</param>
    /// <param name="column">列名</param>
    /// <param name="operator">操作符</param>
    /// <param name="value">值</param>
    /// <returns></returns>
    public WhereBuild Add(bool condition, string column, string @operator, object value)
    {
        if (condition)
        {
            var item = Condition.Create(column, @operator, value);
            item.whereBuild = this;
            Conditions.Add(item);
        }

        return this;
    }

    /// <summary>
    /// 添加条件
    /// </summary>
    /// <param name="cond">条件验证,只有值为true时,条件才会真正添加到SQL中参与过滤</param>
    /// <param name="condition">条件对象</param>
    /// <returns></returns>
    public WhereBuild Add(bool cond, Condition condition)
    {
        if (cond)
        {
            condition.whereBuild = this;
            Conditions.Add(condition);
        }

        return this;
    }

    public string ToWhere(out Dictionary<string, object> parameters)
    {
        parameters = new();
        var sql = "";
        if (Conditions.Count > 0)
        {
            var param = new Dictionary<string, object>();
            sql += string.Join(" AND ", Conditions.Select(x =>
            {
                var build = x.Build(out var p);
                param.AddAll(p);
                return build;
            }));
            parameters.AddAll(param);
        }

        return sql;
    }

    public Condition And(bool cond, string column, string @operator, object value) =>
        And(cond, new Condition(column, @operator, value));

    public Condition And(string column, string @operator, object value) => And(true, column, @operator, value);

    public Condition And(bool cond, Condition condition)
    {
        Add(cond, condition);
        return condition;
    }

    public Condition And(Condition condition)
    {
        return And(true, condition);
    }

    public Condition And()
    {
        var and = new Condition(string.Empty, string.Empty, string.Empty);
        and.whereBuild = this;
        Conditions.Add(and);
        return and;
    }

    public Condition Or(bool cond, string column, string @operator, object value)
    {
        return Or(cond, new Condition(column, @operator, value));
    }

    public Condition Or(string column, string @operator, object value)
    {
        return Or(true, column, @operator, value);
    }

    public Condition Or(bool cond, Condition condition)
    {
        Add(cond, condition);
        return condition;
    }

    public Condition Or(Condition condition)
    {
        return Or(true, condition);
    }

    public Condition Or()
    {
        var or = new Condition(string.Empty, string.Empty, string.Empty);
        or.whereBuild = this;
        Conditions.Add(or);
        return or;
    }


    public TableBuilder BuildEnd()
    {
        return TableBuilder;
    }

    public bool Any()
    {
        return Conditions.Count > 0;
    }
}

public class JoinTable(string tableLeft, string tableRight, string columnLeft, string columnRight, string joinType)
{
    public string TableLeft { get; set; } = tableLeft;

    public string TableRight { get; set; } = tableRight;

    public string ColumnLeft { get; set; } = columnLeft;

    public string ColumnRight { get; set; } = columnRight;

    public string JoinType { get; set; } = joinType;

    public override string ToString()
    {
        var sql = $"{JoinType} JOIN {TableRight} ON ";
        if (!columnLeft.Contains("."))
        {
            sql += $"{TableLeft}.";
        }

        sql += $"{ColumnLeft} = ";
        if (!ColumnRight.Contains("."))
        {
            sql += $"{TableRight}.";
        }

        sql += $"{ColumnRight}";
        return sql;
    }
}

public enum JoinType
{
    Inner,
    Left,
    Right,
    Full
}

public class Condition
{
    internal WhereBuild whereBuild;

    public string Column { get; set; }

    public string Operator { get; set; }

    public object Value { get; set; }

    public string ParameterName { get; set; }

    public List<Condition> Ands { get; set; }

    public List<Condition> Ors { get; set; }

    public readonly Func<Dictionary<string, object>,string, string> BuildParameters;
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="column">数据库列名称</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="buildParameters">
    /// 参数处理器,如果需要自定义参数处理,则传入此参数
    /// <para>参数列表: 用于接收参数</para>
    /// <para>当前参数名称: 表示当前参与运算的参数名称,默认为@+数据库列名</para>
    /// <para>返回的SQL字符串: 表示运算符之后的SQL代码</para>
    /// <para>
    ///     Func&lt;参数列表, 当前参数名称, 返回的SQL字符串&gt;
    /// </para>
    ///
    /// </param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    public Condition(string column, string @operator, Func<Dictionary<string, object>, string, string> buildParameters, string parameterName = null)
    {
        Column = column;
        Operator = @operator;
        BuildParameters = buildParameters;
        ParameterName = string.IsNullOrWhiteSpace(parameterName) ? $"@{column}" : parameterName;
    }
    /// <summary>
    /// 返回条件构建器
    /// </summary>
    /// <returns></returns>
    public WhereBuild WhereBuild()
    {
        return whereBuild;
    }
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="column">数据库列名称</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    public Condition(string column, string @operator, object value,string parameterName=null)
    {
        Column = column;
        Operator = @operator;
        Value = value;
        ParameterName = string.IsNullOrWhiteSpace(parameterName) ? $"@{column}" : parameterName;
        BuildParameters = (para,paramName) =>
        {
            para.Add(paramName, Value);
            return paramName;
        };
    }
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="column">数据库列名称</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="and">`与`条件列表</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    public Condition(string column, string @operator, object value, List<Condition> and, string parameterName = null)
    {
        Column = column;
        Operator = @operator;
        Value = value;
        Ands = and;
        ParameterName = string.IsNullOrWhiteSpace(parameterName) ? $"@{column}" : parameterName;
        BuildParameters = (para, paramName) =>
        {
            para.Add(paramName, Value);
            return paramName;
        };
    }
    /// <summary>
    /// 构造函数
    /// </summary>
    /// <param name="column">数据库列名称</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="and">`与`条件列表</param>
    /// <param name="or">`或`条件列表</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    public Condition(string column, string @operator, object value, List<Condition> and, List<Condition> or, string parameterName = null)
    {
        Column = column;
        Operator = @operator;
        Value = value;
        Ands = and;
        Ors = or;
        ParameterName = string.IsNullOrWhiteSpace(parameterName) ? $"@{column}" : parameterName;
        BuildParameters = (para, paramName) =>
        {
            para.Add(paramName, Value);
            return paramName;
        };
    }
    /// <summary>
    /// 构建SQL并返回参数
    /// </summary>
    /// <param name="parameters">参数列表</param>
    /// <returns></returns>
    public string Build(out Dictionary<string, object> parameters)
    {
        var para = new Dictionary<string, object>();
        var result = "";
        if (!string.IsNullOrEmpty(Column))
        {
            result += $"{Column} {Operator} {BuildParameters?.Invoke(para,ParameterName)}";
        }

        if (Ands != null)
        {
            result += $" AND ({string.Join(" AND ", Ands.Select(x => {
                var sql = x.Build(out var param);
                para.AddAll(param);
                return sql;
            }))})";
        }

        if (Ors != null)
        {
            result += $" OR ({string.Join(" OR ", Ors.Select(x => {
                var sql = x.Build(out var param);
                para.AddAll(param);
                return sql;
            }))})";
        }

        parameters = para;
        // 移除多余的AND和OR
        result = result.Replace("AND ()", "").Replace("OR ()", "");
        // 移除多余的空格
        result = result.Replace("  ", " ");
        // 移除多余的括号
        result = result.Replace("()", "");
        // 移除多余的逗号
        result = result.Replace(",)", ")");
        // 移除多余的AND和OR
        result = result.Replace("AND AND", "AND").Replace("OR OR", "OR");
        // 移除开头的AND和OR
        result = result.TrimStart('A', 'N', 'D', 'O', 'R');
        return result;
    }
    /// <summary>
    /// 创建等于条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateEquals(string column,object value, string parameterName = null)
    {
        return Create(column, "=", value, parameterName);
    }
    /// <summary>
    /// 创建不等于条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateNotEquals(string column, object value, string parameterName = null)
    {
        return Create(column, "<>", value, parameterName);
    }
    /// <summary>
    /// 创建大于条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateGreaterThan(string column, object value, string parameterName = null)
    {
        return Create(column, ">", value, parameterName);
    }

    /// <summary>
    /// 创建大于等于条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateGreaterThanOrEquals(string column, object value, string parameterName = null)
    {
        return Create(column, ">=", value, parameterName);
    }
    /// <summary>
    /// 创建小于条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateLessThan(string column, object value, string parameterName = null)
    {
        return Create(column, "<", value, parameterName);
    }
    /// <summary>
    /// 创建小于等于条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateLessThanOrEquals(string column, object value, string parameterName = null)
    {
        return Create(column, "<=", value, parameterName);
    }

    /// <summary>
    /// 创建模糊匹配条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="joinOperator">字符串连接符</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateLike(string column, object value,string joinOperator="||", string parameterName = null)
    {
        return Create(column, "LIKE", (para,paraName) =>
        {
            para.Add(paraName, value);
            return $"'%'{joinOperator} {paraName} {joinOperator}'%'";
        },parameterName);
    }
    /// <summary>
    /// 创建前缀匹配条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="joinOperator">字符串连接符</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreatePrefix(string column, object value,string joinOperator="||", string parameterName = null)
    {
        return Create(column, "LIKE", (para,paraName) =>
        {
            para.Add(paraName, value);
            return $"{paraName} {joinOperator} '%'";
        },parameterName);
    }
    /// <summary>
    /// 创建后缀匹配条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="joinOperator">字符串连接符</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateSuffix(string column, object value,string joinOperator="||", string parameterName = null)
    {
        return Create(column, "LIKE", (para,paraName) =>
        {
            para.Add(paraName, value);
            return $"'%' {joinOperator} {paraName}";
        },parameterName);
    }




    /// <summary>
    /// 创建包含条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateIn(string column, object[] value, string parameterName = null)
    {
        return Create(column, "IN", (para, paraName) =>
        {
            for (var i = 0; i < value.Length; i++)
            {
                para.Add($"{paraName}_{i}", value[i]);
            }
            return $"({string.Join(",", value.Select((_, i) => $"{paraName}_{i}"))})";
        }, parameterName);
    }
    /// <summary>
    /// 创建不包含条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateNotIn(string column, object[] value, string parameterName = null)
    {
        return Create(column, "NOT IN", (para, paraName) =>
        {
            for (var i = 0; i < value.Length; i++)
            {
                para.Add($"{paraName}_{i}", value[i]);
            }
            return $"({string.Join(",", value.Select((_, i) => $"{paraName}_{i}"))})";
        }, parameterName);
    }
    /// <summary>
    /// 创建区间条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value1">值1</param>
    /// <param name="value2">值2</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateBetween(string column, object value1, object value2, string parameterName = null)
    {
        return Create(column, "BETWEEN", (para, paraName) =>
        {
            para.Add($"{paraName}_1", value1);
            para.Add($"{paraName}_2", value2);
            return $"{paraName}_1 AND {paraName}_2";
        }, parameterName);
    }
    /// <summary>
    /// 创建非区间条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="value1">值1</param>
    /// <param name="value2">值2</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition CreateNotBetween(string column, object value1, object value2, string parameterName = null)
    {
        return Create(column, "NOT BETWEEN", (para, paraName) =>
        {
            para.Add($"{paraName}_1", value1);
            para.Add($"{paraName}_2", value2);
            return $"{paraName}_1 AND {paraName}_2";
        }, parameterName);
    }
    /// <summary>
    /// 创建条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="parameterHandler">
    ///     参数处理器
    /// </param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition Create(string column, string @operator,
        Func<Dictionary<string, object>, string, string> parameterHandler, string parameterName = null)
    {
        return new Condition(column, @operator, parameterHandler, parameterName);
    }


    /// <summary>
    /// 创建条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition Create(string column, string @operator, object value, string parameterName = null)
    {
        return new Condition(column, @operator, value, parameterName);
    }
    /// <summary>
    /// 创建条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="and">`与`条件列表</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition Create(string column, string @operator, object value, List<Condition> and, string parameterName = null)
    {
        return new Condition(column, @operator, value, and, parameterName);
    }
    /// <summary>
    /// 创建条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="and">`与`条件列表</param>
    /// <param name="or">`或`条件列表</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public static Condition Create(string column, string @operator, object value, List<Condition> and,
        List<Condition> or, string parameterName = null)
    {
        return new Condition(column, @operator, value, and, or, parameterName);
    }
    /// <summary>
    /// 添加与条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public Condition And(string column, string @operator, object value, string parameterName = null)
    {
        var and = new Condition(column, @operator, value, parameterName);
        
        return And(and);
    }
    /// <summary>
    /// 添加与条件
    /// </summary>
    /// <param name="and">条件对象</param>
    /// <returns></returns>
    public Condition And(Condition and)
    {
        this.Ands ??= new List<Condition>();
        this.Ands.Add(and);
        return this;
    }
    /// <summary>
    /// 添加与条件
    /// </summary>
    /// <returns></returns>
    public Condition And()
    {
        var and = new Condition(string.Empty, string.Empty, string.Empty);
        this.Ands ??= new List<Condition>();
        this.Ands.Add(and);
        return and;
    }
    /// <summary>
    /// 添加或条件
    /// </summary>
    /// <param name="column">数据库列名</param>
    /// <param name="operator">关系运算符</param>
    /// <param name="value">值</param>
    /// <param name="parameterName">参数名称(如果为空,则参数名为数据库列名加@前缀,如: user_name =&gt; @user_name)</param>
    /// <returns></returns>
    public Condition Or(string column, string @operator, object value, string parameterName = null)
    {
        return Or(new Condition(column, @operator, value, parameterName));
    }
    /// <summary>
    /// 添加或条件
    /// </summary>
    /// <param name="or">条件对象</param>
    /// <returns></returns>
    public Condition Or(Condition or)
    {
        this.Ors ??= new List<Condition>();
        this.Ors.Add(or);
        return this;
    }
    /// <summary>
    /// 添加或条件
    /// </summary>
    /// <returns></returns>
    public Condition Or()
    {
        var or = new Condition(string.Empty, string.Empty, string.Empty);
        this.Ors ??= new List<Condition>();
        this.Ors.Add(or);
        return or;
    }
}