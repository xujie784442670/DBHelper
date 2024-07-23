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
    public static TableBuilder CreateBuilder(string table)
    {
        return new TableBuilder(table);
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
                throw new ArgumentException("查询所有列时,不应该再指定其他列:"+ string.Join(",", Column));
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

    public TableBuilder(string table)
    {
        Table = table;
        WhereBuild = new WhereBuild(this);
        HavingBuild = new WhereBuild(this);
        ColumnBuild = new ColumnBuilder(this);
    }

    /// <summary>
    /// 添加表连接
    /// </summary>
    /// <param name="table">从表名称</param>
    /// <param name="columnLeft">主表列名</param>
    /// <param name="columnRight">从表列名</param>
    /// <param name="joinType">连接类型</param>
    /// <returns></returns>
    public TableBuilder Join(string table, string columnLeft, string columnRight, JoinType joinType=JoinType.Inner)
    {
        Joins.Add(new JoinTable(Table, table, columnLeft, columnRight, joinType.ToString().ToUpper()));
        return this;
    }

    public ColumnBuilder Column()
    {
        return ColumnBuild;
    }

    public TableBuilder Select(params string[] columns)
    {
        ColumnBuild.Add(columns);
        return this;
    }

    public WhereBuild Where()
    {
        return WhereBuild;
    }

    public WhereBuild Having()
    {
        return HavingBuild;
    }

    public TableBuilder OrderBy(string column, OrderType orderType)
    {
        OrderByInfos.Add(new OrderByInfo { Column = column, OrderType = orderType });
        return this;
    }

    public TableBuilder GroupBy(string column)
    {
        GroupByColumns.Add(column);
        return this;
    }

    public TableBuilder Set(string column, object value)
    {
        Sets.Add(Condition.Create(column, "=", value));
        return this;
    }

    public TableBuilder Insert(string column, object value)
    {
        Values.Add(Condition.Create(column,"",value));
        return this;
    }

    public string ToSelect()
    {
        var sql = ToSelect(out var p);
        if (p.Any())
        {
            throw new ArgumentException("查询语句不应该有参数,或者SQL中有参数需要接收");
        }
        return sql;
    }

    public string ToSelect(out Dictionary<string,object> parameters)
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
        parameters = new();
        // 检查是否有条件
        if (WhereBuild.Any())
        {
            sql += $" WHERE {WhereBuild.ToWhere(out var p)}";
            parameters.AddAll(p);
        }


        if (GroupByColumns.Any())
        {
            sql += $" GROUP BY {string.Join(",", GroupByColumns)}";
        }

        if (HavingBuild.Any())
        {
            sql += $" HAVING {HavingBuild.ToWhere(out var p2)}";
            parameters.AddAll(p2);
        }

        if (OrderByInfos.Any())
        {
            sql += $" ORDER BY {string.Join(",", OrderByInfos)}";
        }
        return sql;
    }
    /// <summary>
    /// 生成新增SQL
    /// </summary>
    /// <param name="parameters"></param>
    /// <param name="isPropertyName">生成的参数名称是否不带@前缀,默认为false</param>
    /// <returns></returns>
    public string ToInsert(out Dictionary<string,object> parameters,bool isPropertyName=false)
    {
        var sql = $"INSERT INTO {Table} ({string.Join(",", Values.Select(c=>c.Column))}) VALUES ({string.Join(",", Values.Select(x => $"@{x.Column}"))})";
        parameters = new();
        foreach (var t in Values)
        {
            parameters[$"{(isPropertyName?"": "@")}{t.Column}"] = t.Value;
        }
        return sql;
    }

    public string ToUpdate(out Dictionary<string,object> parameters)
    {
        var param = new Dictionary<string, object>();
        var sql = $"UPDATE {Table} SET {string.Join(",", Sets.Select(x => {
            var sql = x.Build(out var p);
            param.AddAll(p);
            return sql;
        }))} WHERE {WhereBuild.ToWhere(out var p2)}";
        parameters = new();
        parameters.AddAll(param);
        parameters.AddAll(p2);
        return sql;
    }

    public string ToDelete(out Dictionary<string,object> parameters)
    {
        var sql = $"DELETE FROM {Table} WHERE {WhereBuild.ToWhere(out var p)}";
        parameters = new();
        parameters.AddAll(p);
        return sql;
    }
    
    public string ToCount(out Dictionary<string,object> parameters)
    {
        var sql = $"SELECT COUNT(1) FROM {Table} WHERE {WhereBuild.ToWhere(out var p)}";
        parameters = new();
        parameters.AddAll(p);
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
            Conditions.Add(Condition.Create(column, @operator, value));
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
            Conditions.Add(condition);
        }

        return this;
    }

    public string ToWhere(out Dictionary<string,object> parameters)
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

    public Condition And(string column, string @operator, object value) => And(true,column, @operator, value);

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
        Conditions.Add(and);
        return and;
    }

    public Condition Or(bool cond, string column, string @operator, object value)
    {
        return Or(cond, new Condition(column, @operator, value));
    }

    public Condition Or(string column, string @operator, object value)
    {
        return Or(true,column, @operator, value);
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
    public string Column { get; set; }

    public string Operator { get; set; }

    public object Value { get; set; }

    public List<Condition> Ands { get; set; }

    public List<Condition> Ors { get; set; }

    public Condition(string column, string @operator, object value)
    {
        Column = column;
        Operator = @operator;
        Value = value;
    }

    public Condition(string column, string @operator, object value, List<Condition> and)
    {
        Column = column;
        Operator = @operator;
        Value = value;
        Ands = and;
    }

    public Condition(string column, string @operator, object value, List<Condition> and, List<Condition> or)
    {
        Column = column;
        Operator = @operator;
        Value = value;
        Ands = and;
        Ors = or;
    }

    public string Build(out Dictionary<string,object> parameters)
    {
        var para = new Dictionary<string, object>();
        var result = "";
        if (!string.IsNullOrEmpty(Column))
        {
            result += $"{Column} {Operator} @{Column}";
            para.Add($"@{Column}", Value);
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

    public static Condition Create(string column, string @operator, object value)
    {
        return new Condition(column, @operator, value);
    }

    public static Condition Create(string column, string @operator, object value, List<Condition> and)
    {
        return new Condition(column, @operator, value, and);
    }

    public static Condition Create(string column, string @operator, object value, List<Condition> and,
        List<Condition> or)
    {
        return new Condition(column, @operator, value, and, or);
    }

    public Condition And(string column, string @operator, object value)
    {
        var and = new Condition(column, @operator, value);
        this.Ands ??= new List<Condition>();
        this.Ands.Add(and);
        return this;
    }

    public Condition And(Condition and)
    {
        this.Ands ??= new List<Condition>();
        this.Ands.Add(and);
        return this;
    }

    public Condition And()
    {
        var and = new Condition(string.Empty, string.Empty, string.Empty);
        this.Ands ??= new List<Condition>();
        this.Ands.Add(and);
        return and;
    }

    public Condition Or(string column, string @operator, object value)
    {
        var or = new Condition(column, @operator, value);
        this.Ors ??= new List<Condition>();
        this.Ors.Add(or);
        return this;
    }

    public Condition Or(Condition or)
    {
        this.Ors ??= new List<Condition>();
        this.Ors.Add(or);
        return this;
    }

    public Condition Or()
    {
        var or = new Condition(string.Empty, string.Empty, string.Empty);
        this.Ors ??= new List<Condition>();
        this.Ors.Add(or);
        return or;
    }
}