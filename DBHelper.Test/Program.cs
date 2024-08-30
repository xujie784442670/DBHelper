using Helper;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SQLite;
using System.Data.SqlTypes;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using DBHelper.Test.Properties;
using HmExtension.Commons.Extensions;

namespace DBHelper.Test
{
    internal class Program
    {
        private static Helper.DBHelper helper;
        static Program()
        {
            helper = new Helper.DBHelper(() =>
            {
                var builder = new SQLiteConnectionStringBuilder
                {
                    DataSource = "test.db"
                };
                return new SQLiteConnection(builder.ConnectionString);
            },(name,value)=>new SQLiteParameter(name,value));
            var rs = Sqls.ResourceManager.GetResourceSet(CultureInfo.CurrentCulture, true, true);
            if (rs == null) return;
            foreach (DictionaryEntry o in rs)
            {
                helper.ExecuteNonQuery(o.Value.ToString());
                o.Key.Println("正在尝试创建表:");
            }
        }

        static void Main(string[] args)
        {
            // var tb = SqlBuilder.CreateBuilder("user_info");
            // // 指定要查询的列
            // // 方式1 => select id, username from user_info
            // //tb.Column().Add("id", "username");
            // // 方式2 => select id,username from user_info
            // //tb.Select("id", "username");
            // // 方式3 => select * from user_info
            // //tb.Select("*"); // 或不指定
            // // 指定查询条件
            // var whereBuild = tb.Where();
            // // 方式1
            // whereBuild.And("id", "=", 1);
            // // 方式2
            // string username = "admin";
            // // 仅当username不为空时才添加查询条件
            // whereBuild.And(!string.IsNullOrWhiteSpace(username),"username", "=", "admin");
            // // 方式3
            // var condition = whereBuild.And();
            // condition.And("password", "=", 1);
            // condition.Or("username", "like", "a%");
            // /// 指定排序
            // tb.OrderBy("id", OrderType.ASC);
            // // 指定分组
            // tb.GroupBy("id");
            // // 指定分组条件
            // var having = tb.Having();
            // having.And("id", ">", 1);
            // // 生成查询SQL语句
            // "查询SQL语句".Println("===============", "===============");
            // tb.ToSelect(out var p).Println("SQL ");
            // p.ToList().ForEach(x => x.Println());
            //
            // // 生成统计SQL语句
            // "统计SQL语句".Println("===============", "===============");
            // tb.ToCount(out var p2).Println("SQL ");
            // p2.ToList().ForEach(x => x.Println());
            //
            // // 生成插入SQL语句
            // "插入SQL语句".Println("===============", "===============");
            // var tb2 = SqlBuilder.CreateBuilder("user_info");
            // tb2.Insert("id", 1);
            // tb2.Insert("username", "admin");
            // tb2.ToInsert(out var p3).Println("SQL ");
            // p3.ToList().ForEach(x => x.Println());
            //
            // // 生成更新SQL语句
            // "更新SQL语句".Println("===============", "===============");
            // var tb3 = SqlBuilder.CreateBuilder("user_info");
            // tb3.Set("username", "admin");
            // tb3.Set("password", "123456");
            // tb3.Where().And("id", "=", 1);
            // tb3.ToUpdate(out var p4).Println("SQL ");
            // p4.ToList().ForEach(x => x.Println());
            //
            // // 生成删除SQL语句
            // "删除SQL语句".Println("===============", "===============");
            // var tb4 = SqlBuilder.CreateBuilder("user_info");
            // tb4.Where().And("id", "=", 1);
            // tb4.ToDelete(out var p5).Println("SQL ");
            // and username = @username
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateEquals("username", "admin"))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Equals: ");
            // and username != @username
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateNotEquals("username", "admin"))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Not Equals: ");
            // and id > @id
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateGreaterThan("id", 1))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL GreaterThan: ");
            // and id >= @id
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateGreaterThanOrEquals("id", 1))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL GreaterThanOrEquals: ");
            // and id < @id
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateLessThan("id", 1))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL LessThan: ");
            // and id <= @id
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateLessThanOrEquals("id", 1))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL LessThanOrEquals: ");
            // and username like '%'||@username||'%'
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateLike("username", "a"))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Like: ");
            // and id in (@id_0,@id_1,@id_2)
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateIn("id", new object[] { 1, 2, 3 }))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL In: ");
            // and id not in (@id_0,@id_1,@id_2)
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateNotIn("id", new object[] { 1, 2, 3 }))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Not In: ");
            // and id between @id_1 and @id_2
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateBetween("id", 1, 3))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Between: ");
            // and username like @username||'%'
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreatePrefix("username","a"))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Prefix: ");
            // and username like '%'||@username
            SqlBuilder.CreateBuilder("user_info").Where()
                .And(Condition.CreateSuffix("username", "a"))
                .WhereBuild().BuildEnd().ToSelect<Dictionary<string, object>>(out _).Println("SQL Suffix: ");



            // 测试简化传参

            // var user = new UserInfo
            // {
            //     Username = "admin",
            //     Password = "123"
            // };
            // helper.IsPrintSql = true;
            // helper.IsPrintParameters = true;
            // helper.IsPrintResult = true;
            // // 传对象
            // "传对象".Println();
            // helper.ExecuteNonQuery("insert into user_info(username,password) values(@Username,@Password)", user);
            //
            // // 传字典
            // "传字典".Println();
            // var dict = new Dictionary<string, object>
            // {
            //     { "Username", "root" },
            //     { "Password", "root" }
            // };
            // helper.ExecuteNonQuery("insert into user_info(username,password) values(@Username,@Password)", dict);

            // 表连接测试
            // var tb = SqlBuilder.CreateBuilder("user_info");
            // tb.Join("order_info", "user_id", "id");
            // tb.Join("order_detail", "o.id", "order_id", JoinType.Left);
            // tb.ToSelect().Println("SQL: => ");

            // 新增对象测试
            // UserInfo user = new UserInfo
            // {
            //     Username = "admin",
            //     Password = "123"
            // };
            // helper.InsertSelective(user);
            /*
             * SELECT * FROM user_info u
             * user_info u INNER JOIN order_info o ON o.user_id = u.id
             * user_info u LEFT JOIN order_detail od ON od.order_id = o.id
             */
            Console.ReadKey();
        }
    }
}

public class UserInfo
{
    public int? Id { get; set; }

    public string Username { get; set; }

    public string Password { get; set; }


}