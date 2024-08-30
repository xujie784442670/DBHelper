# 数据库工具类
> 简化数据库访问,提供了数据库连接,关闭,查询,更新等操作.支持事务,并且提供了动态SQL工具

## 1. 创建数据库工具类
### 1.1 方式1
**这种方式创建的工具类,无法使用一下功能:**
> 1. DBHelper中的InsertSelective方法
> 2. SqlBuilder中的ToSelect<`DbParameter[]`>(out var parameres)方法
> 3. SqlBuilder中的ToInsert<`DbParameter[]`>(out var parameres)方法
> 4. SqlBuilder中的ToUpdate<`DbParameter[]`>(out var parameres)方法
> 5. SqlBuilder中的ToDelete<`DbParameter[]`>(out var parameres)方法
> 6. SqlBuilder中的ToCount<`DbParameter[]`>(out var parameres)方法

**但是可以使用SqlBuilder中以下方法:**
> 1. ToSelect<`Dictionary<string, object>`>(out var parameres)
> 2. ToInsert<`Dictionary<string, object>`>(out var parameres)
> 3. ToUpdate<`Dictionary<string, object>`>(out var parameres)
> 4. ToDelete<`Dictionary<string, object>`>(out var parameres)
> 5. ToCount<`Dictionary<string, object>`>(out var parameres)
```C#
var helper = new Helper.DBHelper(() =>
{
    var builder = new SQLiteConnectionStringBuilder
    {
        DataSource = "test.db"
    };
    return new SQLiteConnection(builder.ConnectionString);
});
```

### 1.2 方式2
> 这种方式创建的工具类,可以使用所有功能
```C#
var helper = new Helper.DBHelper(() =>
{
    var builder = new SQLiteConnectionStringBuilder
    {
        DataSource = "test.db"
    };
    return new SQLiteConnection(builder.ConnectionString);
},(name,value)=>new SQLiteParameter(name,value));
```
