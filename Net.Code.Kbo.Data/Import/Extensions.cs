using Net.Code.ADONet;
using Net.Code.Csv;

namespace Net.Code.Kbo;

static class Extensions
{
    class TableInfo
    {
        public string name { get; set; } = null!;
    }

    extension(IDb db)
    {

        // the database contains no tables, or all tables are empty
        public bool IsEmpty
        {
            get 
            {
                const string sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';";
                var tables = db.Sql(sql).AsEnumerable<TableInfo>().ToList();
                foreach (var table in tables)
                {
                    var count = db.Sql($"SELECT COUNT(1) FROM {table.name};").AsScalar<int>();
                    if (count > 0) return false; // found a table with rows
                }
                return true; 
            }
        }

        public int? GetTableCount(string tableName)
        {
            const string sql = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name = @name;";
            var tables = db.Sql(sql).WithParameter("name", tableName).AsEnumerable<TableInfo>().ToList();
            if (tables.Count == 0) return null; // table does not exist
            return db.Sql($"SELECT COUNT(1) FROM {tableName};").AsScalar<int>();
        }

        public void DropAndRecreate(string tableName)
        {
            var create = db.Sql($"SELECT sql FROM sqlite_master WHERE type = 'table' AND name = '{tableName}';").AsScalar<string>();
            if (string.IsNullOrEmpty(create))
                throw new InvalidOperationException($"Table {tableName} not found in database");
            db.Sql($"DROP TABLE IF EXISTS {tableName}").AsNonQuery();
            db.Sql(create).AsNonQuery();
        }
    }
}