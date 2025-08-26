using Net.Code.ADONet;

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
    }
}