using System.Data.Common;
using DBHelpers;
using Even.Persistence.PostgreSql;
using Even.Persistence.Sql;

namespace Even.Tests.Persistence
{
#if POSTGRES
    public
#endif
    class PostgreSqlStoreTests : EventStoreTests
    {
        private string ConnectionString = "Server=localhost;Uid=postgres;Database=even_test;";

        protected override IEventStore CreateStore()
        {
            return new PostgreSqlStore(ConnectionString, true);
        }

        protected override void ResetStore()
        {
            var db = new DBHelper(DbProviderFactories.GetFactory("Npgsql"), ConnectionString);

            var store = (PostgreSqlStore)Store;

            var a = store.EventsTable;
            var b = store.ProjectionIndexTable;
            var c = store.ProjectionCheckpointTable;

            db.ExecuteNonQuery($"TRUNCATE TABLE \"{a}\"; TRUNCATE TABLE \"{b}\"; TRUNCATE TABLE \"{c}\";ALTER SEQUENCE	\"{a}_globalsequence_seq\" RESTART");
        }
    }
}
