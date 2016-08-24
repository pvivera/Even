using System;
using System.Data.Common;
using Even.Persistence.Sql;
using Npgsql;

namespace Even.Persistence.PostgreSql
{
    public class PostgreSqlStore : BaseSqlStore
    {
        public PostgreSqlStore(DbProviderFactory factory, string connectionString, bool createTables) : base(factory, connectionString, createTables)
        {
        }

        public PostgreSqlStore(string connectionString, bool createTables = false)
            : this(DbProviderFactories.GetFactory("Npgsql"), connectionString, createTables)
        { }

        protected override string EscapeIdentifier(string identifier)
        {
            return "\"" + identifier + "\"";
        }

        protected override string CommandText_CreateTables => @"
create table if not exists ""{0}"" (
  GlobalSequence bigserial not null primary key,
  EventID UUID not null,
  StreamHash bytea not null,
  StreamName varchar(200) not null,
  EventType varchar(50) not null,
  UtcTimestamp timestamp not null,
  Metadata bytea,
  Payload bytea not null,
  PayloadFormat int not null,
  CONSTRAINT event UNIQUE(EventID)
);

create table if not exists ""{1}"" (
  ProjectionStreamHash bytea not null,
  ProjectionStreamSequence int not null,
  GlobalSequence bigint not null,
  primary key (ProjectionStreamHash, ProjectionStreamSequence),
  unique (ProjectionStreamHash, GlobalSequence)
);

create table if not exists ""{2}"" (
  ProjectionStreamHash bytea not null primary key,
  LastGlobalSequence bigint not null
);
";
        protected override string CommandText_SelectGeneratedGlobalSequence => "SELECT LASTVAL()";
        protected override void HandleException(Exception ex)
        {
            var sqlEx = ex as NpgsqlException;

            if (sqlEx?.Code == "23505")
                throw new DuplicatedEntryException();
        }
    }
}