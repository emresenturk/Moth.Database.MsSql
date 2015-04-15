using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using Moth.Configuration;
using Moth.Data;
using Moth.Expressions;

namespace Moth.Database.MsSql
{
    public class MsSqlDatabase : Database
    {
        private readonly SqlConnection connection;

        public MsSqlDatabase()
        {
            connection = new SqlConnection();
        }

        public MsSqlDatabase(IDatabaseConfiguration configuration)
            : base(configuration)
        {
            connection = new SqlConnection(configuration.ConnectionString);
            connection.Open();
        }

        public override Entity Create(Entity entity, TypeExpression entityType)
        {
            var tableName = entityType.Type.ToTableName();
            var columnNames = entity.PropertyNames.Where(p => p != "Id").Select(p => string.Format("{0}.[{1}]", tableName, p));
            var parameters = entity.PropertyNames.Where(p => p != "Id").Select((p, i) => new Parameter("@" + i, entity[p])).ToArray();
            var query = string.Format("INSERT INTO {0} ({1}) OUTPUT INSERTED.* VALUES({2})", tableName, string.Join(",", columnNames), string.Join(",", parameters.Select(p => p.Name)));
            using (var command = CreateCommand(query, parameters))
            {
                return Read(command).FirstOrDefault();
            }
        }

        public override Entity Update(Entity entity, TypeExpression entityType)
        {
            var tableName = entityType.Type.ToTableName();
            var columnNames = entity.PropertyNames.Where(p => p != "Id" && p != "UId").Select(p => string.Format("{0}.[{1}]", tableName, p));
            var parameters = entity.PropertyNames.Where(p => p != "Id" && p != "UId").Select((p, i) => new Parameter("@" + i, entity[p])).ToList();
            parameters.Add(new Parameter("@UId", entity["UId"]));
            var query = string.Format("UPDATE {0} SET {1} OUTPUT INSERTED.* WHERE {2}", tableName,
                string.Join(",", columnNames.Select((p, i) => p + "=@" + i)), string.Format("{0}.[UId]=@UId", tableName));
            using (var command = CreateCommand(query, parameters.ToArray()))
            {
                return Read(command).FirstOrDefault();
            }
        }

        public override Entity Delete(Entity entity, TypeExpression entityType)
        {
            var tableName = entityType.Type.ToTableName();
            var query = string.Format("DELETE FROM {0} OUTPUT DELETED.* WHERE {0}.[UId]=@UId", tableName);
            var parameters = new[] {new Parameter("@UId", entity["UId"])};
            using (var command = CreateCommand(query, parameters))
            {
                return Read(command).FirstOrDefault();
            }
        }

        protected override IList<Entity> RetrieveByText(Query query)
        {
            return ReadByText(query).ToList();
        }

        protected override int NonQueryByText(Query query)
        {
            using (var command = CreateCommand(query))
            {
                return command.ExecuteNonQuery();
            }
        }

        protected override IEnumerable<Entity> ReadByText(Query query)
        {
            using (var command = CreateCommand(query))
            {
                return Read(command);
            }
        }

        protected override IList<Entity> RetrieveByExpression(ExpressionQuery query)
        {
            return ReadByExpression(query).ToList();
        }

        protected override IEnumerable<Entity> ReadByExpression(ExpressionQuery query)
        {
            var queryString = query.ToSelectQuery();
            using (var command = CreateCommand(queryString, query.Parameters.Select(p =>
            {
                p.Name = "@" + p.Name.TrimStart('@');
                return p;
            }).ToArray()))
            {
                return Read(command);
            }
        }

        private SqlCommand CreateCommand(string query, params Parameter[] parameters)
        {
            var command = connection.CreateCommand();
            command.CommandType = CommandType.Text;
            command.CommandText = query;
            if (parameters != null)
            {
                foreach (var parameter in parameters)
                {
                    command.Parameters.AddWithValue(parameter.Name, parameter.Value ?? DBNull.Value);
                }
            }

            return command;
        }

        private SqlCommand CreateCommand(Query query)
        {
            return CreateCommand(query.Command, query.Parameters.ToArray());
        }

        private static IEnumerable<Entity> Read(SqlCommand command)
        {
            var reader = command.ExecuteReader();
            while (reader.Read())
            {
                var properties = new List<Property>();
                for (var i = 0; i < reader.VisibleFieldCount; i++)
                {
                    var fieldName = reader.GetName(i);
                    var fieldType = reader.GetFieldType(i);
                    var fieldValue = reader.IsDBNull(i) ? null : reader[i];
                    properties.Add(new Property(fieldName, fieldType, fieldValue));
                }

                yield return new Entity(properties.ToArray());
            }
        }

        public override void Dispose()
        {
            connection.Dispose();
            GC.SuppressFinalize(this);
        }
    }
}