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
            connection.Open();
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

    static class ExpressionQueryExtension
    {
        public static string ToSelectQuery(this ExpressionQuery query)
        {
            var from = query.SubQuery != null
                ? string.Format("FROM ({0}) AS F{1}", ((ExpressionQuery) query.SubQuery).ToSelectQuery(),
                    query.QueryIndex)
                : string.Format("FROM {0}", string.Join(", ", query.Types.Select(t => t.Type.ToTableName())));
            var where = query.Filters.Any()
                ? string.Format("WHERE {0}",
                    string.Join(" AND ", query.Filters.Select(e => string.Format("({0})", e.ToSqlStatement()))))
                : string.Empty;
            var orderBy = query.Sorts.Any()
                ? string.Format("ORDER BY {0}", string.Join(",", query.Sorts.Select(s => s.ToSqlStatement())))
                : null;

            var select = query.Projections.Any() 
                ? string.Format("SELECT ROW_NUMBER() OVER({2}) AS RowId{1}, {0}", string.Join(",", query.Projections.Select(p => p.ToSqlStatement())), query.QueryIndex, orderBy ?? "ORDER BY Id")
                : string.Format("SELECT ROW_NUMBER() OVER ({1}) AS RowId{0}, *", query.QueryIndex, orderBy ?? "ORDER BY Id");

            var sqlQuery = string.Format("{0} {1} {2} {3}", select, from, where, orderBy).Trim();
            if (query.Partitions.Any())
            {
                sqlQuery = string.Format("* FROM ({0}) AS P{1}", sqlQuery, query.QueryIndex);
                foreach (var partitionExpression in query.Partitions)
                {
                    var partitionMethod = partitionExpression as MethodExpression;
                    if (partitionMethod != null)
                    {
                        if (partitionMethod.Type == MethodType.First ||
                            partitionMethod.Type == MethodType.FirstOrDefault)
                        {
                            sqlQuery = string.Format("SELECT TOP 1 {0}", sqlQuery);
                        }
                        else if (partitionMethod.Type == MethodType.Last ||
                                 partitionMethod.Type == MethodType.LastOrDefault)
                        {
                            sqlQuery = string.Format("SELECT TOP 1 {0} ORDER BY RowId{1} DESC", sqlQuery,
                                query.QueryIndex);
                        }

                        else if (partitionMethod.Type == MethodType.Single ||
                                 partitionMethod.Type == MethodType.SingleOrDefault)
                        {
                            sqlQuery = string.Format("SELECT {0}", sqlQuery);
                        }

                        else if (partitionMethod.Type == MethodType.Take)
                        {
                            sqlQuery = string.Format("SELECT TOP {0} {1}", partitionMethod.Parameter.ToSqlStatement(),
                                sqlQuery);
                        }
                        else if (partitionMethod.Type == MethodType.Skip)
                        {
                            sqlQuery = string.Format("{0} WHERE RowId{1} > {2}", sqlQuery, query.QueryIndex,  partitionMethod.Parameter.ToSqlStatement());
                        }
                    }
                }
            }

            return sqlQuery;
        }

        private static string ToSqlStatement(this IQueryExpression expression)
        {
            var binaryExpression = expression as BinaryExpression;
            if (binaryExpression != null)
            {
                return binaryExpression.BinaryToSqlStatement();
            }
            var constantExpression = expression as ConstantExpression;
            if (constantExpression != null)
            {
                return constantExpression.ToSqlConstantString();
            }
            var parameterExpression = expression as ParameterExpression;
            if (parameterExpression != null)
            {
                return "@" + parameterExpression.Parameter.Name.TrimStart('@');
            }
            var memberExpression = expression as MemberExpression;
            if (memberExpression != null)
            {
                var orderExpression = memberExpression as OrderExpression;
                if (orderExpression != null)
                {
                    return string.Format("[{0}.{1}].[{2}] {3}", string.Join(".", orderExpression.Namespace), orderExpression.ObjectName, orderExpression.MemberName, orderExpression.Direction == SortDirection.Ascending ? "ASC" : "DESC");    
                }
                return string.Format("[{0}.{1}].[{2}]", string.Join(".", memberExpression.Namespace), memberExpression.ObjectName, memberExpression.MemberName);
            }
            var methodExpression = expression as MethodExpression;
            if (methodExpression != null)
            {
                throw new NotImplementedException();
                //return methodExpression.MethodToSqlStatement();
            }

            throw new ArgumentOutOfRangeException("expression", string.Format("Expression Type: {0}", expression.GetType()));
        }

        private static string BinaryToSqlStatement(this BinaryExpression binaryExpression)
        {
            var left = binaryExpression.Left.ToSqlStatement();
            var format = binaryExpression.Operator.ToSqlBinaryFormat();
            var right = binaryExpression.Right.ToSqlStatement();
            return string.Format(format, left, right);
        }

        public static string ToTableName(this Type type)
        {
            return string.Format("[{0}.{1}]", type.Namespace, type.Name);
        }

        private static string ToSqlBinaryFormat(this BinaryOperator @operator)
        {
            var operators = new Dictionary<BinaryOperator, string>
            {
                {BinaryOperator.Add, "({0} + {1})"},
                {BinaryOperator.Divide, "({0} / {1})"},
                {BinaryOperator.Subtract, "({0} - {1})"},
                {BinaryOperator.Multiply, "({0} * {1})"},
                {BinaryOperator.Power, "POWER({0}, {1})"},
                {BinaryOperator.Modulo, "({0} % {1})"},
                {BinaryOperator.And, "({0} & {1})"},
                {BinaryOperator.Or, "({0} & {1})"},
                {BinaryOperator.ExclusiveOr, "({0} ^ {1})"},
                {BinaryOperator.AndAlso, "({0} AND {1})"},
                {BinaryOperator.OrElse, "({0} OR {1})"},
                {BinaryOperator.Equal, "({0} = {1})"},
                {BinaryOperator.NotEqual, "({0} <> {1})"},
                {BinaryOperator.GreaterThan, "({0} > {1})"},
                {BinaryOperator.GreaterThanOrEqual, "({0} >= {1})"},
                {BinaryOperator.LessThan, "({0} < {1})"},
                {BinaryOperator.LessThanOrEqual, "({0} <= {1})"}
            };

            return operators[@operator];
        }

        private static string ToSqlConstantString(this ConstantExpression expression)
        {
            return TypeIsNumber(expression.ValueType) ? expression.Value.ToString() : string.Format("'{0}'", expression.Value);
        }

        private static bool TypeIsNumber(Type type)
        {
            switch (Type.GetTypeCode(type))
            {
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.UInt16:
                case TypeCode.UInt32:
                case TypeCode.UInt64:
                case TypeCode.Int16:
                case TypeCode.Int32:
                case TypeCode.Int64:
                case TypeCode.Decimal:
                case TypeCode.Double:
                case TypeCode.Single:
                    return true;
                default:
                    return false;
            }
        }
    }
}