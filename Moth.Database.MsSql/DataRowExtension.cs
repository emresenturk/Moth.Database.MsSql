using System.Collections.Generic;
using System.Data;
using Moth.Data;

namespace Moth.Database.MsSql
{
    internal static class DataRowExtension
    {
        public static Entity ToEntity(this DataRow row, DataColumnCollection columns)
        {
            var properties = new List<Property>();
            foreach (DataColumn column in columns)
            {
                var property = new Property(column.ColumnName, column.DataType, row[column]);
                properties.Add(property);
            }

            var entity = new Entity(properties.ToArray());
            return entity;
        }
    }
}