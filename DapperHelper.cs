using Dapper;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Linq;

namespace Cong.Utility
{
    public partial class DapperHelper
    {
        private static string connStr = ConfigurationManager.ConnectionStrings["connStr"].ConnectionString;

        public static int Execute(string sql, object parameters)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (IDbTransaction tran = conn.BeginTransaction())
                {
                    try
                    {
                        int count = conn.Execute(sql, parameters, tran, null, CommandType.Text);
                        tran.Commit();
                        return count;
                    }
                    catch (Exception)
                    {
                        tran.Rollback();
                        return 0;
                    }
                }
            }
        }

        public static int Execute<T>(string sql, T t)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (IDbTransaction tran = conn.BeginTransaction())
                {
                    try
                    {
                        int count = conn.Execute(sql, t, tran, null, CommandType.Text);
                        tran.Commit();
                        return count;
                    }
                    catch
                    {
                        tran.Rollback();
                        return 0;
                    }
                }
            }
        }

        public static int ExecuteSP(string sql, object parameters)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Execute(sql, parameters, null, null, CommandType.StoredProcedure);
            }
        }

        public static int ExecuteSP(string sql, DynamicParameters dp)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Execute(sql, dp, null, null, CommandType.StoredProcedure);
            }
        }

        public static object Scalar(string sql, object parameters)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Query<string>(sql, parameters, null, true, null, CommandType.Text).FirstOrDefault();
            }
        }

        public static object ScalarSP(string sql, object parameters)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Query<string>(sql, parameters, null, true, null, CommandType.StoredProcedure).FirstOrDefault();
            }
        }

        public static object ScalarSP(string sql, DynamicParameters dp)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Query<string>(sql, dp, null, true, null, CommandType.StoredProcedure).FirstOrDefault();
            }
        }

        public static T QueryOne<T>(string sql, object parameters)
        {
            return QueryMany<T>(sql, parameters).FirstOrDefault();
        }

        public static T QueryOneSP<T>(string sql, DynamicParameters dp)
        {
            return QueryManySP<T>(sql, dp).FirstOrDefault();
        }

        public static IEnumerable<T> QueryMany<T>(string sql, object parameters)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Query<T>(sql, parameters, null, true, null, CommandType.Text);
            }
        }

        public static IEnumerable<T> QueryManySP<T>(string sql, DynamicParameters dp)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Query<T>(sql, dp, null, true, null, CommandType.StoredProcedure);
            }
        }

        public static IEnumerable<T> QueryAll<T>(string sql)
        {
            return QueryMany<T>(sql, null);
        }

        public static IEnumerable<T> QueryAllSP<T>(string sql)
        {
            return QueryManySP<T>(sql, null);
        }

        public static IEnumerable<Master> QueryMaster<Master, Entity>(string sql, object parameters, string pfField)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                //Type t = typeof(Master);
                //Master lookup = (Master)t.Assembly.CreateInstance(t.FullName);
                Master lookup = default(Master);
                var list = conn.Query<Master, Entity, Master>(
                    sql,
                    (master, entity) =>
                    {
                        if (lookup == null || !lookup.GetType().GetProperty(pfField).GetValue(lookup, null).ToString().Equals(master.GetType().GetProperty(pfField).GetValue(master, null).ToString()))
                        {
                            lookup = master;
                        }
                        if (entity != null)
                        {
                            ((ICollection<Entity>)lookup.GetType().GetProperty(entity.GetType().Name).GetValue(lookup, null)).Add(entity);
                        }
                        return lookup;
                    },
                    parameters,
                    null,
                    true,
                    pfField,
                    null,
                    CommandType.Text);
                list = list.Distinct<Master>();
                return list;
            }
        }

        public static IEnumerable<Entity> QueryEntity<Entity, Master>(string sql, object parameters, string pfField)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                return conn.Query<Entity, Master, Entity>(
                    sql,
                    (entity, master) =>
                    {
                        entity.GetType().GetProperty(master.GetType().Name).SetValue(entity, master, null);
                        return entity;
                    },
                    parameters,
                    null,
                    true,
                    pfField,
                    null,
                    CommandType.Text);
            }
        }

        public static void QueryTwoEntity<T1, T2>(string sql, object parameters, ref IEnumerable<T1> list1, ref IEnumerable<T2> list2)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (var multi = conn.QueryMultiple(sql, parameters))
                {
                    list1 = multi.Read<T1>().ToList();
                    list2 = multi.Read<T2>().ToList();
                }
            }
        }

        public static void QueryThreeEntity<T1, T2, T3>(string sql, object parameters, ref IEnumerable<T1> list1, ref IEnumerable<T2> list2, ref IEnumerable<T3> list3)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (var multi = conn.QueryMultiple(sql, parameters))
                {
                    if (!multi.IsConsumed)
                    {
                        list1 = multi.Read<T1>().ToList();
                        list2 = multi.Read<T2>().ToList();
                        list3 = multi.Read<T3>().ToList();
                    }
                }
            }
        }

        public static void QueryTwoEntitySP<T1, T2>(string sql, object parameters, ref IEnumerable<T1> list1, ref IEnumerable<T2> list2)
        {
            using (IDbConnection conn = new SqlConnection(connStr))
            {
                conn.Open();
                using (var multi = conn.QueryMultiple(sql, parameters, null, 5000, CommandType.StoredProcedure))
                {
                    list1 = multi.Read<T1>().ToList();
                    list2 = multi.Read<T2>().ToList();
                }
            }
        }
    }
}
