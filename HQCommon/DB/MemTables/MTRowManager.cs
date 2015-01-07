using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Text;
using System.Threading;

namespace HQCommon.MemTables
{
    /// <summary> Interface for class/struct types that may be used with
    /// RowManager&lt;&gt;. Implementations must be named after a database
    /// table (e.g. HQCommon.MemTables.Stock or .StockRow for dbo.Stock)
    /// and must contain fields (not properties!) for each column of the
    /// corresponding database table, with names and types matching the
    /// columns of the table (e.g. for db column 'Mycol1 VARCHAR(1024)'
    /// the corresponding field is 'string Mycol1'; for 'myCol2
    /// smalldatetime NULL' the corresponding field is 'DateTime? myCol2').
    /// Furthermore, fields representing db columns should be annotated
    /// with [DbColumnAttribute], otherwise RowManager&lt;&gt; will consider
    /// them as different-purpose, non-database-column fields. Exception:
    /// if no fields have [DbColumnAttribute], then all public fields are
    /// considered. The 'Ordinal' property of [DbColumnAttribute] need NOT
    /// match the column order in the database. Its purpose is to identify
    /// struct/class members in IFieldOperation.Apply() calls.
    /// <para>
    /// Note: struct implementation is recommended only if the table will
    /// be read-only in the .NET program (like in case of MemTables).
    /// When insertions/deletions may occur, structs are not recommended.
    /// </para>
    /// When a type R implements this interface without IRowWithApply,
    /// RowManager&lt;R&gt; will attempt to implement the missing
    /// ApplyToAllFields() method using Reflection.Emit. The generated
    /// implementation calls IFieldOperation.Apply() for each of the
    /// above-mentioned db column representation fields in the order of
    /// DbColumnAttribute.Ordinal. </summary>
    public interface IRow
    {
        /// <summary> This property is assigned/used by RowManager&lt;&gt; operations </summary>
        DbOperationFlag RowState { get; set; }
    }

    /// <summary> Supplementary interface to opt out from Reflection.Emit </summary>
    public interface IRowWithApply : IRow
    {
        /// <summary> Calls p_op.Apply() for every field of <i>this</i> row.
        /// Called in the following situations:<para>
        /// - During loading, called on every (newly created) row instance.</para>
        /// - During saving, called on rows where (RowState &amp; ExistInDb)==0.
        /// </summary>
        void ApplyToAllFields(IFieldOperation p_op);
    }
    // it is public because used as return type of GetFieldOpApplicator()
    public delegate void FieldOperationApplicator<T>(IFieldOperation p_op, ref T p_tableRow);

    public interface IFieldOperation
    {
        void Apply<T>(int p_ordinal, ref T p_field);
    }

    public class RowBase : IRow
    {
        public DbOperationFlag RowState { get; set; }
    }

    public sealed class DbTableNameAttribute : Attribute
    {
        public string Name { get; private set; }
        public DbTableNameAttribute(string p_name) { Name = p_name; }
    }

    /// <summary> Flags in string: ?=nullable, r=REAL=Single, f=FLOAT=Double,
    /// I=identity, C=Computed, D=DATE, s=smalldatetime, 3=tinyint, 4=smallint,
    /// 5=int, 6=bigint </summary>
    /// <see cref="DbColFlag"/>
    public sealed class DbColumnAttribute : Attribute
    {
        public readonly ushort Ordinal;
        internal _Flag m_flags;

        /// <summary> This ctor is provided for convenience: specifying flags
        /// in a string is much more compact and lucid. </summary>
        public DbColumnAttribute(ushort p_ordinal, string p_flags)
        {
            Ordinal = p_ordinal;
            m_flags = default(_Flag);
            DbColFlag[] v = EnumUtils<DbColFlag>.Values;
            for (int i = (p_flags == null) ? 0 : p_flags.Length; --i >= 0; )
            {
                char ch = p_flags[i];
                int j = v.IndexOf(f => unchecked((char)f) == ch);
                if (j < 0)
                    throw new ArgumentException(p_flags, "p_flags");
                m_flags |= checked((_Flag)((int)v[j] >> 16));
            }
            int k = (int)(m_flags & _Flag._ExclusiveMask);
            if ((k & (k - 1)) != 0)     // multiple exclusive flags are present
                throw new ArgumentException(p_flags);
        }
        public DbColumnAttribute(ushort Ordinal, DbColFlag Flags = default(DbColFlag))
        {
            this.Ordinal = Ordinal;
            m_flags = checked((_Flag)((int)Flags >> 16));
            int f = (int)(m_flags & _Flag._ExclusiveMask);
            if ((f & (f - 1)) != 0)     // multiple exclusive flags are present
                throw new ArgumentException(m_flags.ToString());
        }
        public static string FlagsAsString(DbColFlag p_flags)
        {
            int flags = (int)p_flags >> 16;
            if (flags == 0)
                return String.Empty;
            var result = new StringBuilder();
            DbColFlag[] v = EnumUtils<DbColFlag>.Values;
            foreach (DbColFlag df in v) // in ascending order of numerical value
                if (df != DbColFlag._CharacterMask && unchecked((char)df) != 0
                    && ((int)flags & ((int)df >> 16)) == ((int)df >> 16))
                    result.Append(unchecked((char)df));
            return result.ToString();
        }
    }

    /// <summary> Provides additional information about the type of a column
    /// _in the database_. Generally the type of the .NET field must match
    /// the type of the database column, so that the .NET field type provides
    /// information about the database column type, too. However, even in this
    /// case further information may be necessary (e.g. is it an IDENTITY column?
    /// is it a computed column? etc).
    /// When we allow differences between the .NET field type and the database
    /// column type (see below) clearly we need additional information to know
    /// the database column type. This enum is used to specify the database column
    /// type in these cases (to be used with DbColumnAttribute).<para>
    /// Differences between .NET field type and the database column type are
    /// only allowed when: </para><para>
    /// - When the database column is FLOAT/REAL [NULL], the .NET type may be
    ///  either float[?]/double[?]/decimal[?]. The size of the .NET type may
    ///  differ from the size of the database type if DbColFlag.Single or
    ///  .Double is specified (DbColFlag.Single indicates that the database
    ///  type is REAL, DbColFlag.Double means FLOAT).
    ///  When the database column is nullable, the .NET type may be non-nullable
    ///  only if DbColFlag.Nullable is specified: in this case DBNull will be
    ///  translated to NaN (or decimal.MinValue) back and forth. </para><para>
    /// - When the type of the database column is a nullable datetime type
    ///  (DATE, SMALLDATETIME etc.) then the .NET type may be non-nullable
    ///  only if DbColFlag.Nullable is specified. In this case DBNull will be
    ///  translated to Utils.NO_DATE (back and forth). </para><para>
    /// - integer size differences are also allowed, e.g. database type INT
    ///  and .NET type sbyte (or sbyte-based enum) </para><para>
    /// In all other cases the size, type and nullability of the .NET type
    /// must match the type of the database column.</para><para>
    /// DbColFlag.Computed and .Identity are needed to omit the column when
    /// generating INSERT commands
    /// </para><para>
    /// DbColFlag.Date/Smalldatetime are optional: allow to shorten the string
    /// representation of datetime values when composing INSERT commands.</para>
    /// </summary>
    public enum DbColFlag : int
    {
        /// <summary> T-SQL: REAL </summary>
        Single          = 'r' + (_Flag.Single << 16),   // Please keep characters documented at DbColumnAttribute!
        /// <summary> T-SQL: FLOAT </summary>
        Double          = 'f' + (_Flag.Double << 16),
        Date            = 'D' + (_Flag.Date << 16),
        Smalldatetime   = 's' + (_Flag.Smalldatetime << 16),
        TinyInt         = '3' + (_Flag.TinyInt << 16),
        SmallInt        = '4' + (_Flag.SmallInt << 16),
        Int             = '5' + (_Flag.Int << 16),
        BigInt          = '6' + (_Flag.BigInt << 16),

        Identity        = 'I' + (_Flag.Identity << 16),
        Computed        = 'C' + (_Flag.Computed << 16),
        Nullable        = '?' + (_Flag.Nullable << 16),

        _CharacterMask  = 0xffff
    }

    /// <summary> Extended type info about a db column (for internal use) </summary>
    [Flags] enum _Flag : ushort
    {
        _ExclusiveMask  = 255,
        Single          = 1,
        Double          = 2,
        Date            = 4,
        Smalldatetime   = 8,
        TinyInt         = 16,
        SmallInt        = 32,
        Int             = 64,
        BigInt          = 128,

        Identity        = 256,
        Computed        = 512,
        Nullable        = 1024
    }

    [Flags]
    public enum DbOperationFlag : byte
    {
        /// <summary> If missing, SubmitChanges() will generate INSERT for this row </summary>
        ExistsInDb = 1,
        /// <summary> If present, SubmitChanges() will generate DELETE for this row -- NOT IMPLEMENTED YET </summary>
        ToDelete = 2
    }

    internal enum DefaultTimeout : int { Sec = 10 * 60 };

    public sealed class RowManager<T> : RowManager0<T> where T : IRow, new()
    {
        /// <summary> Contains retry logic </summary>
        public static IList<T> LoadAllRows(DBManager p_dbManager)
        {
            return DBManager.ExecuteWithRetry(p_dbManager, (dbManager,_) => LoadAllRows_noRetry(dbManager).ToArrayFast());
        }
        public static IEnumerable<T> LoadAllRows_noRetry(DBManager p_dbManager)
        {
            return LoadRows_noRetry(p_dbManager, (int)DefaultTimeout.Sec, null, null);
        }
    /*
        public static IEnumerable<T> LoadAllRows(DbDataReader p_dataReader)
        {
            var load = new RowManagerInternal.LoadOperation { m_columns = Rep.m_columns, m_src = p_dataReader };
            FieldOperationApplicator<T> apply = GetFieldOpApplicator();
            while (load.m_src.Read())
            {
                var result = new T { RowState = DbOperationFlag.ExistsInDb };
                apply(load, ref result);
                yield return result;
            }
        }
        public static IEnumerable<T> LoadAllRows(DataTable p_table)
        {
            return LoadAllRows(p_table.CreateDataReader());
        }
    */
        /// <summary> Contains retry logic </summary>
        public static IList<T> LoadRows(DBManager p_dbManager, string p_where, params object[] p_args)
        {
            return LoadRows(p_dbManager, (int)DefaultTimeout.Sec, p_where, p_args);
        }
        /// <summary> Contains retry logic </summary>
        public static IList<T> LoadRows(DBManager p_dbManager, int p_timeoutSec, string p_where, params object[] p_args)
        { 
            return DBManager.ExecuteWithRetry(0, (_,__) => LoadRows_noRetry(p_dbManager, p_timeoutSec, p_where, p_args).ToArrayFast());
        }
        /// <summary> Calls of this method need to be protected with DBManager.ExecuteWithRetry() or similar technique </summary>
        public static IEnumerable<T> LoadRows_noRetry(DBManager p_dbManager, int p_timeoutSec, string p_where, params object[] p_args)
        {
            if (p_timeoutSec == 0)
                p_timeoutSec = (int)DefaultTimeout.Sec;
            if (p_args != null && 0 < p_args.Length)
                p_where = String.Format(System.Globalization.CultureInfo.InvariantCulture, p_where, (object[])p_args);
            var sb = new StringBuilder("SELECT "); sb.Append(StrColumnList);
            sb.Append(" FROM ["); sb.Append(TableName); sb.Append("] ");
            if (p_where != null)
                sb.Append(p_where);
            var load = new RowManagerInternal.LoadOperation { m_columns = Rep.m_columns };
            FieldOperationApplicator<T> apply = GetFieldOpApplicator();
            using (ThreadManager.Singleton.RetardApplicationExit(null, _ => typeof(RowManager<T>).ToString()))
            {
                foreach (DbDataReader row in p_dbManager.ExtremeQuery(sb.ToString(), p_timeoutSec))
                {
                    load.m_src = row;
                    var result = new T { RowState = DbOperationFlag.ExistsInDb };
                    apply(load, ref result);
                    yield return result;
                    if (ApplicationState.IsOtherThreadExiting)  // do not stop if the current thread is performing the exit
                        break;
                }
            }
        }

        public static void SubmitChanges(DBManager p_dbManager, IEnumerable<T> p_rows, int p_timeoutSec = (int)DefaultTimeout.Sec)
        {
            do
            {
                InsertRows(p_dbManager, p_rows, p_timeoutSec);
            } while (false);
            // TODO: generate DELETE command as well
        }

        protected override IEnumerable<T> LoadAllRows_inst(DBManager p_dbManager)       { return LoadAllRows(p_dbManager); }
        protected override void AddFlagToRowState(ref T p_val, DbOperationFlag p_flag)  { p_val.RowState |=  p_flag; }
        protected override bool IsIRowWithDefaultCtor()                                 { return true; }
        protected override T CreateT()                                                  { return new T(); }
        protected override IEnumerable<KeyValuePair<T, int>> FilterRows(IEnumerable<T> p_src, DbOperationFlag p_mask, DbOperationFlag p_value)
        {
            int i = -1;
            foreach (T t in p_src.EmptyIfNull())
                if (0 <= ++i && (t.RowState & p_mask) == p_value)
                    yield return new KeyValuePair<T, int>(t, i);
        }
    }

    // Class for that (considerable) portion of code that does not require T : IRow, new()
    public class RowManager0<T>
    {
        static internal RowManagerInternal g_rep;  // internal [rep]resentation
        static internal RowManagerInternal Rep
        {
            get { return g_rep ?? (g_rep = new RowManagerInternal(typeof(T))); }
        }

        public static string TableName                  { get { return Rep.m_tableName; } }
        public static string StrColumnList              { get { return Rep.StrColumnList; } }
        public static string StrColumnListForInsert     { get { return Rep.StrColumnListForInsert; } }
        public static int    NumberOfColumns            { get { return Rep.m_columns.Length; } }
        public static IList<KeyValuePair<string, FieldInfo>> ColumnList
        {
            get { return Rep.m_columns.SelectList((_, c) => new KeyValuePair<string, FieldInfo>(c.ColName, c.m_reflectionInfo)); }
        }

        /// <summary> Does *not* filter p_rows for IRow.RowState != ExistsInDb, instead,
        /// incorporates values from all rows into the resulting string. If p_returnIdentity
        /// is true and T has an identity column, the returned INSERT statement will contain
        /// an OUTPUT INSERTED.* clause (makes the server return the generated identity
        /// values together with all inserted info), to help identify the row the new ID
        /// belongs to.
        /// The function returns string[3]: [0] is the INSERT command, [1] is the name of
        /// the SqlParameter referenced by the INSERT command (comprised of a fixed prefix
        /// + p_paramNamePostfix), and [2] is the value of the SqlParameter. </summary>
        // Reasons for employing SqlParameter:
        // - to avoid SQL injection
        // - to assist reuse of execution plans
        // - to allow for overcoming the 2GB limit of a single string :)
        // p_paramNamePostfix allows concatenating multiple INSERT commands into a single transaction.
        public static string[] ComposeInsertCommand(IEnumerable<T> p_rows,
            bool p_insertIdentity, bool p_returnIdentity, string p_paramNamePostfix = null)
        {
            return Rep.ComposeInsertCommand(EnumerateColumnValues(p_rows, false),
                p_insertIdentity, p_returnIdentity, p_paramNamePostfix);
        }

        public static string ToString(T p_row)      { return ToString(ref p_row); }
        public static string ToString(ref T p_row)
        {
            return Rep.ToString(EnumerateColumnValues(Utils.Single(p_row), true));
        }

        /// <summary> Returns NumberOfColumns items for every item of p_seq </summary>
        public static IEnumerable<object> EnumerateColumnValues(IEnumerable<T> p_seq, bool p_forToString)
        {
            var save = new RowManagerInternal.SaveOperation {
                m_columns = Rep.m_columns,
                m_buffer  = new object[Rep.m_columns.Length],
                m_forToString = p_forToString
            };
            FieldOperationApplicator<T> apply = GetFieldOpApplicator();
            foreach (T t in p_seq.EmptyIfNull())
            {
                T copy = t;
                apply(save, ref copy);
                foreach (object o in save.m_buffer)
                    yield return o;
            }
        }

        public static DbColumnAttribute GetDbColumAttribute<TField>(System.Linq.Expressions.Expression<Func<T, TField>> p_fieldSelector)
        {
            MemberInfo m = ((System.Linq.Expressions.MemberExpression)p_fieldSelector.Body).Member;
            return m.GetCustomAttributes(typeof(DbColumnAttribute), true).Cast<DbColumnAttribute>().SingleOrDefault();
        }

        public static FieldOperationApplicator<T> GetFieldOpApplicator()
        {
            if (g_fieldOpApp4RowWithApply == null)
            {
                if (typeof(IRowWithApply).IsAssignableFrom(typeof(T)))
                    g_fieldOpApp4RowWithApply = RowManagerInternal.GetFieldOpApp4RowWithApply<T>();
                else lock (Rep)
                    if (g_fieldOpApp4RowWithApply == null)
                    {
                        Delegate result = Rep.GenerateApplicator_locked(typeof(FieldOperationApplicator<T>));
                        Thread.MemoryBarrier();
                        g_fieldOpApp4RowWithApply = (FieldOperationApplicator<T>)result;
                    }
            }
            return g_fieldOpApp4RowWithApply;
        }
        static FieldOperationApplicator<T> g_fieldOpApp4RowWithApply;

        static RowManager0<T> Singleton
        {
            get
            {
                if (g_singleton == null)
                {
                    RowManager0<T> x = new RowManager0<T>();
                    if (typeof(IRow).IsAssignableFrom(typeof(T)))
                        try
                        {
                            Func<object> template = Creation_template<MemTables.Currency>;
                            x = (RowManager0<T>)Utils.ReplaceGenericParameters(template, out template, typeof(T))();
                        }
                        catch { }   // occurs when T:new() isn't satisfied.
                    g_singleton = x;
                }
                return g_singleton;
            }
        }
        static RowManager0<T> g_singleton;
        static object Creation_template<TRow>() where TRow : MemTables.IRow, new()      { return new RowManager<TRow>(); }
        protected virtual IEnumerable<T> LoadAllRows_inst(DBManager p_dbManager)        { throw new NotSupportedException(); }
        protected virtual void AddFlagToRowState(ref T p_val, DbOperationFlag p_flag)   { }
        static            void AddFlagToRowState2(ref T p_val, DbOperationFlag p_flag)  { ((IRow)p_val).RowState |= p_flag; }   // only used when T is ref.type
        protected virtual bool IsIRowWithDefaultCtor()                                  { return false; }
        protected virtual T CreateT()
        {
            return typeof(T).IsValueType ? default(T) : Activator.CreateInstance<T>();
        }

        /// <summary> The returned delegate incorporates retry logic. Returns null if cannot load T (is not IRow). </summary>
        public static Func<DBManager, IEnumerable<T>> MakeLoaderForAllRows()
        {
            return Singleton.IsIRowWithDefaultCtor() ? Singleton.LoadAllRows_inst : (Func<DBManager, IEnumerable<T>>)null;
        }

        /// <summary> When T implement IRow, the function skips those items of p_rows[]
        /// that contain the ExistsInDb flag in their RowState property. </summary>
        public static void InsertRows(DBManager p_dbManager, IEnumerable<T> p_rows, int p_timeoutSec = (int)DefaultTimeout.Sec)
        {
            RowManager0<T> S = Singleton;
            IEnumerable<KeyValuePair<T, int>> ftmp = S.FilterRows(p_rows, DbOperationFlag.ExistsInDb, 0);
            var fieldsToUpdate = new BitVector();
            IEnumerable<T> filtered; int[] indices = null; Lld lld = null;
            IList<T> rowsList = p_rows as IList<T>;
            int finishing = (typeof(T).IsValueType) && (rowsList == null) ? 0 : 1;   // 1: updatable, 0: not updatable
            if (finishing == 0)                                 // finishing==0: none
                filtered = ftmp.GetKeys();
            else if (0 <= Rep.m_firstIdentity)
            {
                finishing = 2;                                  // finishing==2: update identity field + RowState
                for (int i = Rep.m_columns.Length; --i >= 0; )
                    if ((Rep.m_columns[i].m_flags & _Flag.Identity) != 0)   // TODO: add here other types of generated fields (timestamp/GUID/etc.)
                        fieldsToUpdate.SetBitGrow(i);
                if (rowsList != null)
                    lld = new Lld(ftmp, rowsList, fieldsToUpdate);
                else
                {
                    rowsList = ftmp.GetKeys().ToArrayFast();            // T is reference type
                    lld = new Lld(rowsList.Select((t,i) => new KeyValuePair<T, int>(t, i)), rowsList, fieldsToUpdate);
                }
                filtered = lld.Keys;
            }
            else if (!typeof(IRow).IsAssignableFrom(typeof(T)))
            {
                finishing = 0;
                filtered = ftmp.GetKeys();
            }
            else if (rowsList == null)                          // finishing==1: update IRow.RowState (no identity field)
            {
                filtered = ftmp.GetKeys();                              // T is reference type
                Utils.ProduceOnce(ref filtered);
            }
            else                                                        // T is either value type or ref.type
            {
                indices = ftmp.Select(kv => kv.Value).ToArrayFast();
                filtered = Utils.SelectList(indices, (_, i) => rowsList[i]);
            }
            ftmp = null;
            if (Utils.TryGetCount(filtered) == 0)
                return;
            string[] cmd = ComposeInsertCommand(filtered, p_insertIdentity: false, p_returnIdentity: finishing > 1);
            if (String.IsNullOrEmpty(cmd[2]))
                return;
            using (ThreadManager.Singleton.RetardApplicationExit(S))
            {
                object result = p_dbManager.ExecuteSqlCommand(DBType.Remote, cmd[0], CommandType.Text,
                    new DbParameter[] { DBUtils.String2SqlParameter<System.Data.SqlClient.SqlParameter>(cmd[1], cmd[2]) },
                    finishing > 1 ? SqlCommandReturn.Table : SqlCommandReturn.None, p_timeoutSec);
                switch (finishing)
                {
                    case 2 :
                        // Remember that for INSERT...OUTPUT INSERTED.*  "There is no guarantee that the order
                        // in which the changes are applied to the table and the order in which the rows are
                        // inserted into the output table or table variable will correspond." http://j.mp/VHifpt
                        // This is why we have to complicate things with Lld
                        FieldOperationApplicator<T> apply = GetFieldOpApplicator();
                        var load = new RowManagerInternal.LoadOperation { 
                            m_columns = Rep.m_columns,
                            m_src =  ((DataTable)result).CreateDataReader()
                        };
                        for (T t = S.CreateT(); load.m_src.Read(); )
                            lld.Update(ref t, apply, load);
                        break;
                    case 1 : // no identity column in T
                        var addFlag = S.IsIRowWithDefaultCtor() ? S.AddFlagToRowState : (dAddFlagToRowState)AddFlagToRowState2;
                        if (indices != null)
                            foreach (int i in indices)
                            {
                                T copy = rowsList[i]; addFlag(ref copy, DbOperationFlag.ExistsInDb);
                                rowsList[i] = copy;
                            }
                        else
                            foreach (T t in filtered)
                            {
                                T copy = t; addFlag(ref copy, DbOperationFlag.ExistsInDb);
                            }
                        break;
                    default :
                        break;
                }
            }
        }
        private delegate void dAddFlagToRowState(ref T p_val, DbOperationFlag p_flag);

        protected virtual IEnumerable<KeyValuePair<T, int>> FilterRows(IEnumerable<T> p_src, DbOperationFlag p_mask, DbOperationFlag p_value)
        {
            var list = p_src as IList<T>;
            return (list == null) ? p_src.Select((t,i) => new KeyValuePair<T, int>(t, i))
                        : Utils.SelectList(list, (i,t) => new KeyValuePair<T, int>(t, i));
        }
        class Lld : ListLookupDictionary<T, int>
        {
            IList<T> m_orig;
            readonly FieldsEqualityComparer<T> m_eq;
            readonly bool m_isValType;
            BitVector m_hasBeenUpdated;
            public override int  GetHashCode(T p_key)           { return m_eq.GetHashCode(p_key); }
            public override bool KeyEquals(T p_key1, T p_key2)  { return m_eq.Equals(p_key1, p_key2); }
            public override T    GetKey(int p_value)            { return m_orig[p_value]; }
            public Lld(IEnumerable<KeyValuePair<T,int>> p_filtered, IList<T> p_orig, BitVector p_colsToIgnore)
            {
                m_eq = new FieldsEqualityComparer<T>(p_colsToIgnore);
                m_orig = p_orig;
                m_isValType = typeof(T).IsValueType;
                Utils.StrongAssert(m_orig != null);
                var tmp = new QuicklyClearableList<int>().EnsureCapacity(m_orig.Count);
                foreach (var kv in p_filtered)
                    tmp.Add(kv.Value);
                if (0 < tmp.Count)
                    RebuildDataStructure(tmp.TrimExcess(), tmp.m_count);    // duplicate keys are allowed
            }
            public bool Update(ref T p_tmp, FieldOperationApplicator<T> p_apply, IFieldOperation p_load)
            {
                int idxHere, inOrig; bool b;
                p_apply(p_load, ref p_tmp);
                using (IEnumerator<KeyValuePair<int, int>> it = GetValues(p_tmp).GetEnumerator())
                {                                               // sequence of >1 items if duplicates are present
                    if (!it.MoveNext())
                        return false;                           // 0 items: p_item is not found at all
                    do
                    {
                        idxHere = it.Current.Key; inOrig = it.Current.Value; b = it.MoveNext();
                    } while (b && m_hasBeenUpdated[idxHere]);   // keep m_hasBeenUpdated[] empty if there're no duplicate items
                }
                if (b)
                    m_hasBeenUpdated.SetBitGrow(idxHere);
                T orig = m_orig[inOrig];
                p_apply(p_load, ref orig);                      // see explanation at note#130208
                Singleton.AddFlagToRowState(ref orig, DbOperationFlag.ExistsInDb);
                if (m_isValType)
                    m_orig[inOrig] = orig;
                return true;
            }
        }
    }

    [DebuggerDisplay("{m_rowType.Name,nq} (RowManagerInternal)")]
    internal class RowManagerInternal
    {
        internal enum TypeInfo : byte
        {
            None = 0,
            SqlBool,
            SqlByte,
            SqlDateTime០NETDateOnly,
            SqlDateTime០NETDateTimeAsInt,
            SqlDateTime,
            SqlDecimal,
            SqlDouble,
            SqlSingle,
            SqlSingle០NETDouble,
            SqlInt16,
            SqlInt32,
            SqlInt64,
            SqlString
        }
        internal sealed class LoadOperation : IFieldOperation
        {
            internal DbDataReader m_src;
            internal ColumnInfo[] m_columns;
            public void Apply<T>(int col, ref T p_dst)
            {
                int isNull = 0;
                if (((m_columns[col].m_flags & _Flag.Nullable) != 0) && m_src.IsDBNull(col))
                {
                    if (m_columns[col].m_useDefaultTforNull)
                    {
                        p_dst = default(T);
                        return;
                    }
                    isNull = -1;
                }
                object c = m_columns[col].m_loadConversion;
                switch (m_columns[col].m_typeInfo)
                {
                    case TypeInfo.SqlBool :
                        p_dst = ((Conversion<bool, T>)c).ThrowOnNull(m_src.GetBoolean(col));
                        return;
                    case TypeInfo.SqlByte :
                        p_dst = ((Conversion<byte, T>)c).ThrowOnNull(m_src.GetByte(col));
                        return;
                    case TypeInfo.SqlDateTime០NETDateOnly :
                        p_dst = ((Conversion<DateOnly, T>)c).ThrowOnNull(GetDateTimeOrNO_DATE(isNull | col));
                        return;
                    case TypeInfo.SqlDateTime០NETDateTimeAsInt :
                        p_dst = ((Conversion<DateTimeAsInt, T>)c).ThrowOnNull(GetDateTimeOrNO_DATE(isNull | col));
                        return;
                    case TypeInfo.SqlDateTime :
                        p_dst = ((Conversion<DateTime, T>)c).ThrowOnNull(GetDateTimeOrNO_DATE(isNull | col));
                        return;
                    case TypeInfo.SqlDecimal :
                        p_dst = ((Conversion<decimal, T>)c).ThrowOnNull(isNull != 0 ? decimal.MinValue : m_src.GetDecimal(col));
                        return;
                    case TypeInfo.SqlDouble :
                        p_dst = ((Conversion<double, T>)c).ThrowOnNull(isNull != 0 ? double.NaN : m_src.GetDouble(col));
                        return;
                    case TypeInfo.SqlSingle :
                        p_dst = ((Conversion<float, T>)c).ThrowOnNull(isNull != 0 ? float.NaN : m_src.GetFloat(col));
                        return;
                    case TypeInfo.SqlSingle០NETDouble :
                        if (isNull != 0)
                            p_dst = ((Conversion<double, T>)c).ThrowOnNull(double.NaN);
                        else    // convert to double through 'decimal', to improve accuracy
                        {
                            const float MIN = (float)Decimal.MinValue, MAX = (float)Decimal.MaxValue;
                            float f = m_src.GetFloat(col);      // this is never NaN because MSSQL does not support NaNs
                            double d = (MIN < f && f < MAX) ? (double)(decimal)f : (double)f;
                            p_dst = ((Conversion<double, T>)c).ThrowOnNull(d);
                        }
                        return;
                    case TypeInfo.SqlInt16 :
                        p_dst = ((Conversion<short, T>)c).ThrowOnNull(m_src.GetInt16(col));
                        return;
                    case TypeInfo.SqlInt32 :
                        p_dst = ((Conversion<int, T>)c).ThrowOnNull(m_src.GetInt32(col));
                        return;
                    case TypeInfo.SqlInt64 :
                        p_dst = ((Conversion<long, T>)c).ThrowOnNull(m_src.GetInt64(col));
                        return;
                    case TypeInfo.SqlString :
                        p_dst = ((Conversion<string, T>)c).DefaultOnNull(m_src.GetString(col));
                        return;
                    default :
                        throw new InvalidOperationException(String.Format(
                            "m_typeInfo=={0} - not supported field type {1} of {2}",
                            m_columns[col].m_typeInfo, m_columns[col].m_reflectionInfo.FieldType,
                            m_columns[col].ColName));
                }
            }
            DateTime GetDateTimeOrNO_DATE(int col)  { return (col < 0) ? Utils.NO_DATE : m_src.GetDateTime(col); }
        }
        internal sealed class SaveOperation : IFieldOperation
        {
            internal object[] m_buffer;
            internal ColumnInfo[] m_columns;
            internal bool m_forToString;    // what is the purpose of this operation: true=ToString() false=write to db
                                            // in the latter case enums has to be translated to numbers, DateOnly->DateTime etc.
            public void Apply<T>(int p_ordinal, ref T p_field)
            {
                if (p_field == null)
                {
                    m_buffer[p_ordinal] = null;
                    return;
                }
                bool isNull = false;
                // Translate NaN/NO_DATE to NULL
                if ((m_columns[p_ordinal].m_flags & _Flag.Nullable) != 0    // == database column is nullable
                    && !m_columns[p_ordinal].m_useDefaultTforNull)          // == T is not reference type and not nullable
                {                                                           // (database NULL must be represented by .NET null when T is ref.type or nullable)
                    switch (Type.GetTypeCode(typeof(T)))
                    {
                        case TypeCode.Single:
                            // "(float)p_field" is compiler error CS0030: Cannot convert type 'T' to 'float'
                            // __refvalue/__makeref are used to implement a non-boxing conversion T->float
                            isNull = float.IsNaN(__refvalue(__makeref(p_field), float));
                            break;
                        case TypeCode.Double:
                            isNull = double.IsNaN(__refvalue(__makeref(p_field), double));
                            break;
                        case TypeCode.Decimal:
                            isNull = decimal.MinValue.Equals(__refvalue(__makeref(p_field), decimal));
                            break;
                        case TypeCode.DateTime:
                            isNull = (Utils.NO_DATE == __refvalue( __makeref(p_field), DateTime));
                            break;
                        case TypeCode.Object:
                            isNull = (typeof(T) == typeof(DateOnly) && DateOnly.NO_DATE.Equals( __refvalue(__makeref(p_field), DateOnly)))
                                  || (typeof(T) == typeof(DateTimeAsInt) && DateTimeAsInt.NO_DATE.Equals( __refvalue(__makeref(p_field), DateTimeAsInt)));
                            break;
                        default:
                            break;
                    }
                }
                object c;
                if (isNull)
                    c = null;
                else if (m_forToString)
                    c = p_field;
                else
                {
                    c = m_columns[p_ordinal].m_saveConversion;
                    switch (m_columns[p_ordinal].m_typeInfo)
                    {
                        // Now T (the .NET-type) may be nullable, but p_field!=null/NaN/NO_DATE:
                        case TypeInfo.SqlBool:      case TypeInfo.SqlString:
                        case TypeInfo.SqlDouble:    case TypeInfo.SqlDecimal:
                        case TypeInfo.SqlSingle:    case TypeInfo.SqlSingle០NETDouble: c = p_field; break;
                        case TypeInfo.SqlDateTime០NETDateOnly:
                        case TypeInfo.SqlDateTime០NETDateTimeAsInt:
                        case TypeInfo.SqlDateTime:  c = ((Conversion<T, DateTime>)c).ThrowOnNull(p_field); break;
                        case TypeInfo.SqlByte:      c = ((Conversion<T, byte>    )c).ThrowOnNull(p_field); break;
                        case TypeInfo.SqlInt16:     c = ((Conversion<T, short>   )c).ThrowOnNull(p_field); break;
                        case TypeInfo.SqlInt32:     c = ((Conversion<T, int>     )c).ThrowOnNull(p_field); break;
                        case TypeInfo.SqlInt64:     c = ((Conversion<T, long>    )c).ThrowOnNull(p_field); break;
                        default: throw new InvalidOperationException(String.Format(
                            "m_typeInfo=={0} - not supported field type {1} of {2}",
                            m_columns[p_ordinal].m_typeInfo, m_columns[p_ordinal].m_reflectionInfo.FieldType,
                            m_columns[p_ordinal].ColName));
                    }
                }
                m_buffer[p_ordinal] = c;
            }
        }

        internal struct ColumnInfo
        {
            const int EstimatedSize64 = 40; // x64 (8+1+1+1+8+8+8[+5]) [with alignment]

            internal System.Reflection.FieldInfo m_reflectionInfo;
            internal _Flag m_flags;
            /// <summary> true if field type is nullable or a reference type </summary>
            internal bool m_useDefaultTforNull;
            internal TypeInfo m_typeInfo;
            internal object m_loadConversion, m_saveConversion;
            string m_colName;
            internal string ColName { get { return m_colName ?? (m_colName = m_reflectionInfo.Name); } }
        }
        internal Type m_rowType;
        internal ColumnInfo[] m_columns;
        internal string m_tableName;
        internal int m_firstIdentity;    // index of the first identity column, -1 if none
        //internal Delegate m_generatedApplicator;

        public string StrColumnList
        {
            get { return '[' + String.Join("],[", m_columns.Select(col => col.ColName)) + ']'; }
        }
        public string StrColumnListForInsert
        {
            get
            {
                return '[' + String.Join("],[", m_columns
                            .Where(col => (col.m_flags & (_Flag.Identity | _Flag.Computed)) == 0)
                            .Select(col => col.ColName)) + ']';
            }
        }

        internal RowManagerInternal(Type p_rowType)
        {
            m_rowType = p_rowType;
            m_tableName = m_rowType.Name;
            var ta = Utils.GetAttribute<DbTableNameAttribute>(m_rowType);
            if (ta != null)
                m_tableName = ta.Name;
            else if (m_tableName.EndsWith("Row"))   // "StockRow" => "Stock"
                m_tableName = m_tableName.Substring(0, m_tableName.Length - 3);
            FieldInfo[] fields = m_rowType.GetFields(BindingFlags.Public |
                BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.FlattenHierarchy);
            var attrs = new DbColumnAttribute[fields.Length];
            var cols = new RowManagerInternal.ColumnInfo[fields.Length];
            int n = 0;
            for (int i = fields.Length; --i >= 0; n += (attrs[i] == null ? 0 : 1))
                attrs[i] = Utils.GetAttribute<DbColumnAttribute>(fields[i]);
            bool noFieldHasAttribute = (n == 0);
            n = 0;
            for (int i = 0; i < fields.Length; ++i)
                if (attrs[i] != null || (noFieldHasAttribute 
                    // include private and omit compiler-generated fields (e.g. *k__BackingField), to facilitate FieldsEqualityComparer
                    && !fields[i].IsDefined(typeof(System.Runtime.CompilerServices.CompilerGeneratedAttribute))
                    && !fields[i].IsDefined(typeof(System.Xml.Serialization.XmlIgnoreAttribute))))
                {
                    int j = -1;
                    ColumnInfo colInfo;
                    DetectColumnInfo(fields[i], attrs[i] ?? (object)n++, out j, out colInfo);
                    if (cols.Length <= j)
                        Array.Resize(ref cols, j + 1);
                    cols[j] = colInfo;
                }
            for (n = cols.Length - 1; n >= 0 && cols[n].m_reflectionInfo == null; )
                --n;
            if (++n < cols.Length)
                Array.Resize(ref cols, n);
            m_firstIdentity = -1;
            for (int i = 0; i < cols.Length; ++i)
            {
                if (cols[i].m_reflectionInfo == null)
                    throw Error(Utils.FormatInvCult("{1} values are not continuous: no *field* with {1}={0}", i,
                        (new Func<System.Linq.Expressions.Expression<Func<DbColumnAttribute, ushort>>>(() => (_ => _.Ordinal))().Body
                            as System.Linq.Expressions.MemberExpression).Member.GetQualifiedMemberName(true)));
                if (m_firstIdentity < 0 && (cols[i].m_flags & _Flag.Identity) != 0)
                    m_firstIdentity = i;
            }
            m_columns = cols;
        }

        void DetectColumnInfo(FieldInfo f, object p_attrOrSuggestedOrdinal, out int p_ordinal, out ColumnInfo p_colInfo)
        {
            var info = new ColumnInfo { m_reflectionInfo = f };
            var attr = p_attrOrSuggestedOrdinal as DbColumnAttribute;
            p_ordinal = (attr == null) ? (int)p_attrOrSuggestedOrdinal : attr.Ordinal;
            Utils.StrongAssert(p_ordinal >= 0);
            if (attr != null)
                info.m_flags = attr.m_flags;

            Type ft = f.FieldType, u = null;
            info.m_useDefaultTforNull = (!ft.IsValueType || (u = Nullable.GetUnderlyingType(ft)) != null);
            if (u != null)
                info.m_flags |= _Flag.Nullable;

            var typeInfo = TypeInfo.None;
            TypeCode tc = Type.GetTypeCode(u ?? ft);
            switch (tc)
            {
                case TypeCode.SByte :   case TypeCode.Byte   : typeInfo = TypeInfo.SqlByte;   u = typeof(byte);   break;
                case TypeCode.Int16 :   case TypeCode.UInt16 : typeInfo = TypeInfo.SqlInt16;  u = typeof(short);  break;
                case TypeCode.Int32 :   case TypeCode.UInt32 : typeInfo = TypeInfo.SqlInt32;  u = typeof(int);    break;
                case TypeCode.Int64 :   case TypeCode.UInt64 : typeInfo = TypeInfo.SqlInt64;  u = typeof(long);   break;
                case TypeCode.Char  :   case TypeCode.String : typeInfo = TypeInfo.SqlString; u = typeof(string); break;
                case TypeCode.Boolean:  typeInfo = TypeInfo.SqlBool;     u = typeof(bool);     break;
                case TypeCode.Single:   typeInfo = TypeInfo.SqlSingle;   u = typeof(float);    break;
                case TypeCode.Double:   typeInfo = TypeInfo.SqlDouble;   u = typeof(double);   break;
                case TypeCode.Decimal:  typeInfo = TypeInfo.SqlDecimal;  u = typeof(decimal);  break;
                case TypeCode.DateTime: typeInfo = TypeInfo.SqlDateTime; u = typeof(DateTime); break;
                case TypeCode.Object:   // e.g. DateOnly, DateTimeAsInt
                    if (typeof(DateOnly) == (u ?? ft))
                    {
                        typeInfo = TypeInfo.SqlDateTime០NETDateOnly; u = typeof(DateOnly);
                        tc = TypeCode.Empty;    // we use it to indicate that 'ft' is DateOnly[?] or DateTimeAsInt[?]
                    }
                    else if (typeof(DateTimeAsInt) == (u ?? ft))
                    {
                        typeInfo = TypeInfo.SqlDateTime០NETDateTimeAsInt; u = typeof(DateTimeAsInt);
                        tc = TypeCode.Empty;
                    }
                    else
                        goto default;
                    break;
                case TypeCode.DBNull:   case TypeCode.Empty :
                default:
                    //throw Error("not supported field type: " + ft, info.ColName); -- not suitable for FieldsEqualityComparer
                    tc = TypeCode.Object;
                    break;
            }
            switch (info.m_flags & _Flag._ExclusiveMask)    // info about the sql type
            {
                // 'tc' is typeCode of the .NET type, TypeCode.Empty in case of DateOnly/DateTimeAsInt
                // 'typeInfo' specifies what DbDataReader.Get*() function to use (when loading)
                //                   or how to box the value before ToString() (when saving)
                // 'u' is the CLR type corresponding to 'typeInfo', u==null for reference types
                // 'ft' specifies the type of the .NET field (destination when loading, src when saving)

                case (_Flag)0:      // no info about sql type: assume it's identical to .NET field type
                    if (typeInfo == TypeInfo.SqlDateTime០NETDateOnly)
                        info.m_flags |= _Flag.Date;
                    else if (typeInfo == TypeInfo.SqlDateTime០NETDateTimeAsInt)
                        info.m_flags |= _Flag.Smalldatetime;
                    break;

                case _Flag.Single:  // sql type is "REAL" (=Single)
                    if (tc == TypeCode.Empty)
                        throw Error("REAL->" + ft + " conversion is not supported", info.ColName);
                    // When loading into 'double', improve accuracy by involving 'decimal' (special 'SingleToDouble' mode):
                    typeInfo = (tc == TypeCode.Double) ? TypeInfo.SqlSingle០NETDouble : TypeInfo.SqlSingle;
                    u = (typeInfo == TypeInfo.SqlSingle០NETDouble) ? typeof(double) : typeof(float);
                    break;

                case _Flag.Double : // sql type is "FLOAT" (=Double)
                    if (tc == TypeCode.Empty)
                        throw Error("FLOAT->" + ft + " conversion is not supported", info.ColName);
                    typeInfo = TypeInfo.SqlDouble; u = typeof(double);
                    break;

                case _Flag.TinyInt :    typeInfo = TypeInfo.SqlByte;    u = typeof(byte);   break;
                case _Flag.SmallInt :   typeInfo = TypeInfo.SqlInt16;   u = typeof(short);  break;
                case _Flag.Int :        typeInfo = TypeInfo.SqlInt32;   u = typeof(int);    break;
                case _Flag.BigInt :     typeInfo = TypeInfo.SqlInt64;   u = typeof(long);   break;

                case _Flag.Date :
                case _Flag.Smalldatetime :
                    if (tc != TypeCode.Empty)
                    {
                        // Conversion to numeric types will fail -- no warn here
                        typeInfo = TypeInfo.SqlDateTime; u = typeof(DateTime);
                    }
                    break;
                default:
                    throw new NotImplementedException(info.m_flags.ToString());
            }
            if (u != null)
            {
                info.m_loadConversion = Utils.CreateConversion(u, ft);
                info.m_saveConversion = Utils.CreateConversion(ft, tc == TypeCode.Empty ? typeof(DateTime) : u);
            }
            info.m_typeInfo = typeInfo;
            p_colInfo = info;
        }
        internal InvalidOperationException Error(string p_msg, string p_fieldName = null)
        {
            return new InvalidOperationException(p_msg + " at " + m_rowType + (p_fieldName == null ? null : "." + p_fieldName));
        }

        /// <summary> For simplicity, p_values[] must contain values for all columns, including
        /// identity and computed column(s), too.
        /// Values of identity column(s) will be ignored unless p_insertIdentity==true. 
        /// The function returns string[3]: [0] is the INSERT command, [1] is the name of
        /// the SqlParameter referenced by the INSERT command (comprised of a fixed prefix
        /// + p_paramNamePostfix), and [2] is the value of the SqlParameter. </summary>
        /// </summary>
        internal string[] ComposeInsertCommand(System.Collections.IEnumerable p_values, bool p_insertIdentity,
            bool p_returnIdentity, string p_paramNamePostfix)
        {
            // This function composes 3 strings, similar to these:
            //
            // result[0]:
            //   INSERT INTO tableName (col1,..,col<n>) OUTPUT INSERTED.*
            //     SELECT S.[0], NULLIF(S.[1],''), ... S.[<n-1>]
            //     FROM (SELECT (SeqNr / <n>) AS R, (SeqNr % <n>) AS M, Item
            //           FROM dbo.SplitStringToTable(@p_insertData##, ':')
            //     ) P PIVOT (MIN(Item) FOR M IN ([0],[1],...,[<n-1>])) AS S
            // result[1]:
            //   @p_insertData##
            // result[2]:
            //   val11:..:val1n:val21:..:val2n:...
            //
            // instead of "*" the separator SEP is used in SplitStringToTable() and result[2]
            const char SEP = (char)20;  // DC4, "Device Control 4" control character, within the 0..127 range
                                        // This separator allows newline/tab/,/'/"/etc in items
            // Note: SplitStringToTable() is used because it minimizes script size and produces 
            // a single INSERT statement, thus no transactioning is needed to support retries

            _Flag skip = _Flag.Computed | (p_insertIdentity ? 0 : _Flag.Identity);
            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            string comma = String.Empty, pivotCols = comma;
            var fmt = new _Flag[m_columns.Length];
            var sb = new StringBuilder();
            if (p_insertIdentity && 0 <= m_firstIdentity)
                sb.AppendFormat("SET IDENTITY_INSERT [{0}] ON;{1}", m_tableName, Environment.NewLine);
            sb.Append("INSERT INTO ["); sb.Append(m_tableName); sb.Append("] (");
            int n = m_columns.Length, nColsOut = 0, j = sb.Length;
            sb.Append(")");
            if (p_returnIdentity && 0 <= m_firstIdentity)
            {
                // Return all columns ('OUTPUT INSERTED.*') to help identifying which ID belongs to which row
                // (because no ordering is guaranteed)
                sb.Append(" OUTPUT INSERTED.*");
            }
            sb.Append(" SELECT ");
            for (int i = 0; i < n; ++i)
            {
                _Flag flags = m_columns[i].m_flags;
                if ((flags & skip) != 0)
                    continue;
                int before = sb.Length;
                sb.Insert(j, comma + '[' + m_columns[i].ColName + ']');
                j += sb.Length - before;
                bool isNullable = (flags & _Flag.Nullable) != 0;
                sb.AppendFormat(invCult, isNullable ? "{0}NULLIF(S.[{1}],'')" : "{0}S.[{1}]", comma, nColsOut);
                pivotCols += comma + '[' + nColsOut.ToString(invCult) + ']';
                switch (flags & _Flag._ExclusiveMask)
                {
                    case _Flag.Date: case _Flag.Smalldatetime:
                        fmt[nColsOut] = (flags & _Flag._ExclusiveMask); break;
                    default: break;
                }
                ++nColsOut;
                comma = ",";    // must be omitted when 'continue' occurs above
            }
            string[] result = { null, "@p_insertData" + p_paramNamePostfix, null };
            sb.AppendFormat(" FROM (SELECT (SeqNr / {0}) AS R, (SeqNr % {0}) AS M, Item"
                + " FROM dbo.SplitStringToTable({1},'{2}')) P PIVOT (MIN(Item) FOR M IN ({3})) AS S",
                nColsOut, result[1], SEP, pivotCols);
            if (p_insertIdentity && 0 <= m_firstIdentity)
                sb.AppendFormat(";{1}SET IDENTITY_INSERT [{0}] OFF;", m_tableName, Environment.NewLine);
            result[0] = sb.ToString();
            sb.Clear();
            int count = 0, k = -1;
            foreach (object val in p_values)
            {
                if ((m_columns[++k >= n ? k = 0 : k].m_flags & skip) != 0)
                    continue;
                if (count++ > 0)
                    sb.Append(SEP);
                if (val == null)
                    continue;
                switch (fmt[k])
                {
                    case _Flag.Date          : sb.AppendFormat(invCult, "{0:yyyyMMdd}", val); break;
                    case _Flag.Smalldatetime : sb.Append(DBUtils.DateTime2Str((DateTime)val)); break;
                    default :
                        string s = Convert.ToString(val, invCult); 
                        if (!String.IsNullOrEmpty(s))
                            sb.Append(s);   // sb.Append(s.Replace("'", "''"));
                        break;
                }
            }
            result[2] = sb.ToString();
            return result;
        }

        internal string ToString(System.Collections.IEnumerable p_values)
        {
            var invCult = System.Globalization.CultureInfo.InvariantCulture;
            return Utils.ComposeCSVLine(",", null, p_values.Cast<object>().Select((val, i) => {
                if (val == null)
                    return val;
                switch (m_columns[i % m_columns.Length].m_flags & _Flag._ExclusiveMask)
                {
                    case _Flag.Date:          return String.Format(invCult, "{0:yyyy-MM-dd}", val);
                    case _Flag.Smalldatetime: return String.Format(invCult, "{0:yyyy'-'MM'-'dd'T'HH':'mm':00Z'}", val);
                    default: 
                        string s = Convert.ToString(val, invCult);
                        return String.IsNullOrEmpty(s) ? s : s.Replace("'", "''");
                }
            }));
        }

        // p_delegateType == typeof(FieldOperationApplicator<T>)
        internal Delegate GenerateApplicator_locked(Type p_delegateType)
        {
            // public static void FieldOperationApplicator4tableName(IFieldOperation p_op, ref <m_rowType> p_this)
            // {
            //    p_op.Apply(0, ref p_this.<field0>);
            //    p_op.Apply(1, ref p_this.<field1>);
            //    ...
            // }
            MethodInfo signature = p_delegateType.GetMethod("Invoke");
            ParameterInfo[] pars = signature.GetParameters();
            Utils.StrongAssert(typeof(IFieldOperation).IsAssignableFrom(pars[0].ParameterType)  // p_op
                && (m_rowType == pars[1].ParameterType || m_rowType.MakeByRefType() == pars[1].ParameterType)); // p_this
            // Note: the DynamicMethod() ctor overload used below specifies 'm_rowType' as 'owner'.
            // This is only necessary when m_rowType isn't visible publicly (e.g. nested within
            // a non-public class).
            var method = new DynamicMethod(Utils.GetTidyTypeName(p_delegateType)+"4" + m_rowType.Name, // alternative: m_rowType.Name + "\u1785" + p_delegateType.Name,
                signature.ReturnType, pars.Select(par => par.ParameterType).ToArray(), m_rowType);
            method.DefineParameter(1, ParameterAttributes.None, pars[0].Name);  // p_op:   Ldarg_0 (because static method)
            method.DefineParameter(2, ParameterAttributes.None, pars[1].Name);  // p_this: Ldarg_1
            MethodInfo genApply = typeof(IFieldOperation).GetMethod("Apply");
            bool isRefType = !m_rowType.IsValueType;
            ILGenerator il = method.GetILGenerator(m_columns.Length * 20);
            for (int i = 0; i < m_columns.Length; ++i)
            {
                il.Emit(OpCodes.Ldarg_0);
                il.Emit(OpCodes.Ldc_I4, i);
                il.Emit(OpCodes.Ldarg_1);
                if (isRefType)
                    il.Emit(OpCodes.Ldind_Ref);
                il.Emit(OpCodes.Ldflda, m_columns[i].m_reflectionInfo);
                il.Emit(OpCodes.Callvirt, genApply.MakeGenericMethod(m_columns[i].m_reflectionInfo.FieldType));
            }
            il.Emit(OpCodes.Ret);
            return method.CreateDelegate(p_delegateType);
        }

        internal static FieldOperationApplicator<T> GetFieldOpApp4RowWithApply<T>()
        {
            return (FieldOperationApplicator<T>)Utils.ReplaceGenericParameters(
                (FieldOperationApplicator<IRowWithApply>)TemplateForRowWithApply<IRowWithApply>,
                typeof(FieldOperationApplicator<T>), typeof(T));
        }
        internal static void TemplateForRowWithApply<T>(IFieldOperation p_op, ref T p_tableRow)
            where T : IRowWithApply
        {
            p_tableRow.ApplyToAllFields(p_op);
        }
    }

}


//
// Extension methods
//

namespace HQCommon
{
    public static partial class DBUtils
    {
        public static void InsertToDb<T>(this IEnumerable<T> p_rows, DBManager p_dbManager, int p_timeoutSec = (int)MemTables.DefaultTimeout.Sec)
        {
            MemTables.RowManager0<T>.InsertRows(p_dbManager, p_rows, p_timeoutSec);
        }
        public static void SubmitChanges<T>(this IEnumerable<T> p_rows, DBManager p_dbManager, int p_timeoutSec = (int)MemTables.DefaultTimeout.Sec)
            where T : MemTables.IRow, new()
        {
            MemTables.RowManager<T>.SubmitChanges(p_dbManager, p_rows, p_timeoutSec);
        }
        /// <summary> Contains retry logic </summary>
        public static void FillWithRows<T>(this ICollection<T> p_dst, DBManager p_dbManager, string p_where = null)
            where T : MemTables.IRow, new()
        {
            p_dst.AddRange(MemTables.RowManager<T>.LoadRows(p_dbManager, (int)MemTables.DefaultTimeout.Sec, p_where, null));
        }
        public static MemTables.DbOperationFlag GetRowState<T>(ref T p_row)
            where T : MemTables.IRow
        {
            return p_row.RowState;
        }
        public static void SetRowState<T>(ref T p_row, MemTables.DbOperationFlag p_value)
            where T : MemTables.IRow
        {
            p_row.RowState = p_value;
        }

        public static string ToStringByFields<T>(T p_row)       { return MemTables.RowManager0<T>.ToString(ref p_row); }
        public static string ToStringByFields<T>(ref T p_row)   { return MemTables.RowManager0<T>.ToString(ref p_row); }

        /// <summary> p_unicodeMode: 0=detect, 1=force VARCHAR, 2=force NVARCHAR </summary>
        internal static DbParameter String2SqlParameter<P>(string p_parName, string p_stringVal, int p_unicodeMode = 0)
            where P : DbParameter, new()
        {
            var result = new P() { ParameterName = p_parName };
            switch (p_unicodeMode)
            {
                case 0 : result.DbType = IsFitInVARCHAR(p_stringVal) ? DbType.AnsiString : DbType.String; break;
                case 1 : result.DbType = DbType.AnsiString; break;
                case 2 : result.DbType = DbType.String; break;
                default: throw new ArgumentOutOfRangeException("p_unicodeMode");
            }
            result.Value = p_stringVal;
            return result;
        }

        internal enum FormatInfoForSql : sbyte { Uninitialized = 0, DateTime, DateOnly, DateTimeAsInt, Bool, WithoutQuotes, Other };

        /// <summary> Returns null if p_data == null </summary>
        internal static string FormatForSql<T>(T p_data, ref FormatInfoForSql p_fmtInfo)
        {
            if (p_fmtInfo == FormatInfoForSql.Uninitialized)
            {
                FormatInfoForSql fmt = FormatInfoForSql.Other;
                Type t = typeof(T);
                if (t.IsValueType)          // true for nullable types, too
                {
                    t = (default(T) != null) ? p_data.GetType() : Nullable.GetUnderlyingType(t);
                    if (t == typeof(DateOnly))
                        fmt = FormatInfoForSql.DateOnly;
                    else if (t == typeof(DateTimeAsInt))
                        fmt = FormatInfoForSql.DateTimeAsInt;
                    else if (t == typeof(DateTime))
                        fmt = FormatInfoForSql.DateTime;
                    else switch (Type.GetTypeCode(t))
                    {
                        case TypeCode.Boolean:
                            fmt = FormatInfoForSql.Bool; break;
                        case TypeCode.SByte  : case TypeCode.Byte   :
                        case TypeCode.Int16  : case TypeCode.UInt16 :
                        case TypeCode.Int32  : case TypeCode.UInt32 :
                        case TypeCode.Int64  : case TypeCode.UInt64 :
                        case TypeCode.Single : case TypeCode.Double : 
                        case TypeCode.Decimal:
                            fmt = FormatInfoForSql.WithoutQuotes; break;
                        default :
                            break;
                    }
                }
                p_fmtInfo = fmt;
            }
            if (p_data == null)
                return null;
            DateTime? d = null;
            string s = null;
            switch (p_fmtInfo)
            {
                case FormatInfoForSql.DateOnly:
                    d = Conversion<T, DateOnly>.Default.ThrowOnNull(p_data);
                    break;
                case FormatInfoForSql.DateTimeAsInt:
                    d = Conversion<T, DateTimeAsInt>.Default.ThrowOnNull(p_data);
                    break;
                case FormatInfoForSql.DateTime:
                    d = Conversion<T, DateTime>.Default.ThrowOnNull(p_data);
                    break;
                case FormatInfoForSql.Other:
                case FormatInfoForSql.WithoutQuotes:
                    s = Conversion<T, string>.Default.DefaultOnNull(p_data);
                    break;
                case FormatInfoForSql.Bool:
                    s = Conversion<T, bool>.Default.ThrowOnNull(p_data) ? "1" : "0";
                    break;
                default:
                    throw new NotImplementedException("don't know how to convert values of type "
                        + typeof(T) + " to string for SQL (e.g. WHERE condition)");
            }
            if (d.HasValue)
                s = "'" + (d.Value.Kind == DateTimeKind.Utc ? DBUtils.UtcDateTime2Str(d.Value)
                                                            : DBUtils.DateTime2Str(d.Value)) + "'";
            else if (!String.IsNullOrEmpty(s) && FormatInfoForSql.WithoutQuotes < p_fmtInfo)
                s = "'" + s.Replace("'", "''") + "'";
            return s;
        }

        /// <summary> Sets certain fields of p_object to NaN: those fields<para>
        /// - whose .NET type is either 'float' or 'double' (*not* float? or double?) </para><para>
        /// - have a [DbColumn] attribute specifying DbColFlag.Nullable </para><para>
        /// - and current value is 0 </para>
        /// </summary>
        public static T InitFloatFieldsToNaN<T>(T p_object)
        {
            MemTables.RowManager0<T>.GetFieldOpApplicator()(Helper4InitFloatFieldsToNaN<T>.Default, ref p_object);
            return p_object;
        }
        internal class Helper4InitFloatFieldsToNaN<A> : MemTables.IFieldOperation
        {
            public static Helper4InitFloatFieldsToNaN<A> Default = new Helper4InitFloatFieldsToNaN<A>();
            public void Apply<T>(int p_ordinal, ref T p_field)
            {
                switch (Type.GetTypeCode(typeof(T)))
                {
                    case TypeCode.Single: // T is not float? but float
                        if (IsDbColumnNullable(p_ordinal) && __refvalue(__makeref(p_field), float ) == 0)
                            __refvalue(__makeref(p_field), float ) = float.NaN;
                        break;
                    case TypeCode.Double: 
                        if (IsDbColumnNullable(p_ordinal) && __refvalue(__makeref(p_field), double) == 0)
                            __refvalue(__makeref(p_field), double) = double.NaN;
                        break;
                    default:
                        break;
                }
            }
            protected virtual bool IsDbColumnNullable(int p_ordinal)
            {
                return (MemTables.RowManager0<A>.Rep.m_columns[p_ordinal].m_flags & MemTables._Flag.Nullable) != 0;
            }
        }
    }

    public static partial class Utils
    {
        /// <summary> Sets those float/double fields of p_object to NaN that have current value 0.
        /// Important: float?/double? fields are NOT affected. </summary>
        public static T FillFloatFieldsToNaN<T>(T p_object)
        {
            MemTables.RowManager0<T>.GetFieldOpApplicator()(Helper4FillFloatFieldsToNaN<T>.Default, ref p_object);
            return p_object;
        }
        class Helper4FillFloatFieldsToNaN<A> : DBUtils.Helper4InitFloatFieldsToNaN<A>
        {
            protected override bool IsDbColumnNullable(int p_ordinal) { return true; }
        }
    }

    /// <summary> Provides a field-by-field implementation of GetHashCode(T) and Equals(T,T).
    /// (Properties are ignored, but non-public fields are used!)
    /// It is better than EqualityComparerᐸTᐳ.Default.GetHashCode(T) because that would
    /// consider the first field only (see stackoverflow.com/a/5927853) for value types,
    /// or the object reference instead of the contents (for reference types).
    /// In spite of this, EqualityComparerᐸTᐳ.Default.Equals(T,T) is employed if T is
    /// IEquatableᐸTᐳ. (When it isn't, the field-by-field comparison implemented by this
    /// class is preferred to .Default.Equals(T,T) in order to avoid ReferenceEquals
    /// (when T is ref.type) or the boxing (when T is value type).)
    /// <para>IMPORTANT: The .Default global instance of this class IS THREAD SAFE,
    /// but other instances are NOT THREAD SAFE. </para></summary>
    public class FieldsEqualityComparer<T> : IEqualityComparer<T>, MemTables.IFieldOperation
    {
        const byte GetMaxOrdinal = 0, InitFieldsArray = 1, CopyFields = 2, CopyFieldsTo = 3, CalcEquals = 4, CalcHashCode = 5;
        const byte IsClearNeeded = 1, IsGlobalDefaultInstance = 2, UseDefaultEq = 4, GlobalDefInstHasBeenCloned = 8;

        class FieldStore<F> : FieldStoreBase {
            public F m_data;
            public IEqualityComparer<F> m_fieldCmp;
            public override void Clear() { m_data = default(F); }
        }
        abstract class FieldStoreBase           { public abstract void Clear(); }
        class PrimitiveStore<F> : FieldStore<F> { public override void Clear() { } }

        [ThreadStatic]
        static FieldsEqualityComparer<T> g_reserved;
        static FieldsEqualityComparer<T> g_threadSafeGlobalInstance;
        /// <summary> Assigning a new value to this property promotes that value to be thread safe,
        /// but cannot be done after the first use. </summary>
        public static FieldsEqualityComparer<T> Default
        {
            get { return g_threadSafeGlobalInstance ?? (g_threadSafeGlobalInstance = new FieldsEqualityComparer<T>(0)); }
            set
            {
                if (g_threadSafeGlobalInstance != null && (g_threadSafeGlobalInstance.m_flags & GlobalDefInstHasBeenCloned) != 0)
                    throw new InvalidOperationException("error in " + Utils.GetCurrentMethodName());
                    // It is error because g_reserved already contains clone(s) of the current g_threadSafeGlobalInstance,
                    // and I cannot reset them. So despite of changing g_threadSafeGlobalInstance here, GetThreadStatic()
                    // would return the old continuation in old threads.
                value.m_flags |= IsGlobalDefaultInstance;
                g_threadSafeGlobalInstance = value;
            }
        }
        FieldsEqualityComparer<T> GetThreadStatic() { return g_reserved ?? (g_reserved = Clone()); }

        byte m_task, m_flags;
        int m_result;
        FieldStoreBase[] m_fieldsCopy;
        protected BitVector m_fieldsToIgnore;

        // Note: these ctors make 'this' not thread safe!
        public FieldsEqualityComparer() { }
        public FieldsEqualityComparer(BitVector p_columnsToIgnore) { m_fieldsToIgnore = p_columnsToIgnore; }

        // Internal ctor -- for the thread-safe global instance only.
        // Other instances cannot benefit from the 'SingleThreaded' trick, because
        // g_reserved can store only 1 instance per T type (and per thread) and
        // that instance is reserved: the continuation of the .Default instance.
        private FieldsEqualityComparer(int _)
        {
            m_flags = (byte)(IsGlobalDefaultInstance | (typeof(IEquatable<T>).IsAssignableFrom(typeof(T)) ? UseDefaultEq : 0));
        }

        public virtual int GetHashCode(T obj)
        {
            FieldsEqualityComparer<T> sth = (m_flags & IsGlobalDefaultInstance) != 0 ? GetThreadStatic() : this;
            sth.Init(ref obj);
            return sth.Do(CalcHashCode, ref obj);
        }
        public virtual bool Equals(T x, T y)
        {
            if ((m_flags & UseDefaultEq) != 0)
                return EqualityComparer<T>.Default.Equals(x, y);
            FieldsEqualityComparer<T> sth = (m_flags & IsGlobalDefaultInstance) != 0 ? GetThreadStatic() : this;
            sth.Init(ref x);
            try
            {
                sth.Do(CopyFields, ref x);  // this copies data to a globally referenced area (-> danger of memory leak, solved below)
                return 0 != sth.Do(CalcEquals, ref y, 1);
            }
            finally { sth.Clear(); }        // avoid memory leak
        }

        // This operation is not related to IEqualityComparer<>, just practical to be implemented here
        public virtual T Assign(ref T p_dst, T p_src)
        {
            FieldsEqualityComparer<T> sth = (m_flags & IsGlobalDefaultInstance) != 0 ? GetThreadStatic() : this;
            sth.Init(ref p_src);
            try
            {
                sth.Do(CopyFields, ref p_src);  // this copies data to a globally referenced area (-> danger of memory leak, solved below)
                sth.Do(CopyFieldsTo, ref p_dst);
                return p_dst;
            }
            finally { sth.Clear(); }        // avoid memory leak
        }

        /// <summary> Must be p_clone!=null when calling this method from an override in a descendant class </summary>
        protected virtual FieldsEqualityComparer<T> Clone(FieldsEqualityComparer<T> p_clone = null)
        {
            var result = p_clone ?? (FieldsEqualityComparer<T>)Activator.CreateInstance(GetType());
            result.m_flags = m_flags;
            if ((m_flags & IsGlobalDefaultInstance) != 0)
            {
                result.m_flags &= unchecked((byte)~IsGlobalDefaultInstance);
                m_flags |= GlobalDefInstHasBeenCloned;
            }
            result.m_fieldsToIgnore = m_fieldsToIgnore;
            return result;
        }

        void Init(ref T p_value)
        {
            if (m_fieldsCopy != null)
                return;
            Utils.StrongAssert(p_value != null);    // due to 'ref p_value.*' in the 'p_op.Apply(<ordinal>, ref p_value.<field>)' calls
            m_fieldsCopy = new FieldStoreBase[Do(GetMaxOrdinal, ref p_value, -1) + 1];
            if (m_fieldsCopy.Length == 0)
                throw new NotSupportedException(typeof(FieldsEqualityComparer<T>) + " works on fields only, but "
                    + typeof(T) + " does not have any fields");
            Do(InitFieldsArray, ref p_value);
        }
        void Clear()
        {
            if ((m_flags & IsClearNeeded) != 0)
                foreach (FieldStoreBase f in m_fieldsCopy)
                    if (f != null)
                        f.Clear();
        }
        int Do(byte p_task, ref T p_value, int p_res0 = 0)
        {
            m_task = p_task; m_result = p_res0;
            // p_value MUST NOT be null, due to the 'p_op.Apply(#, ref p_value.<field>)' calls
            MemTables.RowManager0<T>.GetFieldOpApplicator()(this, ref p_value);
            return m_result;
        }
        void MemTables.IFieldOperation.Apply<F>(int p_ordinal, ref F p_field)
        {
            switch (m_task)
            {
                case GetMaxOrdinal:
                    if (m_result < p_ordinal)
                        m_result = p_ordinal;
                    break;
                case InitFieldsArray:
                    bool ml = TypeInfo<F>.Def.CanCauseMemoryLeak;
                    FieldStore<F> f = ml ? new FieldStore<F>() : new PrimitiveStore<F>();
                    m_flags |= (ml ? IsClearNeeded : (byte)0);
                    f.m_fieldCmp = GetEqCmpForField<F>(p_ordinal);
                    m_fieldsCopy[p_ordinal] = f;
                    break;
                case CopyFields:
                    ((FieldStore<F>)m_fieldsCopy[p_ordinal]).m_data = p_field;
                    break;
                case CopyFieldsTo:
                    p_field = ((FieldStore<F>)m_fieldsCopy[p_ordinal]).m_data;
                    break;
                case CalcEquals:
                    if (m_result != 0 && !m_fieldsToIgnore[p_ordinal])
                    {
                        f = (FieldStore<F>)m_fieldsCopy[p_ordinal];
                        m_result = f.m_fieldCmp.Equals(f.m_data, p_field) ? 1 : 0;
                    }
                    break;
                case CalcHashCode:
                    if (!m_fieldsToIgnore[p_ordinal])
                    {
                        f = (FieldStore<F>)m_fieldsCopy[p_ordinal];
                        m_result = ((m_result << 5) - m_result) ^ f.m_fieldCmp.GetHashCode(p_field);
                    }
                    break;
                default:
                    throw new InvalidOperationException();
            }
        }
        protected virtual IEqualityComparer<F> GetEqCmpForField<F>(int p_ordinal)
        {
            return EqualityComparer<F>.Default;
        }
    }

    /// <summary> Overrides .Equals(T,T) to use EpsilonEqCmp at [float|double][?] fields,
    /// rendering .Equals() inconsistent with .GetHashCode():<para>
    /// 1) when x,y are "epsilon-near", Equals(x,y) is true but their GetHashCode()s may be different;</para><para>
    /// 2) Equals() is not transitive: "x-near-y" and "y-near-z" does not guarantee "x-near-z".</para><para>
    /// Therefore this class is suitable for Equals()-checks only -- GetHashCode() throws exception (on floats only,
    /// see EpsilonEqCmp.GetHashCode()).</para>
    /// Instances of this class are NOT THREAD SAFE! </summary>
    public class EpsilonEqCmpStructured<T> : FieldsEqualityComparer<T>
    {
        protected EpsilonEqCmp m_ecmp;
        public EpsilonEqCmpStructured() : this(Utils.REAL_EPS, Utils.FLOAT_EPS) { }
        public EpsilonEqCmpStructured(double p_dEps, float p_fEps) { m_ecmp = new EpsilonEqCmp(p_dEps, p_fEps); }
        protected override IEqualityComparer<F> GetEqCmpForField<F>(int p_ordinal)
        {
            switch (Type.GetTypeCode(typeof(F).IsValueType && (default(F) == null) ? Nullable.GetUnderlyingType(typeof(F)) : typeof(F)))
            {
                case TypeCode.Single : case TypeCode.Double : return (IEqualityComparer<F>)(object)m_ecmp;
                default :                                     return EqualityComparer<F>.Default;
            }
        }
    }

}
