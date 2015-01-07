//#define ReallyUseSqlCe // should be defined in Build/Conditional compilation symbols, not here
#if ReallyUseSqlCe
// Also required a project reference to "%ProgramFiles%\Microsoft SQL Server Compact Edition\v4.0\Desktop\System.Data.SqlServerCe.dll" 
using System.Data.SqlServerCe;
#else
using SqlCeConnection  = System.Data.SqlClient.SqlConnection;
using SqlCeParameter   = System.Data.SqlClient.SqlParameter;
using SqlCeCommand     = System.Data.SqlClient.SqlCommand;
using SqlCeDataAdapter = System.Data.SqlClient.SqlDataAdapter;
#endif
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using HQCommon.DB;

namespace HQCommon
{
	public enum SqlCommandReturn
	{
		None, SimpleScalar, Table
	}

	class QueuedSQLRecord
	{
		internal string m_sql;
		internal CommandType m_commandType;
		internal IList<DbParameter> m_sqlParams;
		internal SqlCommandReturn m_resultType;
		/// <summary> It must be one of the followings:
		/// Action, Action&lt;DataTable, Exception&gt; or
		/// Action&lt;DataTable, Exception, object&gt;
		/// </summary>
		internal Delegate m_callback;   // see also in ExecuteCallbackInAnotherThread()
		internal object m_callbackParam;
		internal object m_result;
		internal Exception m_exception;
		internal int m_commandTimeoutSec = -1;
        internal int m_nMaxTry;         // 0 means default number of (re)tries
		/// <summary> Allowed values:
		/// - null: (for non-queued commands only) create a temporary connection
		///   for the sql command and dispose it. (For queued commands, "null" has
		///   the same meaning as the corresponding DBType enum constant);
		/// - a DBType enum constant: use DBManager.[Local|Remote]SqlConnection 
		///   (along with the corresponding lock, this ensures that DBManager.Exit()
		///   won't close the connection before the completion of the command);
		/// - an Sql[Ce]Connection object (will be opened if necessary but won't be
		///   closed/disposed) (not available for queued commands)
		/// </summary>
		internal object m_connection;
	}

	// remote DBManagager works even if the Internet is off for a while. When the Internet is back, the RemoteConnection works like a charm
	// it is a .NET SqlConnection feature
	public class DBManager : HQCommon.INonBlockingDbInfo
	{
		bool m_isEnabled;       // to protect sensitive data from accessing it, after the user signed out
        Action m_lastAnnouncer;

		// the following connections strings are possible currently:
		// @"Data Source=|DataDirectory|\HedgeQuant.sdf;Max Database Size=2047"     // local DB sdf file, SQL CE
		// @"server=(local);database=HedgeQuant;UID=sa; PWD=sa"                     // local server for test
		// @"server=(local),1502;database=HedgeQuant;UID=HQServer; PWD=H667QServer"     // remote server, but as local: login as HQServer (for server programs)
		// @"Data Source=89.255.130.105,1502;Initial Catalog=HedgeQuant;Persist Security Info=True;User ID=sa;Password=RegencyClose006" // remote server: login as sa (for HQ clients)
        // @"Server=tcp:k6297l5iy9.database.windows.net;Database=HedgeQuant;User ID=sa11235sa@k6297l5iy9;Password=RegencyClose006;Trusted_Connection=False;Encrypt=True;" // remote server: login as sa11235sa (for HQ clients)
		string m_remoteConnectionString;
		readonly object m_remoteConnectionLock = new object();   // it is not possible to use the same SqlConnection from 2 threads, it throws an exception
		SqlConnection m_remoteSqlConnection;

		readonly object m_localConnectionLock = new object();
		SqlCeConnection m_localSqlConnection;

		LinkedList<QueuedSQLRecord> m_remoteQueuedSQLscripts = new LinkedList<QueuedSQLRecord>();
		LinkedList<QueuedSQLRecord> m_localQueuedSQLscripts = new LinkedList<QueuedSQLRecord>();

		//public event Action DbSynchronizationCompletedEvent;
        public event Action BeforeRemoteDbAccessEvent;

		//DBDataClassesDataContext m_localCeDataContext = null;       // for Linq 
		//DBDataClassesDataContext m_remoteDataContext = null;        // for Linq 
		// DataContext's connection is not the same as SQL's connection
		object m_remoteDataContextConnectionLock = new object();
		object m_localDataContextConnectionLock = new object();

        public readonly MemoryTables MemTables;
        ///// <summary> Keeps a strong reference on a MemtablesTickerProvider object </summary>
        //object m_tickerProviderGuard;

		public DBManager()
		{
			// this runs in the main thread
			MemTables = new MemoryTables(this);
		}

		#region LINQ
		///// <summary> Note: LINQ queries on the returned tables 
		///// must be synchronized using the appropriate lock 
		///// (see RemoteLinqLock/LocalCeLinqLock below). </summary>
		//public Table<TEntity> GetLinqTable<TEntity>() where TEntity : class
		//{
		//    return GetLinqTable<TEntity>(DBType.Remote);
		//}

		///// <summary> Note: LINQ queries on the returned tables 
		///// must be synchronized using the appropriate lock 
		///// (see RemoteLinqLock/LocalCeLinqLock below). </summary>
		//public Table<TEntity> GetLinqTable<TEntity>(DBType p_dbType) where TEntity : class
		//{
		//    DBDataClassesDataContext dataContext = null;
		//    if (p_dbType == DBType.Local)
		//    {
		//        if (m_localCeDataContext == null)
		//            m_localCeDataContext = new DBDataClassesDataContext(@"server=(local);database=HedgeQuant;UID=sa; PWD=sa");      // it is fixed  
		//        dataContext = m_localCeDataContext;
		//    }
		//    else
		//    {
		//        if (m_remoteDataContext == null)
		//            m_remoteDataContext = new DBDataClassesDataContext(m_remoteConnectionString);
		//        dataContext = m_remoteDataContext;
		//    }

		//    return dataContext.GetTable<TEntity>();
		//}

		/// <summary> Note: LINQ queries on the tables of these data contexts
		/// must be synchronized when executed, using the appropriate lock 
		/// (see RemoteLinqLock/LocalCeLinqLock below). </summary>
		public DBDataClassesDataContext CreateDataContext(DBType p_dbType)
		{
            if (!IsEnabled)
            {
                Log.Warning("*** WARNING in {0}: DBManager.IsEnabled=false", Utils.GetCurrentMethodName());
                return null;
            }
			if (p_dbType == DBType.Local)
			{
				throw new NotImplementedException();
				//if (m_localCeDataContext == null)
				//    m_localCeDataContext = new DBDataClassesDataContext(@"server=(local);database=HedgeQuant;UID=sa; PWD=sa");      // it is fixed  
				//return m_localCeDataContext;
			}
			else
			{
				//if (m_remoteDataContext == null)
				//    m_remoteDataContext = new DBDataClassesDataContext(m_remoteConnectionString);
				//return m_remoteDataContext;
				string connStr = m_remoteConnectionString;
				if (String.IsNullOrEmpty(connStr))
					return null;

                BeforeRemoteDbAccessEvent.Fire();
                DBDataClassesDataContext dataContext = new DBDataClassesDataContext(m_remoteConnectionString);
                //The default value of the DataContext class's CommandTimeout  is set to 30 seconds. Any database queries taking a longer time to complete than 30 seconds will throw a System.Data.SqlClient.SqlException: Timeout expired Exception. 
                dataContext.CommandTimeout = 90;    // 90 sec
                return dataContext;
			}
		}

        /// <summary> This method is a syntactic sugar for this: <para>
        ///  for (int nTry = 1; true; ++nTry)                                                                           </para><para>
        ///  try {                                                                                                      </para><para>
        ///      using (DBDataClassesDataContext dataContext = CreateDataContext(DBType.Remote)) p_task(dataContext);   </para><para>
        ///  } catch (SqlException e) {                                                                                 </para><para>
        ///      if (p_nMaxTry &lt;= nTry || !DBManager.IsSqlExceptionToRetry(e)) throw; // + log message               </para><para>
        ///  }</para>
        /// </summary>
        public void ExecuteWithDataContextAndRetry(int p_nMaxTry, Action<DBDataClassesDataContext> p_task)
        {
            ExecuteWithDataContextAndRetry(p_task, p_nMaxTry);
        }
        public void ExecuteWithDataContextAndRetry(Action<DBDataClassesDataContext> p_task, int p_nMaxTry = 0)
        {
            ExecuteWithDataCtxR<int>(ctx => { p_task(ctx); return 0; }, p_nMaxTry, p_task);
        }
        public TReturn ExecuteWithDataContextAndRetry<TReturn>(Func<DBDataClassesDataContext, TReturn> p_task, int p_nMaxTry = 0)
        {
            return ExecuteWithDataCtxR<TReturn>(p_task, p_nMaxTry, p_task);
        }
        TReturn ExecuteWithDataCtxR<TReturn>(Func<DBDataClassesDataContext, TReturn> p_task, int p_nMaxTry,
            Delegate p_originalTask)
        {
            return ExecuteSqlActionWithRetry(p_task, (task, connTester) => {
                using (DBDataClassesDataContext dataContext = CreateDataContext(DBType.Remote))
                {
                    connTester(dataContext.Connection);
                    return task(dataContext);
                }
            }, p_nMaxTry, p_originalTask);
        }

		// Locks for synchronizing LINQ queries related to Table objects
		// returned by GetLinqTable<>() 
		public object RemoteLinqLock  { get { return GetLinqLock(DBType.Remote); } }
		public object LocalCeLinqLock { get { return GetLinqLock(DBType.Local); } }

		public object GetLinqLock(DBType p_dbType)
		{
			return p_dbType == DBType.Local ? m_localDataContextConnectionLock
											: m_remoteDataContextConnectionLock;
		}

		#endregion

		#region General

        /// <summary> Used to protect sensitive data from accessing it, after the user
        /// signed out and before signed in. See email Archidata2012/4f0efe7d
        /// </summary>
        public bool IsEnabled
        {
            get { return m_isEnabled; }
            set
            {
                if (!m_isEnabled && value)
                    PostponeOfflineToOnlineAnnouncement();
                m_isEnabled = value;
            }
        }

        void PostponeOfflineToOnlineAnnouncement()
        {
            // Trigger the ChangeNotification.OnDbManagerEnable() event
            // when BeforeRemoteDbAccessEvent occurs, or after 10 seconds.
            // Whichever happens first, cancels the other, to ensure that
            // the notification is not generated twice (even if the two
            // happens concurrently). Also cancel a previously postponed
            // notification if it's still pending.
            const int TimeoutMsec = 10 * 1000;
            Timer t = null;
            // Cancel previous announcement, if still pending
            Action onRemoteDbAccess = Interlocked.Exchange(ref m_lastAnnouncer, null);
            if (onRemoteDbAccess != null)   // yes, it was pending
                onRemoteDbAccess();         // complete canceling (cancel its timer, too). This won't call ChangeNotification.OnDbManagerEnable()
            m_lastAnnouncer = onRemoteDbAccess = delegate {
                lock (onRemoteDbAccess)
                {
                    BeforeRemoteDbAccessEvent -= onRemoteDbAccess;
                    if (t == null)
                        return;
                    Utils.DisposeAndNull(ref t);
                }
                if (onRemoteDbAccess == Interlocked.CompareExchange(ref m_lastAnnouncer, null, onRemoteDbAccess) && IsEnabled)
                    ChangeNotification.OnDbManagerEnable(this);
            };
            t = new Timer(p_action => ((Action)p_action)(), onRemoteDbAccess, TimeoutMsec, Timeout.Infinite);
            BeforeRemoteDbAccessEvent += onRemoteDbAccess;
        }

		public SqlConnection RemoteSqlConnection
		{
			get
			{
                // Note: the following check for IsEnabled is added to ensure that
                // the remote connection won't be reopened once DBManager.Exit() is
                // entered (which sets IsEnabled=false). Consider the scenario when
                // one thread is executing DBManager.Exit() due to exception handling,
                // and another thread is calling ExecuteSqlCommandInternal()
                if (IsEnabled)
                {
                    if (m_remoteSqlConnection == null)
                        ForceOpenRemoteConnection();
                    else
                        BeforeRemoteDbAccessEvent.Fire();
                }
				return m_remoteSqlConnection;
			}
		}

		public DbConnection LocalSqlConnection
		{
			get
			{
				if (m_localSqlConnection != null)
					return m_localSqlConnection;

				// Instead of force-open, we do simple "open", because it would be
				// senseless to close & reopen the connection for every thread that
				// gets blocked here during the first synchronization (it is normal).
				OpenLocalConnection();
				return m_localSqlConnection;
			}
		}

		/// <summary> No attempt to connect when just querying this property. THREAD SAFE. </summary>
		public bool WasRemoteConnectionAlive
		{
			get { return m_remoteSqlConnection != null; }
		}

		/// <summary> Returns true if SqlCE has been connected 
		/// successfully (at least once). Does not attempt to connect it. </summary>
		public bool WasLocalConnectionAlive
		{
			get { return m_localSqlConnection != null; }
		}
        ///// <summary> Returns default(DateTime) if SynchronizeLocalDB() has not been called yet </summary>
        //public DateTime SdfLastSyncTimeUtc
        //{
        //    get
        //    {
        //        lock (m_localConnectionLock)
        //            if (m_sdfFilenameAndLastSyncTime != null)
        //                return m_sdfFilenameAndLastSyncTime.m_second;
        //        return default(DateTime);
        //    }
        //}

		public void Init_WT()
		{
            //m_remoteDataSet = new DataSet();
            //m_localDataSet = new DataSet();
		}

		/// <summary>Throws exception if the connection fails.</summary>
		/// <exception cref="HQCommon.ConnectionException">If failed to connect</exception>
		public void ConnectToServer(string p_connectionString)
		{
            IsEnabled = true;
            SetRemoteConnectionString(p_connectionString);
            Exception result;
            if (!ForceOpenRemoteConnection(out result))
                throw new ConnectionException("Connection to SQL Server failed."
                    + " Connection string:\n" + p_connectionString, result, this);
		}

		public void SetRemoteConnectionString(string p_fullConnectionString)
		{
            // Embed application name in the conn.string
            p_fullConnectionString = EmbedAppnameInConnString(p_fullConnectionString);

			if (m_remoteConnectionString == p_fullConnectionString)
				return;

			// clear the previous connection, because it is invalid now
			if (m_remoteSqlConnection != null)
				lock (m_remoteConnectionLock)
				{
					if (m_remoteSqlConnection != null)
					{
						m_remoteSqlConnection.Close();
						m_remoteSqlConnection = null;
					}
				}

			m_remoteConnectionString = p_fullConnectionString;
			Thread.MemoryBarrier();
		}

		bool ForceOpenRemoteConnection()
		{
			Exception e;
			bool result = ForceOpenRemoteConnection(out e);
			if (e != null)
				Log.PrintException(e, false, "in {0}", Utils.GetCurrentMethodName());
			return result;
		}

		bool ForceOpenRemoteConnection(out Exception p_error)
		{
            var before = new WeakReference(m_remoteSqlConnection);
            BeforeRemoteDbAccessEvent.Fire();   // it must happen _before_ connecting, as the name says

            lock (m_remoteConnectionLock)
            {
                if (m_remoteSqlConnection != null && m_remoteSqlConnection != before.Target)
                {
                    // m_remoteSqlConnection changed: either non-null=>other or null=>non-null
                    // this means that this function was recursed (maybe a BeforeRemoteDbAccessEvent
                    // handler called it, or some another thread).
                    // Conclusion: there's a new connection, we shouldn't drop that
                    p_error = null;
                    return true;
                }
                before = null;
                try
                {
                    using (var previousConnection = m_remoteSqlConnection)
                    {
                        m_remoteSqlConnection = new SqlConnection(m_remoteConnectionString);
                        m_remoteSqlConnection.Open();     // takes 1-2 seconds usually
                        p_error = null;
                    }
                }
                catch (Exception e)
                {
                    m_remoteSqlConnection = null;
                    p_error = e;
                    return false;
                    //MessageBox.Show("Error! Cannot open remote connection to SQL Server. Check Internet connection or try again later.");
                }

                // Use ThreadPool to avoid deadlock
                ThreadPool.QueueUserWorkItem(obj => TickerProvider.InitOrUpdateProviders(new MiniCtx((DBManager)obj), false), this);
            #if DEBUG
                if (Utils.AssertUiEnabled)
                {
                    // To avoid evaluation timeouts in debugger, load the Stock table in advance.
                    // If there's no internet connection, MemTables.Stock throws an exception
                    // (but that's unlikely because the connection succeeded above).
                    try { ReferenceEquals(MemTables.Stock, null); } catch { }
                }
            #endif
                return true;
            }
		}

		// If you need ForceOpen, call ForceCloseLocalConnection() + OpenLocalConnection()
		bool OpenLocalConnection()
		{
			if (m_localSqlConnection != null)
				return true;
            lock (m_localConnectionLock)
			{
				if (m_localSqlConnection != null)
					return true;
				try
				{
					Utils.StrongAssert(m_localSqlConnection == null);

					m_localSqlConnection = new SqlCeConnection(LocalConnectionString);
					m_localSqlConnection.Open();        // can take 0.5 seconds
					return true;
				}
				catch (Exception e)
				{
					Log.PrintException(e, false, "catched in {0}", Utils.GetCurrentMethodName());
					m_localSqlConnection = null;
					//MessageBox.Show("Error! Cannot open local connection to SQL Server. Synchronize the DB");
					return false;
				}
			}
		}
		private string LocalConnectionString
		{
			get { return Properties.Settings.Default.ClientHedgeQuantConnectionString; }
		}

		public void Exit()
		{
            IsEnabled = false;  // see also the notes in RemoteSqlConnection.get
            Thread.MemoryBarrier();
			int timeout = ApplicationState.IsExiting ? 2000 : Timeout.Infinite;
			ForceCloseConnection(m_remoteConnectionLock, ref m_remoteSqlConnection, timeout);
			ForceCloseConnection(m_localConnectionLock, ref m_localSqlConnection, timeout);
		}

		void ForceCloseConnection<TConn>(object p_sync, ref TConn p_connection, int p_timeoutMsec)
			where TConn : DbConnection
		{
			if (p_connection == null)
				return;
			if (!Monitor.TryEnter(p_sync, p_timeoutMsec))
			{
				Log.Warning("Warning: not closing {0} because it is locked for more than {1} msecs",
						p_connection.GetType().Name, p_timeoutMsec);
				return;
			}
			try
			{
				if (p_connection != null)
				{
					p_connection.Close();
					p_connection = null;
				}
			}
			catch (Exception e)
			{
				Log.PrintException(e, false, "occurred in " + Utils.GetCurrentMethodName());
			}
			finally
			{
				Monitor.Exit(p_sync);
			}
		}

		public void ForceCloseLocalConnection()
		{
			ForceCloseConnection(m_localConnectionLock, ref m_localSqlConnection, Timeout.Infinite);
		}

        public static ILogger Log
        {
            get { return Utils.Logger4<DBManager>(); }
        }

        public static string EmbedAppnameInConnString(string p_connString)
        {
            if (p_connString.Contains("App=") || p_connString.Contains("Application Name="))
            {
                var csb = Utils.TryOrLog(p_connString, null, str => new System.Data.SqlClient.SqlConnectionStringBuilder(str));
                if (csb == null || !String.IsNullOrEmpty(csb.ApplicationName))
                    return p_connString;
            }
            string tmp = AppnameForDebug.ToSqlConnectionString;
            return String.IsNullOrEmpty(tmp) ? p_connString
                : String.Concat(p_connString, p_connString.EndsWith(";") ? null : ";", "App=", tmp, ";");
        }

        /// <summary> p_dbManager may be either DBManager, Func&lt;DBManager&gt;, IContext,
        /// a WeakReference to any of these, or null </summary>
        public static DBManager FromObject(object p_dbManager, bool p_throwOnNull)
        {
            DBManager result = p_dbManager as DBManager;
            if (result != null)
                return result;
            var w = p_dbManager as WeakReference;
            if (w != null)
                return FromObject(w.Target, p_throwOnNull);
            var ctx = p_dbManager as IContext;
            var f = (ctx != null) ? ctx.DBManager : (p_dbManager as Func<DBManager>);
            result = (f != null) ? f() : null;
            if (p_throwOnNull)
            {
                if (result == null)
                    throw new ArgumentNullException("p_dbManager");
                if (!result.IsEnabled)
                    throw new InvalidOperationException("DBManager.IsEnabled=false");
            }
            return result;
        }

        public static void ExecuteWithRetry(Action p_task)
        {
            ExecuteSqlActionWithRetry(p_task, (task,_) => { task(); return 0; }, 0, p_task);
        }
        //public void ExecuteWithRetry(Action<DBManager> p_task)
        //{
        //    ExecuteSqlActionWithRetry(p_task, (task,_) => { task(this); return 0; }, 0, p_task);
        //}
        public static void ExecuteWithRetry<TArg>(TArg p_arg, Action<TArg> p_task)
        {
            ExecuteSqlActionWithRetry(new KeyValuePair<TArg, Action<TArg>>(p_arg, p_task),
                (kv,_) => { kv.Value(kv.Key); return 0; }, 0, Utils.GetQualifiedMethodNameLazy(p_task, p_arg));
        }
        public TReturn ExecuteWithRetry<TReturn>(Func<DBManager, TReturn> p_task)
        {
            return ExecuteSqlActionWithRetry(p_task, (task,_) => task(this), 0, p_task);
        }
        /// <summary> p_nMaxTry &lt;= 0 means default number of retries </summary>
        public static TReturn ExecuteWithRetry<TReturn>(Func<TReturn> p_task, int p_nMaxTry = 0)
        {
            return ExecuteSqlActionWithRetry(p_task, (task,_) => task(), p_nMaxTry, p_task);
        }
        public static TReturn ExecuteWithRetry<TArg, TReturn>(TArg p_arg, Func<TArg, ConnectionTesterDelegate, TReturn> p_task, int p_nMaxTry = 0)
        {
            return ExecuteSqlActionWithRetry(p_arg, p_task, p_nMaxTry, null);
        }

		#endregion

		#region ExecuteQuery

		public static System.Data.Common.DbParameter MakeSqlParameter(DBType p_dbType, string p_name, object p_value)
		{
			if (p_dbType == DBType.Local)
				return new SqlCeParameter(p_name, p_value);
			else
				return new SqlParameter(p_name, p_value);
		}

		public DataTable ExecuteQuery(string p_sql)
		{
			return ExecuteSqlCommand(DBType.Remote, p_sql, CommandType.Text, null, SqlCommandReturn.Table, -1) as DataTable;
		}

		public DataTable ExecuteQuery(string p_sql, int p_timeoutSec)
		{
			return ExecuteSqlCommand(DBType.Remote, p_sql, CommandType.Text, null, SqlCommandReturn.Table, p_timeoutSec) as DataTable;
		}

		public DataTable ExecuteQuery(DBType p_dbType, string p_sql)
		{
			return ExecuteSqlCommand(p_dbType, p_sql, CommandType.Text, null, SqlCommandReturn.Table, -1) as DataTable;
		}

        /// <summary> Executes p_sqlCommand on a temporary (remote) connection, using default (30sec) timeout </summary>
		public object ExecuteSqlCommand(string p_sqlCommand, CommandType p_commandType, IList<DbParameter> p_params, SqlCommandReturn p_sqlCommandReturn)
		{
			return ExecuteSqlCommand(DBType.Remote, p_sqlCommand, p_commandType, p_params, p_sqlCommandReturn, -1);
		}

        /// <summary> Executes p_sqlCommand (on a temporary connection if p_dbType==Remote). p_timeoutSec=0 means infinite </summary>
		public object ExecuteSqlCommand(DBType p_dbType, string p_sqlCommand, CommandType p_commandType, IList<DbParameter> p_params,
            SqlCommandReturn p_sqlCommandReturn, int p_timeoutSec)
		{
			QueuedSQLRecord cmd = new QueuedSQLRecord();
			cmd.m_commandType = p_commandType;
			cmd.m_resultType = p_sqlCommandReturn;
			cmd.m_sql = p_sqlCommand;
			cmd.m_sqlParams = p_params;
			cmd.m_commandTimeoutSec = p_timeoutSec;
			cmd.m_connection = (p_dbType == DBType.Local) ? (object)p_dbType : null;
            cmd.m_callback = ExecuteSqlCommand_dummy();             // prevent logging "swallowed" message in case of exception
			ExecuteSqlCommandOrCommands(cmd);
			// Re-throw the exception, if occurred
			if (cmd.m_exception != null)
				throw Utils.PreserveStackTrace(cmd.m_exception);
			return cmd.m_result;
		}
        static Action<object, Exception> ExecuteSqlCommand_dummy() { return delegate { }; }

        /// <summary> Executes p_sqlCommand on DBManager.RemoteSqlConnection using the corresponding lock.
        /// This ensures that DBManager.Exit() won't close the connection before the completion of the command.
        /// This can also be used to execute subsequent T-SQL batches in the same connection.
        /// Note that, however, the connection may get closed and reopened due to connection errors.
        /// Therefore it is not suitable for multi-batch transactions: you may get Msg=3902 (even with p_nMaxTry=1)
        /// "The COMMIT TRANSACTION request has no corresponding BEGIN TRANSACTION."
        /// </summary>
		public object ExecuteSqlCommandRemoteSync(string p_sqlCommand, CommandType p_commandType, IList<DbParameter> p_params,
            SqlCommandReturn p_sqlCommandReturn, int p_timeoutSec, int p_nMaxTry = 0)
		{
			QueuedSQLRecord cmd = new QueuedSQLRecord();
			cmd.m_commandType = p_commandType;
			cmd.m_resultType = p_sqlCommandReturn;
			cmd.m_sql = p_sqlCommand;
			cmd.m_sqlParams = p_params;
			cmd.m_commandTimeoutSec = p_timeoutSec;
            cmd.m_nMaxTry = p_nMaxTry;
			cmd.m_connection = DBType.Remote;
            cmd.m_callback = ExecuteSqlCommandRemoteSync_dummy();   // prevent logging "swallowed" message in case of exception
			ExecuteSqlCommandOrCommands(cmd);
			// Re-throw the exception, if occurred
			if (cmd.m_exception != null)
				throw Utils.PreserveStackTrace(cmd.m_exception);
			return cmd.m_result;
		}
        static Action<object, Exception> ExecuteSqlCommandRemoteSync_dummy() { return delegate { }; }


		public void ExecuteQueryAsync(string p_sql, Action<DataTable, Exception> p_callback, DBType p_dbType = DBType.Remote, int p_timeoutSec = -1)
		{
			ExecuteQueryAsyncInternal(p_dbType, p_sql, CommandType.Text, null, SqlCommandReturn.Table,
				p_callback, null, p_timeoutSec);
		}

		// Use the above overload with an anonymous delegate instead of this
		//public void ExecuteQueryAsync(DBType p_dbType, string p_sql, Action<DataTable, Exception, object> p_callback3, object p_callbackParam, int p_timeoutSec)
		//{
		//    ExecuteQueryAsyncInternal(p_dbType, p_sql, CommandType.Text, SqlCommandReturn.Table, p_callback3, p_callbackParam, p_timeoutSec);
		//}

		/// <summary> p_callback receives a table (SqlCommandReturn.Table) </summary>
		public void ExecuteSqlCommandAsync(DBType p_dbType, string p_sqlCommand, CommandType p_commandType,
			IList<DbParameter> p_params, Action<DataTable, Exception> p_callback, int p_timeoutSec)
		{
			ExecuteQueryAsyncInternal(p_dbType, p_sqlCommand, p_commandType, p_params, SqlCommandReturn.Table,
				p_callback, null, p_timeoutSec);
		}

		/// <summary> p_callback receives null if p_sqlCommandReturn==None </summary>
		public void ExecuteSqlCommandAsync(DBType p_dbType, string p_sqlCommand, CommandType p_commandType,
			IList<DbParameter> p_params, SqlCommandReturn p_sqlCommandReturn, Action<object, Exception> p_callback, 
            int p_timeoutSec)
		{
			ExecuteQueryAsyncInternal(p_dbType, p_sqlCommand, p_commandType, p_params, p_sqlCommandReturn,
				p_callback, null, p_timeoutSec);
		}

		void ExecuteQueryAsyncInternal(DBType p_dbType, string p_sql, CommandType p_commandType,
			IList<DbParameter> p_params, SqlCommandReturn p_sqlCommandReturn, Delegate p_callback,
			object p_callbackParam, int p_timeoutSec)
		{
			QueuedSQLRecord cmd = new QueuedSQLRecord();
			cmd.m_commandType = p_commandType;
			cmd.m_resultType = p_sqlCommandReturn;
			cmd.m_sql = p_sql;
			cmd.m_sqlParams = p_params;
			cmd.m_callback = p_callback;
			cmd.m_callbackParam = p_callbackParam;
			cmd.m_commandTimeoutSec = p_timeoutSec;
			if (p_dbType == DBType.Local)
				AddSqlCommandToQueue(p_dbType, cmd);
			else
				ThreadPool.QueueUserWorkItem(ExecuteSqlCommandOrCommands, cmd);
		}

		void AddSqlCommandToQueue(DBType p_dbType, QueuedSQLRecord p_rec)
		{
			// Queued commands are to be executed with DBManager.[Local|Remote]SqlConnection
			Utils.StrongAssert(p_rec.m_connection == null || p_dbType.Equals(p_rec.m_connection));
			LinkedList<QueuedSQLRecord> queue = (p_dbType == DBType.Remote) ? m_remoteQueuedSQLscripts : m_localQueuedSQLscripts;
			lock (queue)
			{
				queue.AddLast(p_rec);
				if (queue.Count == 1)
					ThreadPool.QueueUserWorkItem(ExecuteSqlCommandOrCommands, queue);
			}
		}

		/// <summary> p_object may be a command (QueuedSQLRecord), or a queue. </summary>
		void ExecuteSqlCommandOrCommands(Object p_object)
		{
			if (p_object == null)
				throw new ArgumentNullException();
			LinkedList<QueuedSQLRecord> queue = null;
			DBType dbType = DBType.Remote;
			QueuedSQLRecord rec = p_object as QueuedSQLRecord;
			if (rec == null)
				queue = (LinkedList<QueuedSQLRecord>)p_object;
			else if (rec.m_connection == null)
			{ }
			else if (rec.m_connection is DBType)
				dbType = (DBType)rec.m_connection;
			else if (rec.m_connection is SqlCeConnection)
				dbType = DBType.Local;

			while (true)
			{
				// if the queue is used, it preserves the order of sql commands (may be important)
				if (queue != null)
				{
					lock (queue)
					{
						if (rec != null)
							queue.RemoveFirst();
						if (queue.Count == 0)
							return;
						rec = queue.First.Value;        // leave in the queue so further requests
					}                                   // won't fork this method concurrently
					dbType = (queue == m_localQueuedSQLscripts) ? DBType.Local : DBType.Remote;
					// Queued commands must always be executed with DBManager.[Local|Remote]SqlConnection
					Utils.StrongAssert(rec.m_connection == null || dbType.Equals(rec.m_connection));
					rec.m_connection = dbType;
				}
				ExecuteSqlActionWithRetry(new KeyValuePair<DBType, QueuedSQLRecord>(dbType, rec),
                    (kv, connTester) => {
                        ExecuteSqlCommandInternal(kv.Key, kv.Value, connTester);
                        return 0;
                    }, rec.m_nMaxTry, rec);

				if (queue == null)
					break;
				if (rec.m_callback != null)
					ThreadPool.QueueUserWorkItem(ExecuteCallback, rec);
			}
			ExecuteCallback(rec);
		}

        /// <summary> Catches any exception during p_sqlAction() and logs it. If the error is 
        /// retryable, re-tries p_sqlAction() at most p_nMaxTry-1 times (i.e. it is tried at most
        /// p_nMaxTry times, 1 means no retry, 0 means default number of retries, nMaxTryDefault).
        /// In other cases re-throws the exception; except when p_rec is a QueuedSQLRecord: in this
        /// case records the Exception object in p_rec.m_exception and returns default(TReturn). <para>
        /// p_rec may be a QueuedSQLRecord, string or Func&lt;String&gt; (T-SQL script or method name),
        /// Delegate (the name of the method is used in log message), null or Object (.ToString()
        /// called when logging error) </para></summary>
        static TReturn ExecuteSqlActionWithRetry<TArg, TReturn>(TArg p_arg,
            Func<TArg, ConnectionTesterDelegate, TReturn> p_sqlAction, int p_nMaxTry, object p_rec)
        {
            if (p_sqlAction == null)
                throw new ArgumentNullException();
            QueuedSQLRecord rec = p_rec as QueuedSQLRecord;
            object whatToRetry;
            if (rec != null)
                whatToRetry = rec.m_sql;
            else if (p_rec == null || p_rec is Delegate)
                whatToRetry = Utils.GetQualifiedMethodNameLazy(p_rec == null ? p_sqlAction : (Delegate)p_rec, p_arg);
            else if (null == (whatToRetry = p_rec as LazyString))
                whatToRetry = (object)(p_rec as String) ?? new LazyString(p_rec);   // LazyString: support for Func<string>

            if (p_nMaxTry <= 0)
                p_nMaxTry = NMaxTryDefault;
            for (int @try = 1; true; ++@try)
            {
                try
                {
                    // IMPORTANT: multi-command sql scripts, IF include modifications, MUST BE enclosed
                    // in transaction:
                    //   SET XACT_ABORT ON; -- don't continue if any SELECT fails
                    //   BEGIN TRANSACTION;
                    //     ...
                    //   COMMIT TRANSACTION;
                    // The caller is responsible for this.
                    // For explanation, see email "Re: SQL server exception: TCP Provider..."
                    // [Tue, 29 Mar 2011 10:24:27 +0200] (date:4d91973b)
                    //
                    if (rec != null && rec.m_exception != null)
                        rec.m_exception = null;     // might be non-null in case of retry
                    return p_sqlAction(p_arg, ExamineConnectionAndWriteToLog);
                }
                catch (Exception error)
                {
                    var r1 = new Func<string, string, string, string>((s, ptn, repl) => {   // replace 1st occurrence of 'ptn'
                        int i = (s ?? (s="")).IndexOf(ptn ?? (ptn=""));
                        return (i < 0) ? s : s.Substring(0, i) + repl + s.Substring(i + ptn.Length);
                    });
                    SqlException sqlex;
                    bool becauseExiting = ApplicationState.IsOtherThreadExiting;
                    if (!becauseExiting && @try < p_nMaxTry 
                        && (Utils.CanBe(error, out sqlex) ? IsSqlExceptionToRetry(sqlex, @try-1)
                                                          : IsNonSqlExceptionToRetry(error, @try-1))
                    ) {
                        // Instead of Error(), we do a Warning() here on the first couple of tries
                        if (System.Diagnostics.TraceLevel.Warning <= Log.Level)
                            Log.Warning("Retriable sql error{0} (try#{1}/{2}):{3}{5}{3}**retrying**: {4}",
                                rec == null ? null : String.Format(" (timeout {0}s{1})", rec.m_commandTimeoutSec, rec.m_commandTimeoutSec == 0 ? "=infinite" : null),
                                @try, p_nMaxTry, Environment.NewLine, DBUtils.LimitedLengthSqlString(whatToRetry, 0, "SQLwrn"),     // 1..4
                                r1(r1(Logger.FormatExceptionMessage(error, false, ""), "*** ", ""), "\n", " ")                      // 5
                            );
                        if (rec != null && !rec.m_sqlParams.IsEmpty())
                        {
                            // Avoid ArgumentException: "The SqlParameter is already contained by
                            // another SqlParameterCollection" (Sql[Ce]Command.Parameters.Add())
                            int n = rec.m_sqlParams.Count;
                            var tmp = new List<DbParameter>(n);
                            for (int i = 0; i < n; ++i)
                                tmp.Add((DbParameter)(((ICloneable)rec.m_sqlParams[i]).Clone()));
                            rec.m_sqlParams = tmp;
                        }
                        switch (@try & 3)   // { 2-5sec, 30sec, 2min, 4min } repeated
                        {
                            case 1: Thread.Sleep(         2000 + new Random().Next(3000)); break;   // wait 2-5 secs
                            case 2: Thread.Sleep(    30 * 1000); break;
                            case 3: Thread.Sleep(2 * 60 * 1000); break;
                            case 0: Thread.Sleep(4 * 60 * 1000); break;
                        }
                        continue;
                    }

                    // After p_nMaxTry failed tries (or if the error is non-retriable)
                    // we report an Error(), instead of Warning()
                    string msg1 = null, msg2 = null;
                    if (rec == null)
                        msg1 = " (rethrowing)";
                    else if (rec.m_callback != null)
                        msg2 = "Now passing this exception to callback=" + Utils.GetQualifiedMethodName(rec.m_callback.Method);
                    else
                        msg1 = " (swallowing)"; // actually, returning 'error' to the caller in rec.m_exception, but then it'll go nowhere

                    if (becauseExiting)
                        msg1 = Utils.FormatInvCult(" (ApplicationExitThread==th#{0}) {1}", ApplicationState.ApplicationExitThread.ManagedThreadId, msg1);

                    // The error message will look like this:
                    // *** XYException in .ExecuteSqlActionWithRetry() try#3/4 <msg1>\n...\nwhile executing "<whatToRetry>"\n<msg2>
                    Utils.AddHqIncidentId(error);
                    msg1 = Logger.FormatExceptionMessage(error, @try==1, "in {0} try#{1}/{2}{3}",  // try#1 is the first try out of p_nMaxTry
                            r1(Utils.GetCurrentMethodName(),typeof(DBManager).FullName,""), @try, p_nMaxTry, msg1);
                    Log.Error("{0}{1}while executing \"{2}\"{1}{3}", msg1, Environment.NewLine,
                        DBUtils.LimitedLengthSqlString(whatToRetry, 0, "SQLerr"), msg2);
                    Utils.MarkLogged(error, @try==1);
                    if (rec != null)
                    {
                        rec.m_exception = error;
                        return default(TReturn);
                    }
                    throw;
                } //~ catch
            }  //~ for
        }
        /// <summary> Default number of retries for Execute…With…Retry() functions </summary>
        public static int NMaxTryDefault { get { return HQCommon.Properties.Settings.Default.SqlNTryDefault; } }

        /// <summary> A method provided by DBManager, which should be invoked
        /// when the connection is believed to be available, just before executing
        /// the actual sql script. It helps to work-around a bug in .NET Framework
        /// connection pooling (it may return an SqlConnection that says State==Open
        /// despite the fact that Sql Azure already closed the connection and thus
        /// the underlying TdsParser is broken http://j.mp/gdw4XL) </summary>
        public delegate void ConnectionTesterDelegate(DbConnection p_conn);

        /// <summary> Attempts to elminiate connection loss error during the actual T-SQL script
        /// by executing a dummy query ("SELECT GETDATE()") in every 10 minutes (at most). This
        /// dummy query will throw exception if the connection is closed. </summary>
        /// <remarks> This is an attempt for more advanced handling (= detecting) sporadic
        /// connection loss errors caused by Sql Azure.
        /// The 5-minute frequency is obeyed by keeping track of the pooled internal connections 
        /// (via WeakReferences) and the last time when the dummy query was executed on them.
        /// See also sql-szivasaim.txt, 110329 </remarks>
        private static void ExamineConnectionAndWriteToLog(DbConnection p_conn)
        {
            if (p_conn == null)
                return;
            ConnectionState st = p_conn.State;
            if (st != ConnectionState.Open)
            {
                Log.Verbose("Attempting to re-open SqlConnection because State={0}", st);
                if (st == ConnectionState.Broken)
                    p_conn.Close();
                p_conn.Open();
            }

            if (g_innerConnProp == null)
            {
                var B = System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic;
                g_innerConnProp = typeof(SqlConnection).GetProperty("InnerConnection", B);          // .NET4.0-SPECIFIC CODE!! works in 4.5
                if (g_innerConnProp != null)
                    g_createTimeField = g_innerConnProp.PropertyType.GetField("_createTime", B);
            }
            if (g_createTimeField == null)
                return;

            object innerConnection = g_innerConnProp.GetValue(p_conn, null);
            if (innerConnection == null)
                return;
            Rec<WeakReference, long> pair = null;
            lock (g_lastChecked)
            {
                for (int i = g_lastChecked.Count; --i >= 0; )
                {
                    pair = g_lastChecked[i];
                    object innerConn = pair.m_first.Target;
                    if (innerConn == null)
                        g_lastChecked.FastRemoveAt(i);
                    else if (innerConn == innerConnection)
                        break;
                }
                if (pair == null)
                    g_lastChecked.Add(pair = new Rec<WeakReference, long>(
                        new WeakReference(innerConnection), ((DateTime)g_createTimeField.GetValue(innerConnection)).Ticks
                    ));
            }
            DateTime lastChecked = new DateTime(pair.m_second);
            if (5 * TimeSpan.TicksPerMinute < (DateTime.UtcNow - lastChecked).Ticks && p_conn is SqlConnection)
            {
                Log.Verbose("{0}#{1:x8} last checked {2}. Running 'SELECT GETDATE()'",
                    innerConnection.GetType().Name, innerConnection.GetHashCode(), Utils.HowLongAgo(lastChecked));
                new SqlCommand("SELECT GETDATE()", (SqlConnection)p_conn).ExecuteScalar();
                Interlocked.Exchange(ref pair.m_second, DateTime.UtcNow.Ticks);
            }
        }
        static System.Reflection.PropertyInfo g_innerConnProp;
        static System.Reflection.FieldInfo g_createTimeField;
        /// <summary> DbConnectionInternal &#8594; DateTime mapping </summary>
        static List<Rec<WeakReference, long>> g_lastChecked = new List<Rec<WeakReference, long>>();

		void ExecuteSqlCommandInternal(DBType p_dbType, QueuedSQLRecord p_rec, ConnectionTesterDelegate p_connTester)
		{
            if (!IsEnabled)
            {
                Log.Warning("*** WARNING: DBManager.IsEnabled=false, skipping sql command:"
                    + Environment.NewLine + DBUtils.LimitedLengthSqlString(p_rec.m_sql, 0, "SQLwrn"));
                //if (p_rec.m_resultType == SqlCommandReturn.Table)
                //    p_rec.m_result = new DataTable();   // prevent NullReferenceException in "foreach (DataRow row in dbManager.ExecuteQuery(...).Rows)" callers
                //                                      ^^ bad idea because makes impossible to detect that the query did not run actually
                return;
            }
			//if (p_dbType == DBType.Remote)
			//    Trace.WriteLine("DBManager.ExecuteSqlCommand(remote): " + p_sqlCommand);

			var disposables = new List<object>(2);
			using (Utils.DisposerStructForAll(disposables))
			{
				DbConnection connection;
				if (p_rec.m_connection == null) // Create a new (temporary) connection for this SqlCommand
				{
					switch (p_dbType)
					{
						// this one should be avoided because it is slow:
						case DBType.Local:
							connection = new SqlCeConnection(LocalConnectionString);
							break;
						// the following is fast because it utilises the connection pool of .NET Framework:
						case DBType.Remote:
                            BeforeRemoteDbAccessEvent.Fire();
							connection = new SqlConnection(m_remoteConnectionString);
							break;
						default: throw new NotImplementedException();
					}
					disposables.Add(connection);
					// Do our best to ensure the completion of this SqlCommand, even in case of Exit()
					disposables.Add(ThreadManager.Singleton.RetardApplicationExit(p_rec.m_sql,
                        sql => DBUtils.LimitedLengthSqlString(sql, 0, "SQLwrn"))); // only called when ThreadManager.IsCollectingDebugInfo
					connection.Open();
				}
				else if (null == (connection = p_rec.m_connection as DbConnection))
				{
					Utils.StrongAssert(p_dbType.Equals(p_rec.m_connection));
					switch (p_dbType)
					{   // only allow 1 thread to query at a time
						case DBType.Local:
							disposables.Add(new UnlockWhenDispose(m_localConnectionLock));  // == lock(m_localConnectionLock)
							connection = LocalSqlConnection;
							break;
						case DBType.Remote:
                            // BeforeRemoteDbAccessEvent.Fire(); -> done in RemoteSqlConnection.get
							disposables.Add(new UnlockWhenDispose(m_remoteConnectionLock)); // == lock(m_remoteConnectionLock)
							connection = RemoteSqlConnection;
							break;
						default: throw new NotImplementedException();
					}
				}
                else if (connection.State == ConnectionState.Closed)
                {
                    BeforeRemoteDbAccessEvent.Fire();
                    connection.Open();
                }

				// 1.  create a command object identifying the stored procedure
				DbCommand command = null;
				if (p_dbType == DBType.Remote)
					command = new SqlCommand(p_rec.m_sql, (SqlConnection)connection);
				else
					command = new SqlCeCommand(p_rec.m_sql, (SqlCeConnection)connection);

				if (p_rec.m_commandTimeoutSec >= 0 && p_dbType == DBType.Remote)    // -1 means default (30 sec)
					command.CommandTimeout = p_rec.m_commandTimeoutSec;             //  0 means infinite

				// set the command object so it knows to execute a stored procedure
				command.CommandType = p_rec.m_commandType;

				// add parameters to command, which will be passed to the stored procedure
				if (p_rec.m_sqlParams != null)
					foreach (DbParameter param in p_rec.m_sqlParams)
						command.Parameters.Add(param);

                if (p_dbType == DBType.Remote && p_connTester != null)
                {
                    // p_connTester((SqlConnection)connection); -- commented out to test if it can be eliminated, seems that M$ has fixed the bug j.mp/1zxTUap
                }

				if (p_rec.m_resultType == SqlCommandReturn.Table)
				{
					DataTable resultTable = new DataTable();

					DbDataAdapter dataAdapter = null;
					if (p_dbType == DBType.Remote)
						dataAdapter = new SqlDataAdapter(command as SqlCommand);
					else
						dataAdapter = new SqlCeDataAdapter(command as SqlCeCommand);

					dataAdapter.Fill(resultTable);
					p_rec.m_result = resultTable;
				}
				else
				{
					DbDataReader reader = null;
					try
					{
                        // TODO: use .ExecuteScalar() instead, as recommended in http://j.mp/X5uvUk (Best Practices for ADO.NET)

						// execute the command, use only the first read
						if (p_dbType == DBType.Remote)
							reader = (command as SqlCommand).ExecuteReader();
						else
							reader = (command as SqlCeCommand).ExecuteReader();

						reader.Read();
						if (p_rec.m_resultType == SqlCommandReturn.SimpleScalar)
							p_rec.m_result = reader.GetValue(0);
						else
							p_rec.m_result = null;
					}
					finally
					{
						if (reader != null)
							reader.Close();
					}
				}
			}
		}

		static void ExecuteCallback(Object p_object)
		{
			QueuedSQLRecord rec = p_object as QueuedSQLRecord;
			var callback2 = rec.m_callback as Action<DataTable, Exception>;
			if (callback2 != null)
			{
				callback2(rec.m_result as DataTable, rec.m_exception);
				return;
			}
			//var callback3 = rec.m_callback as Action<DataTable, Exception, object>;
			//if (callback3 != null)
			//{
			//    callback3(rec.m_result as DataTable, rec.m_exception, rec.m_callbackParam);
			//    return;
			//}
			var callback4 = rec.m_callback as Action<object, Exception>;
			if (callback4 != null)
			{
				callback4(rec.m_result, rec.m_exception);
				return;
			}
			if (rec.m_callback != null)
				rec.m_callback.DynamicInvoke();
		}
		#endregion

		#region ExtremeQuery
		/// <summary> Executes p_sql out of DBManager's queue of sql commands, and
        /// allows consuming the result on-the-fly, as the rows are received. In
        /// other words, avoids buffering the whole result to an in-memory DataTable.
        /// It is primarily designed for queries that download huge amounts of data,
        /// but can be used for small queries as well.<para>
        /// IMPORTANT: the connection may fail in the middle of receiving the result,
        /// thus the retry-policy cannot be enforced within this function. The caller
        /// is responsible for implementing the retry-policy. This usually means
        /// wrapping the call of this function with DBManager.ExecuteWithRetry().</para>
		/// The query is executed in the caller thread using an own sql connection,
        /// to facilitate thread-safety. </summary>
		public IEnumerable<DbDataReader> ExtremeQuery(string p_sql, int p_timeoutSec)
		{
            return !IsEnabled ? Enumerable.Empty<DbDataReader>()
                : new IrregularQuery().Init(p_sql, this, DBType.Remote, p_timeoutSec).ExecuteReader(p_disposeThisAtEnd: true);
		}

		/// <summary> Associate class to execute an sql command out of DBManager's
		/// queue of sql commands (hence its name: irregular).
		/// This class executes the query in the caller thread using an own sql
		/// connection to facilitate thread-safety.
		/// Note that Init() may be called multiple times, this allows executing
		/// several sql commands. </summary>
		/// <remarks>This class is nested into DBManager because it needs access
		/// to DBManager's private connectionString fields</remarks>
		private class IrregularQuery : DisposablePattern    // private: because does not check DBManager.IsEnabled
		{
			public DbConnection Connection { get; private set; }
			public DbCommand Command { get; private set; }

			public IrregularQuery Init(string p_sql, DBManager p_dbManager, DBType p_dbType,
				int p_timeOutSec)
			{
				return Init(p_sql, CommandType.Text, (SqlParameter[])null,
					p_dbManager, p_dbType, p_timeOutSec);
			}

			public IrregularQuery Init<TDbParameter>(string p_sql, CommandType p_commandType,
				IEnumerable<TDbParameter> p_params, DBManager p_dbManager, DBType p_dbType,
				int p_timeOutSec) where TDbParameter : DbParameter
			{
				// Reuse existing connection and command if possible
				bool isLocal = (p_dbType == DBType.Local);
				if (isLocal ? Command is SqlCeCommand : Command is SqlCommand)
				{
					Command.CommandText = p_sql;
					Command.Parameters.Clear();
				}
				else
				{
					Dispose(true);
				}
				if (Command != null)
				{ }
				else if (isLocal)
				{
					Connection = new SqlCeConnection(p_dbManager.LocalConnectionString);
					Command = new SqlCeCommand(p_sql, (SqlCeConnection)Connection);
				}
				else
				{
					// Note: p_dbManager.RemoteSqlConnection.ConnectionString hides the password
                    p_dbManager.BeforeRemoteDbAccessEvent.Fire();
					Connection = new SqlConnection(p_dbManager.m_remoteConnectionString);
					Command = new SqlCommand(p_sql, (SqlConnection)Connection);
				}
				Command.CommandType = p_commandType;
				if (!isLocal && p_timeOutSec >= 0)
					Command.CommandTimeout = p_timeOutSec;

				// add parameters to command, which will be passed to the stored procedure
				if (p_params != null)
					foreach (DbParameter param in p_params)
						Command.Parameters.Add(param);
				return this;
			}

			/// <summary> Opens the connection and returns the same DbDataReader instance
			/// for every row of the result </summary>
			public IEnumerable<DbDataReader> ExecuteReader(bool p_disposeThisAtEnd = false)
			{
				DbDataReader reader = null;
				try
				{
                    // No retry logic here, because the connection can break between 'yield' statements, too
                    // (no way to retry without yielding already yielded rows again)
                    if (Connection.State != ConnectionState.Open)
                        Connection.Open();
                    reader = Command.ExecuteReader();
                    while (reader.Read())
						yield return reader;
				}
				finally
				{
					if (reader != null)
						reader.Dispose();
                    if (p_disposeThisAtEnd)
                        Dispose();
				}
			}

			protected override void Dispose(bool p_notFromFinalize)
			{
				using (var tmp = Command)
					Command = null;
				using (var tmp = Connection)
					Connection = null;
			}
		}
		#endregion

		public string GetSdfFileFullPath()
		{
			string s = LocalConnectionString;
			s = new Regex(@"[^=]+\.sdf").Match(s).Value;
			if (s.Contains("|DataDirectory|"))
				s = s.Replace("|DataDirectory|", AppDomain.CurrentDomain.GetData("DataDirectory")
						as string ?? Utils.GetExeDir());
			return s;
		}

        /// <summary> Returns true for exceptions that occur normally because how the SQL server works.
        /// If such exception occurs, we should Retry the tasks. </summary>
        /// <see cref="http://j.mp/gdw4XL">Sporadic transport-level connection errors from SQL Azure</see>
        /// <param name="p_tryIdx">0 is the first try. Certain errors (like "Invalid object name...'
        /// that indicate bug in the query) must occur in every try, therefore should NOT be retried
        /// if occur during the first try, but if we passed the first try without that error, then
        /// in subsequent tries the same error indicates bug in the communication layer, and should be
        /// retried. </param>
        public static bool IsSqlExceptionToRetry(SqlException p_e, int p_tryIdx = 0)
        {
            if (p_e == null)
                return false;
            int @class = p_e.Class;
            switch (p_e.Number)
            {
                //"The connection is broken and recovery is not possible. The client driver attempted to recover the connection one or more times and all attempts failed. Increase the value of ConnectRetryCount to increase the number of recovery attempts."
                case     0: return IsSqlExceptionToRetry(p_e.InnerException as SqlException, p_tryIdx);

                //"Timeout expired.  The timeout period elapsed prior to completion of the operation or the server is not responding." (Class=11, Number=-2, State=0, LineNumber=0)
                //http://msdn.microsoft.com/en-us/library/cc645611.aspx
                case    -2: return true;

                //This should be retried for SqlAzure only! (which cannot be examined here)
                //"The instance of SQL Server you attempted to connect to does not support encryption." (Class=20, Number=20, State=0, LineNumber=0)
                case    20: return true;

                //"A transport-level error has occurred when receiving results from the server. (provider: TCP Provider, error: 0 - The specified network name is no longer available.) (Class=20, Number=64, State=0, LineNumber=0)" (Win32 error)
                // This can occur with Class=16, too, see notes_done.txt#110620.1
                case    64: return (@class >= 16);

                //"A transport-level error has occurred when receiving results from the server. (provider: TCP Provider, error: 0 - The semaphore timeout period has expired.) (Class=20, Number=121, State=0, LineNumber=0)"
                //http://msdn.microsoft.com/en-us/library/ms681382.aspx   (Win32 error)
                case   121:
                    return (@class >= 16);      // class==16 was observed with OPENROWSET(), see note#110620.1/2012-01-23

                //"Invalid object name '...'." Experienced with OPENROWSET(), see note#110620.1/2012-01-30
                case   208:
                    return (@class == 16 && 0 < p_tryIdx);

                // "TCP Provider: Timeout error [258]" (Class=16, Number=258, State=1, LineNumber=0) (Win32 error: WAIT_TIMEOUT)
                // Experienced with OPENROWSET(), see note#110620.1/2012-12-31, http://msdn.microsoft.com/en-us/library/ms681382.aspx
                case   258:
                    return (@class == 16);

                //"Transaction (Process ID %d) was deadlocked on %.*ls resources with another process and has been chosen as the deadlock victim. Rerun the transaction."
                //http://msdn.microsoft.com/en-us/library/aa337376.aspx
                case  1205: return true;

                //"A network-related or instance-specific error occurred while establishing a connection to SQL Server.
                // The server was not found or was not accessible. Verify that the instance name is correct and that
                // SQL Server is configured to allow remote connections. (provider: TCP Provider, error: 0 - No such host is known.)"
                // (Class=20, Number=11001, State=0, LineNumber=0)  HQEmailDrWatson/done/2012-10-16T014344.4378750Z_YahooDataCrawler.xml
                // http://msdn.microsoft.com/en-us/library/bb326379%28v=sql.100%29.aspx
                case 11001: return true;

                //"Login failed for user '%.*ls'. Reason: Server is in script upgrade mode. Only administrator can connect at this time."
                //why to retry this: http://j.mp/oPY23h
                case 18401: return (@class < 20);

                // See 'docs\robin\notes_done.txt' for more info (look for the following substrings)
                // Experienced with OPENROWSET() commands
                case 65535: return (@class == 16 && (
                       p_e.Message.Contains("Session Provider: Physical connection is not usable [xFFFFFFFF]")
                    || p_e.Message.Contains("TCP Provider: The specified network name is no longer available")
                    || p_e.Message.Contains("TCP Provider: An existing connection was forcibly closed by the remote host")));

            //
            // SQL Azure Connection-loss Errors (http://j.mp/edWus5) (http://j.mp/hGzkaI)
            //
                //"The client was unable to establish a connection because of an error during connection initialization process before login. Possible causes include the following: ... the server was too busy to accept new connections; or there was a resource limitation (insufficient memory or maximum allowed connections) on the server. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.)"
                case   233: return true;

                //"A transport-level error has occurred when sending the request to the server. (provider: TCP Provider, error: 0 - An established connection was aborted by the software in your host machine.) (Class=20, Number=10053, State=0, LineNumber=0)"
                case 10053: return true;

                //"A transport-level error has occurred when receiving results from the server. (provider: TCP Provider, error: 0 - An existing connection was forcibly closed by the remote host.) (Class=20, Number=10054, State=0, LineNumber=0)"
                case 10054:
                    return (@class >= 16);      // class==16 was observed with OPENROWSET(), see note#110620.1/2012-01-23

                // this usually occurs, when the SQL server is Disabled
                //"A network-related or instance-specific error occurred while establishing a connection to SQL Server. The server was not found or was not accessible. Verify that the instance name is correct and that SQL Server is configured to allow remote connections. (provider: TCP Provider, error: 0 - A connection attempt failed because the connected party did not properly respond after a period of time, or established connection failed because connected host has failed to respond.) (Class=20, Number=10060, State=0, LineNumber=0)"
                case 10060: return true;

                //"The service has encountered an error processing your request. Please try again. Error code %d."  (Azure failover)
                case 40197: return true;

                //"The service is currently busy. Retry the request after 10 seconds. Code: %d."
                case 40501: return true;

                //"Database '%.*ls' on server '%.*ls' is not currently available. Please retry the connection later."
                case 40613: return true;
            }
            return false;
        }

        /// <summary> p_tryIdx: 0 is the first try </summary>
        public static bool IsNonSqlExceptionToRetry(Exception p_e, int p_tryIdx)
        {
            if (p_e == null)
                return false;

            // Sometimes we parallelize queries using System.Threading.Tasks
            // (e.g. to overcome the per-connection bandwidth limitation of SqlAzure)
            // In this case retriable SqlExceptions get wrapped into AggregateException
            var a = p_e as AggregateException;
            if (a != null && a.InnerExceptions.Count ==
                a.InnerExceptions.OfType<SqlException>().Count(sqlE => IsSqlExceptionToRetry(sqlE, p_tryIdx)))
                return true;

            //InvalidOperationException: "Timeout expired.  The timeout period elapsed prior to obtaining a connection from the pool.  This may have occurred because all pooled connections were in use and max pool size was reached."
            if ((p_e is InvalidOperationException) && p_e.TargetSite.DeclaringType.Name == "DbConnectionFactory"
                && p_e.TargetSite.Name == "GetConnection")
                return true;

            //Microsoft.SqlServer.Management.Common.ConnectionFailureException with InnerException=SqlException. See note#110620.1/2013-03-11
            if (p_e.GetType().Name == "ConnectionFailureException"      // comes from Microsoft.SqlServer.ConnectionInfo.dll
                && IsSqlExceptionToRetry(p_e.InnerException as SqlException, p_tryIdx))
                return true;

            //Custom case: we caught an exception that shouldn't be retried in general, but now it should
            if (p_e is HQRetryEx && p_tryIdx < ((HQRetryEx)p_e).NTimes)
                return true;

            return false;
        }
    }

    public class HQRetryEx : Exception
    {
        public int NTimes;

        public HQRetryEx(string p_message, Exception p_innerException)
            : this(p_innerException is HQRetryEx ? ((HQRetryEx)p_innerException).NTimes : int.MaxValue, p_innerException) { }
        public HQRetryEx(int p_nTimes = int.MaxValue, Exception p_innerException = null)
            : base("This situation should be retried " + (p_nTimes == int.MaxValue ? null : p_nTimes + " times"), p_innerException)
        {
            NTimes = p_nTimes;
        }
    }

    /// <summary> This interface allows access to some operations of DBManager without
    /// requiring a complete, initialized DBManager instance. This facilitates
    /// alternative implementations of these operations.
    /// All operations of INonBlockingDbInfo must be non-blocking even if there were no db connection yet.
    /// </summary>
    public interface INonBlockingDbInfo
    {
        bool WasRemoteConnectionAlive { get; }
    }

    public static partial class DBUtils
    {
        internal static string GetSqlCeExceptionMessage(Exception e)
        {
            #if ReallyUseSqlCe
            var sqlce = e as System.Data.SqlServerCe.SqlCeException;
            if (sqlce != null && 1 < sqlce.Errors.Count)
                return e.Message + " (" + Utils.Join("; ", sqlce.Errors.Cast<object>()
                    .Select((error, i) => "#" + i + " " + error)) + ")";
            #else
            return null;
            #endif
        }
    }
}


