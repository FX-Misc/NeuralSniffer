// Declarations/code shared by HQCommon and HQCommonPublic

using System;
using System.Data.SqlClient;
using System.Linq;


namespace HQCommon
{
    public enum AssetType : sbyte    // According to dbo.AssetType
    {
        HardCash = 1,
        /// <summary> Important: the SubTableID of this asset type may identify 
        /// either a stock of a company or a ticket of a fund. 
        /// Funds are handled like companies, fund-tickets are handled as stocks. </summary>
        Stock,          // 2
        Futures,        // 3
        Bond,
        Option,         // 5
        Commodity,
        RealEstate,
        BenchmarkIndex, // 8
        Unknown = 0
        // Don't use values below -16 or above +15. Exploited at AssetIdInt32Bits.
        // Don't use sparse values. Exploited at g_assetTypeMin and all related routines.
    }

    public static partial class DBUtils
    {
        /// <summary> Returns a date-only string (without quotes) in a format which is suitable 
        /// in SQL commands.
        /// IMPORTANT: the current format of the string ("05/24/2008") should be changeable
        /// when the SQL server changes, so don't use this method for other purposes
        /// than SQL communication!
        /// </summary>
        // TODO: a szerver megerti a YYYYMMDD formatumot is! Ez annyibol jobb h. rovidebb
        // A help-ben igy talalod meg: 'Unseparated String Format'.  {0:yyyyMMdd}
        // http://msdn.microsoft.com/en-us/library/ms180878.aspx#UnseparatedStringFormat
        public static string Date2Str(DateTime p_date)
        {
            return p_date.ToString("d", System.Globalization.CultureInfo.InvariantCulture);
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
                case 0: return IsSqlExceptionToRetry(p_e.InnerException as SqlException, p_tryIdx);

                //"Timeout expired.  The timeout period elapsed prior to completion of the operation or the server is not responding." (Class=11, Number=-2, State=0, LineNumber=0)
                //http://msdn.microsoft.com/en-us/library/cc645611.aspx
                case -2: return true;

                //This should be retried for SqlAzure only! (which cannot be examined here)
                //"The instance of SQL Server you attempted to connect to does not support encryption." (Class=20, Number=20, State=0, LineNumber=0)
                case 20: return true;

                //"A transport-level error has occurred when receiving results from the server. (provider: TCP Provider, error: 0 - The specified network name is no longer available.) (Class=20, Number=64, State=0, LineNumber=0)" (Win32 error)
                // This can occur with Class=16, too, see notes_done.txt#110620.1
                case 64: return (@class >= 16);

                //"A transport-level error has occurred when receiving results from the server. (provider: TCP Provider, error: 0 - The semaphore timeout period has expired.) (Class=20, Number=121, State=0, LineNumber=0)"
                //http://msdn.microsoft.com/en-us/library/ms681382.aspx   (Win32 error)
                case 121:
                    return (@class >= 16);      // class==16 was observed with OPENROWSET(), see note#110620.1/2012-01-23

                //"Invalid object name '...'." Experienced with OPENROWSET(), see note#110620.1/2012-01-30
                case 208:
                    return (@class == 16 && 0 < p_tryIdx);

                // "TCP Provider: Timeout error [258]" (Class=16, Number=258, State=1, LineNumber=0) (Win32 error: WAIT_TIMEOUT)
                // Experienced with OPENROWSET(), see note#110620.1/2012-12-31, http://msdn.microsoft.com/en-us/library/ms681382.aspx
                case 258:
                    return (@class == 16);

                //"Transaction (Process ID %d) was deadlocked on %.*ls resources with another process and has been chosen as the deadlock victim. Rerun the transaction."
                //http://msdn.microsoft.com/en-us/library/aa337376.aspx
                case 1205: return true;

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
                case 233: return true;

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

}

#region Sensitive data access
namespace HQCommonPublic
{
    /// <summary> .exe.config settings that may be used in HQCommonPublic code
    /// without specifying "factory default" value there (should be provided elsewhere) </summary>
    public enum ExeCfgSettings
    {
        EmailAddressCharmat,
        EmailAddressJeanCharmat,
        EmailAddressCharmat2,
        EmailAddressGyantal,
        EmailAddressRobin,
        EmailAddressLNemeth,
        EmailAddressBLukucz,
        SmsNumberGyantalHU,
        SmsNumberGyantalUK,
        SmsNumberCharmat,
        SmsNumberLNemeth,
        SmsNumberBLukucz,
        SmsNumberRobin,
        TwilioSid,
        TwilioToken,
        ServerHedgeQuantConnectionString,
        SmtpUsername,   // has different name in the .exe.config when HQCommon.dll's g_exeCfgGetter is used: <add key="HQEmailSettings" value="UserName=..."/>
        SmtpPassword,   // has different name in the .exe.config when HQCommon.dll's g_exeCfgGetter is used: <add key="HQEmailSettings" value="Password=..."/>
        SqlNTryDefault
    }
    public static class ExeCfgSettingsEx
    {
        public static string Read(this ExeCfgSettings p_cfg)
        {
            string key = p_cfg.ToString(); long dummy;
            if (long.TryParse(key, out dummy)) return null; // invalid enum value
            Func<string, string> f = g_exeCfgGetter;
            return (f != null) ? f(key) : HQCommon.Utils.ReadFromExeConfig(key);
        }
        /// <summary> Must return null if the named setting (=input string) is not found </summary>
        public static Func<string, string> g_exeCfgGetter;
    }
}

namespace HQCommon
{
    public static partial class Utils
    {
        public static string ReadFromExeConfig(string p_key)
        {
            if (p_key == null)
                return null;
            if (p_key.IndexOf("ConnectionString", StringComparison.OrdinalIgnoreCase) < 0)
                return System.Configuration.ConfigurationManager.AppSettings[p_key]; // returns null if not found
            foreach (System.Configuration.ConnectionStringSettings cs in System.Configuration.ConfigurationManager.ConnectionStrings)
            {
                string name = cs.Name;
                if (name == p_key || (p_key.Length + 1 < name.Length && name.EndsWith(p_key) && name[name.Length - p_key.Length - 1] == '.'))
                {
                    name = cs.ConnectionString;  //  ↓ in these cases ↓  it's just a placeholder so don't use (cf. j.mp/11XOfhL "if Windows Azure ... cannot find a connection string with a matching name..." )
                    return (String.IsNullOrEmpty(name) || name.StartsWith("/*")) ? null : name;
                }
            }
            return null;
        }
        public static string Read(this HQCommonPublic.ExeCfgSettings p_cfg) { return HQCommonPublic.ExeCfgSettingsEx.Read(p_cfg); }
    }
}
#endregion

#region Miscellaneous utility methods
namespace HQCommon
{
    public static partial class Utils
    {
        public static string ToStringWithoutStackTrace(this Exception e)
        {
            string s = (e == null ? null : e.ToString()) ?? String.Empty;
            return s.Substring(0, Math.Min(s.Length, s.IndexOf("\n   at ") & int.MaxValue));
        }
    }
}
#endregion
