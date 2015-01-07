using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Net;
using System.Text.RegularExpressions;
using System.Threading;

namespace HQCommon
{
    public class DownloadHelper
    {
        static Regex g_filenameFinder;

        readonly bool[] m_isDownloadSuccessful = { false };
        string m_remoteFolder;

        public DownloadHelper()
        {
            RemoteFolder = SqOfflineDbFilesUrl;
            RemoteTimeZoneID = SqOfflineDbFilesUrlTimeZoneID;
        }

        /// <summary> Empty string means disable downloading files from the server
        /// (designed for server tools like updateOfflineFiles) </summary>
        public static string SqOfflineDbFilesUrl
        {
            get
            {
                string path = Properties.Settings.Default.SqOfflineDbFilesUrlPathOnly;
                return String.IsNullOrEmpty(path) ? String.Empty : "http://" + Utils.SQWebServerDomain + path;
            }
        }

        public static TimeZoneID SqOfflineDbFilesUrlTimeZoneID
        {
            get { return Utils.ConvertTo<TimeZoneID>(Properties.Settings.Default.SqOfflineDbFilesUrlTimeZoneID); }
        }

        public TimeZoneID RemoteTimeZoneID { get; set; }

        public string RemoteFolder
        {
            get { return m_remoteFolder; }
            set
            {   // must end with slash, or be empty
                if (!String.IsNullOrEmpty(value) && value[value.Length - 1] != '/')
                    throw new ArgumentException("RemoteFolder must end with /");
                m_remoteFolder = value;
            }
        }

        /// <summary> Downloads the file list of the remote folder and parses it.
		/// Returns all the filenames (with actual extension) and DateTimes (converted to UTC).
        /// </summary>
		public IList<KeyValuePair<string, DateTime>> GetServerUtcFilesTimes()
		{
            var result = new List<KeyValuePair<string, DateTime>>();
            if (String.IsNullOrEmpty(RemoteFolder))
                return result;
            string webpageData = null;
            Utils.Logger.Info("Downloading file list from {0}", RemoteFolder);
            try
            {
                HttpStatusCode status = WebRequestHelper.GetPageData(RemoteFolder, out webpageData);
                if (status == HttpStatusCode.OK)
                {
                    webpageData = webpageData.Remove(0, webpageData.IndexOf("<br><br>") + "<br><br>".Length);
                    webpageData = webpageData.Remove(Math.Max(webpageData.LastIndexOf("<br>"),0));
                    string[] separator = { "<br>" };
                    string[] lines = webpageData.Split(separator, StringSplitOptions.RemoveEmptyEntries);
                    if (g_filenameFinder == null)
                        g_filenameFinder = new Regex(">([^<]*)</A>");
                    foreach (string line in lines)
                    {
                        // Sample: { 4/13/2010  5:13 PM     14488476 <A HREF="/wwwServer/OfflineDBFiles/xy">xy</A>}
                        // A[M] or P[M]
                        webpageData = line;     // for log message in case of exception
                        string filename = g_filenameFinder.Match(line).Groups[1].Value;
                        DateTime time;
                        DateTime.TryParse(line.Substring(0, line.IndexOf("M") + 1), out time);
                        result.Add(new KeyValuePair<string, DateTime>(filename, time.ToUtc(RemoteTimeZoneID)));
                    }
                }
            }
            catch (Exception e)
            {
                Utils.Logger.PrintException(e, false, "catched in {0}{1}", Utils.GetCurrentMethodName(),
                    webpageData != null ? " while processing: " + webpageData : null);
            }
            return result;
		}

        // The following code was working at 2013-07-18. It was commented out because was not needed anywhere.
        ///// <summary> Downloads the remote packed file and unpacks it into the given directory.
        ///// The thread is blocked until the download and unpack completes.
        ///// Returns true if both downloading and unpacking was successful. </summary>
        ///// <param name="p_remoteFileName">the remote file, e.g. "HedgeQuant.sdf.7z" </param>
        ///// <param name="p_localDirName">the local directory, e.g. the working directory of the application</param>
        //public bool DownloadAndUnpackRemoteFile(string p_remoteFileName, string p_localDirName,
        //    Action<string> p_beforeDeletingDst, Action<string> p_afterUnpackingToDst)
        //{
        //    if (String.IsNullOrEmpty(RemoteFolder))
        //        return false;
        //    string url = RemoteFolder + p_remoteFileName;
        //    WebClient webClient = new WebClient();
        //    webClient.Proxy = null;
        //    webClient.Credentials = new NetworkCredential("lnemeth", "L667nemeth");
        //
        //    //webClient.DownloadProgressChanged += new DownloadProgressChangedEventHandler(wc_DownloadProgressChanged);
        //    webClient.DownloadFileCompleted += new AsyncCompletedEventHandler(DownloadFileCompleted);
        //    string downloadedFullPath = Path.Combine(p_localDirName, p_remoteFileName);
        //
        //    lock (m_isDownloadSuccessful)
        //    {
        //        webClient.DownloadFileAsync(new Uri(url), downloadedFullPath);
        //        Utils.Logger.Info("Downloading " + url);
        //        m_isDownloadSuccessful[0] = false;
        //        Monitor.Wait(m_isDownloadSuccessful);
        //    }
        //    return m_isDownloadSuccessful[0]
        //        && UnpackDownloadedFile(downloadedFullPath, p_localDirName, p_beforeDeletingDst, p_afterUnpackingToDst);
        //}
        //void DownloadFileCompleted(object p_sender, AsyncCompletedEventArgs e)
        //{
        //    if (e != null && e.Error != null)
        //    {
        //        var we = e.Error as WebException;
        //        if (we != null)
        //            Utils.Logger.PrintException(we, false, "catched at {0}, ResponseUri={1}",
        //                Utils.GetCurrentMethodName(), we.Response == null ? "null" : we.Response.ResponseUri.ToString());
        //        else
        //            Utils.Logger.PrintException(we, false, "catched at {0}", Utils.GetCurrentMethodName());
        //    }
        //    lock (m_isDownloadSuccessful)
        //    {
        //        m_isDownloadSuccessful[0] = (e == null || e.Error == null);
        //        Monitor.PulseAll(m_isDownloadSuccessful);
        //    }
        //}

        /// <summary> p_downloadedFile must be an archive supported by 7-Zip.
        /// It may contain multiple files, which are to be extracted to p_dstDir.
        /// Files with no extension will be recompressed with GZip, adding .gz extension.
        /// p_beforeDeletingDst() is called for files that already exists in p_dstDir.
        /// p_afterUnpackingToDst() is called after unpacking each file.
        /// </summary>
		public static bool UnpackDownloadedFile(string p_downloadedFile, string p_dstDir,
            Action<string> p_beforeDeletingDst, Action<string> p_afterUnpackingToDst)
		{
            bool result = false;
            using (DirectoryGuard tmp = Utils.CreateTmpDir(p_dstDir))
                if (Utils.Run7Zip(String.Format("e -y -- \"{0}\"", p_downloadedFile), tmp.Dir.FullName, null))
                    foreach (FileInfo fi in tmp.Dir.GetFiles())
                    {
                        string dstFn = Path.Combine(p_dstDir, fi.Name);
                        bool isConversionToGzNeeded = String.IsNullOrEmpty(fi.Extension);
                        if (isConversionToGzNeeded)
                            dstFn += ".gz";
                        if (File.Exists(dstFn))
                        {
                            if (p_beforeDeletingDst != null)
                                p_beforeDeletingDst(dstFn);
                            File.Delete(dstFn);
                        }
                        if (!isConversionToGzNeeded)
                        {
                            fi.MoveTo(dstFn);
                            result = true;
                        }
                        else if (result = Utils.Run7Zip(String.Format(
                            @"a -tgzip -mx=5 -mpass=1 -- ""{0}"" ""{1}""", dstFn, fi.Name),
                            tmp.Dir.FullName, null))
                        {
                            new FileInfo(dstFn).LastWriteTimeUtc = fi.LastWriteTimeUtc;
                        }
                        if (p_afterUnpackingToDst != null)
                            p_afterUnpackingToDst(dstFn);
                    }
            return result;
		}
    }
}