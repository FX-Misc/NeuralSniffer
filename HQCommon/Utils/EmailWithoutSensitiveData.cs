using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;

using System.Net.Mail;
using System.Xml;

namespace HQCommon
{

    public static partial class Utils
    {
        /// <summary> Equivalent to
        /// 
        /// 
        /// Utils.SafeSendEmail(p_toAddresses, p_subject, p_body, p_isBodyHtml, Utils.EmailSenderForUsers, p_attachments) </summary>
        public static bool SafeSendEmailToUsers(string p_toAddresses, string p_subject, string p_body, bool p_isBodyHtml = false,
            params HQAttachment [] p_attachments)
        {
            return SafeSendEmail(p_toAddresses, p_subject, p_body, p_isBodyHtml, EmailSenderForUsers, p_attachments);
        }

        /// <summary>
        /// Usage: Utils.SafeSendEmail
        /// ("gyantal@gmail.com;robin@archidata.hu", 
        ///                            "HQ Crawler: automatically generated", "body", false);
        /// </summary>
        public static bool SafeSendEmail(string p_toAddresses, string p_subject, string p_body, bool p_isBodyHtml = false)
        {
            return SafeSendEmail(p_toAddresses, p_subject, p_body, p_isBodyHtml, null, (HQAttachment[])null);
        }

        public static bool SafeSendEmail(string p_toAddresses, string p_subject, string p_body, bool p_isBodyHtml,
            string p_senderAddress = null, params HQAttachment[] p_attachments)
        {

            return SafeSendEmail(new HQEmail
            {
                Sender = p_senderAddress,
                ToAddresses = p_toAddresses,
                Subject = p_subject,
                Body = p_body,
                IsBodyHtml = p_isBodyHtml,
                Attachments = p_attachments
            }) == null;
        }

        public static Exception SafeSendEmail(HQEmail p_email, uint p_nAttempts = 0)
        {
            if (p_email == null)
                return null;
            if (p_nAttempts == 0)
            {
                // Number of attempts before passing the email to HQEmailDrWatson.
                // (Only used if not set by the caller of this function).
                // Doubled if AlternateSsl==true
                p_nAttempts = HQEmailSettings.Get("nAttempts", 7u, n => n > 0);
            }
            int SafeSendEmailRetryWaitMsec = -1, IncrementMsec = 0;
            var nextTry = default(DateTime);
            var exceptions = new QuicklyClearableList<Exception>();
            for (uint i = 0, iIncr = 1; (i++ >> 1) < p_nAttempts; i += iIncr)
            {
                bool enableSsl = HQEmailSettings.Get("EnableSsl", true) ^ ((i & 1) == 0);    // i starts from 1
                try
                {
                    p_email.Send(enableSsl);
                    return null;
                }
                catch (SmtpException e)
                {
                    // If p_nAttempts is large enough:
                    // i=1:    waitMsec := 60s/8  =  7.5sec  -┐
                    // i=2:    waitMsec := 60-7.5 = 52.5sec  -┴→ together: ~1min (i=1,2)
                    // i=3,4   ~ 1.5min
                    // i=5,6   ~ 2.0min
                    // ...
                    // i=15,16 ~ 4.5min -- this corresponds to p_nAttempts==8
                    // i=17,18 ~ 1.0min
                    // ...  In average: ≤ 2.75 min/attempt (= (1+1.5+..+4.5)/8 = 22min/8 )
                    if (SafeSendEmailRetryWaitMsec < 0)
                    {
                        SafeSendEmailRetryWaitMsec = HQEmailSettings.Get("RetryWaitMsec", 60 * 1000, t => t >= 0);
                        IncrementMsec = HQEmailSettings.Get("IncrementMsec", 30 * 1000, t => t >= 0);
                        iIncr = HQEmailSettings.Get("AlternateSsl", false) ? 0u : 1u;
                    }
                    int waitMsec = SafeSendEmailRetryWaitMsec + IncrementMsec * ((int)(i & 15) >> 1);
                    if (p_nAttempts <= ((i + iIncr) >> 1))
                        waitMsec = -1;
                    else if ((i & 1) == 0)
                        waitMsec = (int)Math.Max(0, (nextTry - DateTime.UtcNow).TotalMilliseconds);
                    else
                    {
                        nextTry = DateTime.UtcNow.AddMilliseconds(waitMsec);
                        waitMsec = (iIncr == 0) ? waitMsec >> 3 : waitMsec;
                    }

                    var _ = (waitMsec < 0) ? new { e = e.AddHqIncidentId(), sTrace = true, msg2 = "Not retrying" }
                        : new { e = e, sTrace = false, msg2 = Utils.FormatInvCult("Retrying after {0} secs", waitMsec / 1000.0) };
                    string msg = Logger.FormatExceptionMessage(_.e, _.sTrace, "in {0}, attempt {1}/{2} (EnableSsl={3}){4}{5}",
                        Utils.GetCurrentMethodName(), (i + 1) >> 1, p_nAttempts, enableSsl, Environment.NewLine, _.msg2);
                    exceptions.Add(e);

                    if (0 <= waitMsec)
                    {
                        Utils.Logger.Warning(msg);
                        if (ApplicationState.ApplicationExitThread == System.Threading.Thread.CurrentThread)
                            System.Threading.Thread.Sleep(waitMsec);                    // usual case: we are called from Exit()
                        else if (ApplicationState.SleepIfNotExiting(waitMsec, false))   // HQEmailDrWatson: stop waiting when application is about to stop
                            break;
                        continue;
                    }
                    Utils.Logger.Error(msg);
                    Utils.MarkLogged(e, _.sTrace);
                }
                catch (Exception e) // e.g. one of the attachment files have been deleted, or AuthenticationException during shutdown
                {
                    Utils.Logger.PrintException(e, true, "catched in " + Utils.GetCurrentMethodName());
                    exceptions.Add(e); Utils.MarkLogged(e, true);
                }
                break;
            }
            try
            {
                // release the log file because we're exiting -- necessary in IIS web applications, where the process may not exit.
                // It has to be done here because of the 'Utils.Logger.Error(msg)' above.
                if (ApplicationState.IsExiting)
                    Utils.Logger.LogFile = null;
                p_email.PassToGuard();
            }
            catch { }
            return Utils.SingleOrAggregateException(exceptions);
        }

        public static string EmailSenderForUsers
        {
            get { return Properties.Settings.Default.EmailSenderForUsers; }
            // The value can be "someName <address@server.net>". If someName contains non-alphanumeric chars,
            // base64+utf8 encoding is recommended: "=?UTF-8?B?base64encodedUTF8Bytes?= <address@server.net>"
            set { Properties.Settings.Default["EmailSenderForUsers"] = value; }
        }

        #region Hard-wired email addresses
        // Following properties must be named as EmailPropertyPfx + username
        public const string EmailPropertyNamePfx = "EmailAddress";

        public static string EmailAddressCharmat
        {
            get { return Properties.Settings.Default.EmailAddressCharmat; }
        }
        public static string EmailAddressJeanCharmat
        {
            get { return Properties.Settings.Default.EmailAddressJeanCharmat; }
        }
        public static string EmailAddressCharmat2
        {
            get { return Properties.Settings.Default.EmailAddressCharmat2; }
        }
        public static string EmailAddressGyantal
        {
            get { return Properties.Settings.Default.EmailAddressGyantal; }
        }
        public static string EmailAddressRobin
        {
            get { return Properties.Settings.Default.EmailAddressRobin; }
        }
        public static string EmailAddressLNemeth
        {
            get { return Properties.Settings.Default.EmailAddressLNemeth; }
        }
        public static string EmailAddressBLukucz
        {
            get { return Properties.Settings.Default.EmailAddressBLukucz; }
        }
        #endregion
    }

    public class HQEmail : IXmlPersistable
    {
        public string Sender;
        public string ToAddresses;
        public string Subject;
        public string Body;
        public bool IsBodyHtml;
        public HQAttachment[] Attachments;

        const string aSender = "From", aTo = "To", aSubject = "Subject", aIsBodyHtml = "IsBodyHtml";        // xml [a]ttributes
        const string tBody = "Body", tAttachment = "Attachment", tEmail = "HQEmail";                        // xml [t]ags
        public const string TimeoutMsec = "TimeoutMsec";

        internal void Send(bool p_enableSsl)
        {
            using (MailMessage message = new MailMessage())
            {
                foreach (string toAddress in ToAddresses.Split(new char[] { ';', ',' },
                    StringSplitOptions.RemoveEmptyEntries))
                {
                    string trimmed = toAddress.Trim();
                    if (0 < trimmed.Length)
                        message.To.Add(new System.Net.Mail.MailAddress(trimmed));
                }
                Body = Body ?? String.Empty;
                if (message.To.Count == 0)
                {
                    Utils.Logger.Warning("*** Warning: no addressee for email message '{0}':\n{1}",
                        Subject, Body.Substring(0, Math.Min(Body.Length, 256)));
                    return;
                }

                if (Attachments != null)
                    foreach (HQAttachment a in Attachments)
                        message.Attachments.Add(a.ToAttachment());

                string smtpServer = HQEmailSettings.Get("SmtpServer", "smtp.sendgrid.net");
                bool needSendgridWorkaround = smtpServer.Equals("smtp.sendgrid.net", StringComparison.OrdinalIgnoreCase);

                string s = Sender;               // arbitrary
                if (String.IsNullOrEmpty(s))
                    s = HQEmailSettings.Get<string>("Sender", null) ?? AppnameForDebug.ToEmailSender;
                else if (!s.Contains('@'))
                    s += Utils.RegExtract1(Utils.EmailSenderForUsers, "(@[-.0-9a-zA-Z]+)>?$");
                message.From = new System.Net.Mail.MailAddress(s);
                message.Subject = Subject ?? String.Empty;
                message.IsBodyHtml = IsBodyHtml;
                message.BodyTransferEncoding = System.Net.Mime.TransferEncoding.QuotedPrintable;

                if (needSendgridWorkaround)     // work-around for SendGrid's bug, see: http://j.mp/133GYML  http://goo.gl/UA6IT
                {
                    if (!IsBodyHtml)
                        message.BodyEncoding = System.Text.Encoding.UTF8;
                    Body = Body.Replace("\r\n", "\n");
                }
                message.Body = Body;

                SmtpClient smtpClient = new SmtpClient(smtpServer);
                smtpClient.Port = HQEmailSettings.Get("Port", 587);  // later: try 25, 465, 475, 587 if the default 587 doesn't work // Port: 465 or 587 see http://j.mp/rZ9l6I
                smtpClient.EnableSsl = p_enableSsl;    // to reduce SmtpExceptions (still remains some) ("The SMTP server requires a secure connection or the client was not authenticated")
                smtpClient.UseDefaultCredentials = false;   // do not use current windows username/pwd. (The default would be 'false', too)
                smtpClient.Credentials = new NetworkCredential(
                    HQEmailSettings.Get("UserName", "<TODO_GET_SendGridUserNameFromSQLDB>@azure.com"),
                    HQEmailSettings.Get("Password", "<TODO_GET_IT_FROM somewhere>")
                );
                smtpClient.Timeout = HQEmailSettings.Get(TimeoutMsec, 120000);     // default is 100 000 msec (but it was timed out once)

                // exceptions are handled in the caller SafeSendEmail()
                smtpClient.Send(message);
                Utils.Logger.Info("{0}: Email message was sent", Utils.GetCurrentMethodName());

                //if (new Random().Next(10) >= 4)
                //    throw new Exception("Testing exception handling in " + Utils.GetCurrentMethodName());
            }
        }

        #region IXmlPersistable Members

        public System.Xml.XmlElement Save(System.Xml.XmlElement p_element, ISettings p_context)
        {
            if (p_element == null)
                p_element = new XmlDocument().CreateElement(tEmail);
            var body = p_element.OwnerDocument.CreateElement(tBody);
            body.InnerText = Body;
            body.SetAttribute(aIsBodyHtml, IsBodyHtml ? "true" : "false");
            if (!String.IsNullOrEmpty(Sender))
                p_element.Extend(aSender, Sender);
            p_element.Extend(aTo, ToAddresses, aSubject, Subject, body);
            if (Attachments != null)
                foreach (HQAttachment a in Attachments)
                    p_element.AppendChildElement(tAttachment, a);
            return p_element;
        }

        public void Load(System.Xml.XmlElement p_element, ISettings p_context)
        {
            Sender = p_element.GetAttribute(aSender);
            ToAddresses = p_element.GetAttribute(aTo);
            Subject = p_element.GetAttribute(aSubject);
            foreach (XmlElement e in p_element.GetChildElements(tBody))
            {
                Body = e.InnerText;
                IsBodyHtml = XMLUtils.GetAttribute(e, aIsBodyHtml, false);
                break;
            }
            foreach (XmlElement e in p_element.GetChildElements(tAttachment))
            {
                var a = new HQAttachment();
                a.Load(e, null);
                Utils.AppendArray(ref Attachments, a);
            }
        }
        #endregion

        public virtual void PassToGuard()
        {
            try
            {
                string pickupFolder = GetPickupFolderOfGuard();
                if (String.IsNullOrEmpty(pickupFolder) || !Directory.Exists(pickupFolder))
                {
                    Utils.Logger.Error("Not passing email to guard because cannot find pickup folder ({0})", RegistryLocationOfGuard);
                    return;
                }
                string filename = Utils.FormatInvCult("{0:yyyy'-'MM'-'ddTHHmmss.fffffff'Z'}_{1}.xml",
                    DateTime.UtcNow, Path.GetFileNameWithoutExtension(Utils.GetExeName()));
                filename = Path.Combine(pickupFolder, filename);
                Utils.Logger.Verbose("Saving {0}", filename);
                Save(null, null).SaveNode(filename);
            }
            catch (Exception e)
            {
                Utils.Logger.PrintException(e, false, "catched in " + Utils.GetCurrentMethodName());
            }
        }

        public static KeyValuePair<string, string> RegistryLocationOfGuard
        {
            get
            {
                // NOTE: the following registry location is adjustable in the .exe.config, see HQEmailSettings below
                string loc = HQEmailSettings.Get("RegistryLocationOfGuard",
                        @"HKEY_LOCAL_MACHINE\SOFTWARE\Wow6432Node\HQEmailDrWatsonFolder",   // the last part means Value, not Key!
                        str => str.StartsWith("HKEY_", StringComparison.OrdinalIgnoreCase));
                // IMPORTANT: the above key is not visible for 32bit processes on a 64bit machine!
                // If you compile a .NET .exe with "Prefer-32 bit" build setting (or without <Prefer32Bit>false</Prefer32Bit>
                // in the VS2012 project file) then it will run as 32-bit (usually, as of .NET4.5).
                // This is why GetPickupFolderOfGuard() attempts to access the key with and without the "\Wow6432Node" part.
                int v = loc.LastIndexOf(@"\");
                return new KeyValuePair<string, string>(loc.Substring(0, v & ~(v >> 31)), loc.Substring(v + 1));
            }
        }

        /// <summary> The current implementation works from the registry </summary>
        public static string GetPickupFolderOfGuard()
        {
            if (g_pickupFolderOfGuard != null)  // allow for different implementation. Fixes a HQEmailDrWatson bug (svn#6885)
                return g_pickupFolderOfGuard;
            KeyValuePair<string, string> kv = RegistryLocationOfGuard;
            object val = Microsoft.Win32.Registry.GetValue(kv.Key, kv.Value, null);
            const string wow = @"\Wow6432Node"; int i;
            if (val == null && 0 <= (i = kv.Key.IndexOf(wow, StringComparison.OrdinalIgnoreCase)))
                val = Microsoft.Win32.Registry.GetValue(kv.Key.Remove(i, wow.Length), kv.Value, null);
            string pickupFolder = Utils.ToStringOrNull(val);
            if (String.IsNullOrEmpty(pickupFolder) || !Directory.Exists(pickupFolder))
                pickupFolder = null;
            return pickupFolder;
        }
        public static string g_pickupFolderOfGuard;

        /// <summary> Example of p_recipients: "charmat,JeanCharmat;gyantal,xy@wz.net"
        /// This function substitutes email addresses using the Utils.EmailAddres[Charmat|...] properties.
        /// Unknown/ambiguous property names will be omitted. Returns null for null/empty input only. </summary>
        public static string ParseRecipients(string p_recipients)
        {
            if (String.IsNullOrEmpty(p_recipients))
                return null;
            string[] dst = p_recipients.Split(';', ',');
            List<KeyValuePair<string, System.Reflection.PropertyInfo>> props = null;
            for (int i = dst.Length; --i >= 0; )
                if (!dst[i].Contains('@'))
                {
                    System.Threading.LazyInitializer.EnsureInitialized(ref props, () =>
                        (from p in typeof(Utils).GetProperties(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public)
                         where p.Name.StartsWith(Utils.EmailPropertyNamePfx) && p.PropertyType == typeof(string)
                         select new KeyValuePair<string, System.Reflection.PropertyInfo>(p.Name.Substring(Utils.EmailPropertyNamePfx.Length), p))
                         .ToList());
                    KeyValuePair<string, System.Reflection.PropertyInfo>[] kvs = props.Where(kv =>
                        String.Equals(kv.Key, dst[i], StringComparison.OrdinalIgnoreCase)).ToArray();
                    string @new = null;
                    if (kvs.Length == 1)
                        @new = (string)kvs[0].Value.GetValue(null, null);
                    else
                        Utils.Logger.Error("Error in {2}: recipient '{0}' is {1}! Ignored.", dst[i], kvs.Length == 0 ? "unknown" : "ambiguous",
                            Utils.GetCurrentMethodName());
                    dst[i] = String.IsNullOrEmpty(@new) ? null : @new;
                }
            return String.Join(";", dst.WhereNotNull());
        }

    } //~ HQEmail

    // Parses settings from .exe.config <appSettings><add key="HQEmailSettings" value="..."/></appSettings>
    // where the format of 'value' is "SmtpServer=smtp.gmail.com, Port=587, UserName=..., ..."
    // The list of available keys is not documented: look for uses of HQEmailSettings.Get(key,default)
    // (enough in this file) and make up the list for yourself.
    // IMPORTANT: none of the values (even the password!) may contain whitespace/comma,
    // because comma is delimiter and whitespaces are removed.
    //
    // old gmail account: <appSettings><add key="HQEmailSettings" value="Sender=hedgequantserver@gmail.com,SmtpServer=smtp.gmail.com,UserName=HedgeQuantServer@gmail.com,Password=SGVkZ2VRdWFudFNlcnZlcg"/></appSettings>
    //
    sealed class HQEmailSettings
    {
        static StringableSetting<Dictionary<string, string>> g_cfgParsed = new StringableSetting<Dictionary<string, string>>("HQEmailSettings")
        {
            ParserFuncWDef = (p_stringFromExeCfg, _, ss) =>
            {
                var result = ss.FactoryDefault = new Dictionary<string, string>();
                result.AddRange(Utils.ParseKeyValues<string>(Utils.ParseList<StringSegment>(p_stringFromExeCfg, ',', null)));
                return result;
            },
            ToStringFunc = (dict) => String.Join(",", dict.Select(kv => kv.Key + "=" + kv.Value))
        };
        public static T Get<T>(string p_key, T p_default, Func<T, bool> p_validator = null)
        {
            return Utils.Get(g_cfgParsed.Value, p_key).As(p_default, p_validator);
        }
        public static void Set<T>(string p_key, T p_value)    // does not store back to disk (.exe.config file)
        {
            if (p_value == null)
                g_cfgParsed.Value.Remove(p_key);
            else
                g_cfgParsed.Value[p_key] = p_value.ToString();
        }
    }

    public class HQAttachment : IXmlPersistable
    {
        internal byte[] m_data;
        internal int m_length;
        internal string m_origFn, m_attachmentFn, m_contentId;
        public string m_mime;
        internal Attachment ToAttachment()
        {
            var result = new System.Net.Mail.Attachment(GetStream(), m_attachmentFn, m_mime) { ContentId = m_contentId };
            result.ContentDisposition.Inline = !String.IsNullOrEmpty(m_contentId);
            result.ContentDisposition.FileName = m_attachmentFn;
            return result;
        }

        public static HQAttachment Create(Stream p_data,
            Utils.Compression p_flags = Utils.Compression.Store | Utils.Compression.CloseSource,
            string p_filename = null, string p_contentId = null, bool p_isPlainText = false)
        {
            var result = new HQAttachment
            {
                m_attachmentFn = p_filename,
                m_contentId = p_contentId,
                m_mime = p_isPlainText ? "text/plain" : "application/octet-stream"
            };
            bool doClose = (p_flags & Utils.Compression._Closing) == Utils.Compression.CloseSource;
            try
            {
                var file = p_data as FileStream;
                result.m_origFn = (file != null) ? file.Name : null;
                if (String.IsNullOrEmpty(result.m_attachmentFn))
                    result.m_attachmentFn = (file != null) ? Path.GetFileName(result.m_origFn) : ("file" + Utils.GenerateHqIncidentId());

                if ((p_flags & Utils.Compression._Method) != Utils.Compression.Store || file == null)
                {
                    KeyValuePair<byte[], int> kv = Utils.Compress(p_data, p_flags);
                    result.m_data = kv.Key;
                    result.m_length = kv.Value;
                    doClose = false;
                    if ((p_flags & Utils.Compression._Method) == Utils.Compression.Gzip)
                        result.m_attachmentFn += ".gz";
                }
                switch (Path.GetExtension(result.m_attachmentFn).ToLower())
                {
                    case ".gz": result.m_mime = "application/x-gzip"; break;
                    case ".png": result.m_mime = "image/png"; break;
                    case ".jpg": result.m_mime = System.Net.Mime.MediaTypeNames.Image.Jpeg; break;
                    default: break;
                }
                return result;
            }
            finally
            {
                // Here we close the Stream even if its data is not copied (this occurs when it is a
                // FileStream and no compression is requested), because 'p_data' MUST NOT be passed
                // to the 'new Attachment()' ctor: the new 'Attachment' instance would close the
                // stream at the end of the first SendEmail attempt, even if retry is necessary.
                // Thus we store the filename and re-open the file in ToAttachment().
                if (doClose)
                    p_data.Close();
            }
        }

        Stream GetStream()
        {
            try
            {
                return (m_data != null) ? new MemoryStream(m_data, 0, m_length) : (Stream)File.OpenRead(m_origFn);
            }
            catch (Exception e)
            {
                return new MemoryStream(ConvertErrorMsgToData(e, Utils.GetCurrentMethodName()));
            }
        }
        static byte[] ConvertErrorMsgToData(Exception e, string p_methodName)
        {
            return new System.Text.UTF8Encoding(true).GetBytes(
                Logger.FormatExceptionMessage(e, false, "catched in " + p_methodName));
        }

        #region IXmlPersistable Members
        const string aAttachmentFn = "AttachmentFilename", aContentId = "ContentId", aMimeType = "MimeType";
        public System.Xml.XmlElement Save(System.Xml.XmlElement p_element, ISettings p_context)
        {
            p_element.SetAttribute(aAttachmentFn, m_attachmentFn);
            p_element.SetAttribute(aContentId, m_contentId);
            p_element.SetAttribute(aMimeType, m_mime);
            byte[] data = m_data;
            int len = m_length;
            if (data == null)
                try
                {
                    using (var stream = GetStream())
                    {
                        len = (int)Math.Min(stream.Length, int.MaxValue);
                        data = new byte[len];
                        len = stream.Read(data, 0, len);
                    }
                }
                catch (Exception e)
                {
                    data = ConvertErrorMsgToData(e, Utils.GetCurrentMethodName());
                    len = data.Length;
                }
            p_element.InnerText = Convert.ToBase64String(data, 0, len, Base64FormattingOptions.InsertLineBreaks);
            return p_element;
        }

        public void Load(System.Xml.XmlElement p_element, ISettings p_context)
        {
            m_attachmentFn = p_element.GetAttribute(aAttachmentFn);
            m_contentId = p_element.GetAttribute(aContentId);
            m_mime = p_element.GetAttribute(aMimeType);
            try
            {
                m_data = Convert.FromBase64String(p_element.InnerText);
            }
            catch (Exception e)
            {
                m_data = new System.Text.UTF8Encoding(true).GetBytes(
                    Logger.FormatExceptionMessage(e, false, "catched in " + Utils.GetCurrentMethodName()));
            }
            m_length = m_data.Length;
        }


        #endregion
    } //~ HQAttachment

}