using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Net;
using System.IO;
using System.IO.Compression;
using System.Globalization;
using System.Threading;

namespace HQCommon
{
    public static class WebRequestHelper
    {
        /// <summary> The string argument is the URI </summary>
        public static event Action<string> BeforeInternetAccessEvent;

        public static HttpStatusCode GetPageData(string p_uri, System.Net.CookieContainer p_cookieContainer, out string p_pageData)
        {
            return GetPageData(p_uri, p_cookieContainer, out p_pageData, null);
        }

        public static HttpStatusCode GetPageData(string p_uri, out string p_pageData)
        {
            return GetPageData(p_uri, null, out p_pageData);
        }

        public static HttpStatusCode GetPageData(string p_uri, int p_nTimesToTry, int p_sleepMsec, out string p_pageData)
        {
            return GetPageData(new ReqParams { Uri = p_uri, OverrideProxy = true, ContentType = null },
                out p_pageData, p_nTimesToTry, p_sleepMsec);
        }

        public static HttpStatusCode GetPageData(string p_uri, CookieContainer p_cookieContainer, out string p_pageData, object p_credential)
        {
            return GetPageData(new ReqParams {
                Uri = p_uri, CookieContainer = p_cookieContainer, Credential = p_credential, OverrideProxy = true, ContentType = null
            }, out p_pageData);
        }

        public class ReqParams
        {
            public string           Uri;
            public CookieContainer  CookieContainer;
            public object           Credential;
            public bool             SimulateAjax;
            public bool             AskForJSON;
            public bool             AllowAutoRedirect;
            public bool             OverrideProxy;
            public bool             DisableGzip;
            public string           Referer;
            public string           PostData;
            public string           ContentType = "application/x-www-form-urlencoded";
            public MemoryStream     ResponseBinaryData;
            public WebHeaderCollection ResponseHeaders;
            public Action<HttpWebRequest>   AdditionalSettings;
            public CancellationToken Cancellation;
            public int              TimeoutMsec = Timeout.Infinite;
            /// <summary> This is an out-parameter: returned to the caller </summary>
            public QuicklyClearableList<Exception> Errors;
            //internal ReqParams ChainBefore(Action<HttpWebRequest> p_action)
            //{
            //    AdditionalSettings = (AdditionalSettings != null) ? p_action + AdditionalSettings : p_action;
            //    return this;
            //}
        }

        static HttpWebRequest CreateRequest(ReqParams p_params)
        {
            // create the web request
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(p_params.Uri);
            if (p_params.CookieContainer != null)
                request.CookieContainer = p_params.CookieContainer;
            else
                request.CookieContainer = new CookieContainer();    // we need a CookieContainer, without it: WebException: too many automatic redirections were attempted. For finance.yahoo.com

            if (p_params.Credential != null)
                if (p_params.Credential is NetworkCredential)
                    request.Credentials = (NetworkCredential)p_params.Credential;
                else if (p_params.Credential is CredentialCache)
                {
                    request.UseDefaultCredentials = false;
                    request.PreAuthenticate = true;
                    request.Credentials = (CredentialCache)p_params.Credential;
                }

            request.AllowAutoRedirect = p_params.AllowAutoRedirect;

            if (p_params.ContentType != null)
                request.ContentType = p_params.ContentType;

            if (p_params.AskForJSON)
                request.Accept = "application/json, text/javascript, */*; q=0.01";

            if (p_params.SimulateAjax)
                request.Headers.Add("X-Requested-With", "XMLHttpRequest");

            if (p_params.Referer != null)
                request.Referer = p_params.Referer;

            // disable the proxy?
            //if (this.m_noProxy)
            //{
            if (p_params.OverrideProxy)
                request.Proxy = new WebProxy();
            //    req.ProtocolVersion = HttpVersion.Version10; // default is 1.1
            //}

            if (!p_params.DisableGzip)
            {
                request.Headers.Add(HttpRequestHeader.AcceptEncoding, "gzip, deflate");
                request.AutomaticDecompression = DecompressionMethods.Deflate | DecompressionMethods.GZip;
            }

            if (p_params.AdditionalSettings != null)
                p_params.AdditionalSettings(request);

            p_params.Errors.Clear();
            if (p_params.PostData != null)
            {
                request.ContentLength = p_params.PostData.Length;
                request.Method = "POST";
                //request.Referer = "http://www.navellier.com/tools_research/log_in.aspx";
                Stream requestStream = null;
                try
                {
                    requestStream = request.GetRequestStream();
                    byte[] postBytes = Encoding.ASCII.GetBytes(p_params.PostData);
                    requestStream.Write(postBytes, 0, postBytes.Length);
                }
                catch (Exception e) { p_params.Errors.Add(e); }
                finally
                {
                    if (requestStream != null)
                        requestStream.Close();
                }
            }
            else
            {
                request.Method = "GET";
            }

            return request;
        }

        public static HttpStatusCode GetPageData(ReqParams p_params, out string p_pageData)
        {
            KeyValuePair<HttpStatusCode, string> kv = GetPageDataAsync(p_params).Result;
            p_pageData = kv.Value;
            return kv.Key;
        }

        public static async System.Threading.Tasks.Task<KeyValuePair<HttpStatusCode, string>> GetPageDataAsync(ReqParams p_params)
        {
            HttpStatusCode status = (HttpStatusCode)(-2);
            HttpWebResponse resp = null;
            CancellationTokenSource timeout = null;
            var ctr = default(CancellationTokenRegistration);

            if (0 <= p_params.TimeoutMsec)
            {
                timeout = new CancellationTokenSource();
                timeout.CancelAfter(p_params.TimeoutMsec);
                Utils.CombineCT(ref p_params.Cancellation, timeout.Token);
            }
            // Do not care about ApplicationState.Token if the current thread executes Exit().
            // This allows using WebRequestHelper during Exit().
            if (!ApplicationState.IsExiting || ApplicationState.IsOtherThreadExiting)
                Utils.CombineCT(ref p_params.Cancellation, ApplicationState.Token);

            // initialize the out param (in case of error)
            string pageData = "";
            try
            {
                HttpWebRequest request = CreateRequest(p_params);

                // make the connection
                try
                {
                    System.Threading.Tasks.Task<WebResponse> asyncResponse = request.GetResponseAsync();
                    // Call Abort() when p_params.Cancellation.IsCancellationRequested==true (if already true, call it now)
                    ctr  = p_params.Cancellation.Register(req => ((HttpWebRequest)req).Abort(), request);
                    // HACK: ConfigureAwait() is needed to avoid deadlocking the UI thread if that calls this method
                    // (indirectly) in /noWPF mode. See email#5360b08d
                    resp = (HttpWebResponse)await asyncResponse.ConfigureAwait(continueOnCapturedContext: false);
                }
                catch (Exception e) {
                    p_params.Errors.Add(e);
                }
                if (resp == null)
                    resp = (HttpWebResponse)request.GetResponse();

                if (resp != null)
                {
                    p_params.ResponseHeaders = resp.Headers;

                    // get the page data
                    Stream r = resp.GetResponseStream();
                    if (p_params.ResponseBinaryData != null)
                        r.CopyTo(p_params.ResponseBinaryData);
                    else using (var sr = new StreamReader(r))
                        pageData = sr.ReadToEnd();

                    // get the status code (should be 200)
                    status = resp.StatusCode;
                }
            }
            catch (WebException e)
            {
                //string str = e.Status.ToString();

                resp = e.Response as HttpWebResponse;
                if (null != resp)
                {
                    // get the failure code from the response
                    status = resp.StatusCode;
                    //str += status;
                }
                else
                {
                    status = (HttpStatusCode)(-1);  // generic connection error
                }
                p_params.Errors.Add(e);
            }
            catch (Exception e2)
            {
                status = (HttpStatusCode)(-2);
                p_params.Errors.Add(e2);
            }
            finally
            {
                ctr.Dispose();
                Utils.DisposeAndNull(ref timeout);
                // close every HttpWebResponse Or the 3rd response will Time out with an exception
                if (resp != null)
                {
                    resp.Close();
                }
            }

            return new KeyValuePair<HttpStatusCode, string>(status, pageData);
        }

        public static HttpStatusCode GetPageData(ReqParams p_params, out string p_pageData, int p_nTimesToTry, int p_sleepMsec)
        {
            if (0 < p_nTimesToTry)
            {
                IEnumerable<int> waits = (1 < p_nTimesToTry) ? Enumerable.Repeat(p_sleepMsec, p_nTimesToTry - 1) : null;
                if (1 < p_nTimesToTry && 4 <= p_sleepMsec)
                    waits = waits.Select(x => x - (x >> 2) + new Random().Next(x >> 1));  // randomize sleep times: [0.75..1.25)*p_sleepMsec
                return GetPageData(p_params, out p_pageData, waits);
            }
            p_pageData = String.Empty;
            return HttpStatusCode.NotFound;
        }

        public static HttpStatusCode GetPageData(ReqParams p_params, out string p_pageData, IEnumerable<int> p_msBetweenRetries)
        {
            BeforeInternetAccessEvent.Fire(p_params.Uri);
            using (var it = (p_msBetweenRetries == null) ? null : p_msBetweenRetries.GetEnumerator())
                while (true)
                {
                    p_pageData = "";
                    HttpStatusCode statusCode = WebRequestHelper.GetPageData(p_params, out p_pageData);
                    if (statusCode == HttpStatusCode.OK || statusCode == HttpStatusCode.Found
                        || it == null || !it.MoveNext())
                        return statusCode;
                    Thread.Sleep(it.Current);
                }
        }

        public static HttpStatusCode GetPageData(string p_uri, System.Net.CookieContainer p_cookieContainer, int p_nTimesToTry, int p_sleepMsec, out string p_pageData, object p_credential = null)
        {
            return GetPageData(new ReqParams { Uri = p_uri, CookieContainer = p_cookieContainer, Credential = p_credential },
                out p_pageData, p_nTimesToTry, p_sleepMsec);
        }

        public static HttpStatusCode GetPageData(string p_uri, string p_postData, string p_referer, int p_nTimesToTry, int p_sleepMsec, ref System.Net.CookieContainer p_cookieContainer, out string p_pageData)
        {
            return GetPageData(new ReqParams {
                Uri = p_uri, PostData = p_postData, Referer = p_referer, CookieContainer = p_cookieContainer
            }, out p_pageData, p_nTimesToTry, p_sleepMsec);
        }

        public static HttpStatusCode GetBinaryData(string p_uri, System.Net.CookieContainer p_cookieContainer, out byte[] p_binaryData)
        {
            string dummy;
            var buffer = new MemoryStream();
            HttpStatusCode status = GetPageData(new ReqParams {
                Uri = p_uri, CookieContainer = p_cookieContainer, ContentType = null, ResponseBinaryData = buffer
            }, out dummy);
            p_binaryData = buffer.ToArray();
            return status;
        }
    }

    public class QueryStringBuilder : List<KeyValuePair<string, object>>
    {
        public void Add(string name, object value)
        {
            Add(new KeyValuePair<string, object>(name, value));
        }

        public override string ToString()
        {
            return String.Join("&", this.Select(kv => String.Concat(
                System.Web.HttpUtility.UrlEncode(kv.Key), "=", System.Web.HttpUtility.UrlEncode(kv.Value.ToString())
            )));
        }
    }
}
