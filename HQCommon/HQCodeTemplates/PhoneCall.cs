using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HQCodeTemplates
{
    public static partial class Tools
    {
        /// <summary> Example: MakePhoneCall((Caller)0, "+36705705467", "hello, world", p_nRepeat: 3); </summary>
        public static bool MakePhoneCall(Caller p_from, string p_to, string p_message, int p_nRepeat = 1)
        {
            var p = new PhoneCall { FromNumber = p_from, ToNumber = p_to, Message = p_message, NRepeatAll = p_nRepeat };
            return p.InitiateCall();
        }
    }

    // Only verified phone numbers can be used as a caller. Some of the following numbers are not verified yet.
    // The destination of the call is not limited.
    public enum Caller
    {
        GyantalUK = 0,      // 0 is the default
        Robin, RobinLL,
        LNemeth, Charmat, BLukucz
    }
    public static class CallerExtensions
    {
        public static string AsString(this Caller p_id)
        {
            Func<string, string> fix = (s) => s.StartsWith("+") ? s : ("+" + s);
            switch (p_id)
            {
                case Caller.GyantalUK: return PhoneCall.Fix(HQCommon.Utils.SmsNumberGyantalUK);
                case Caller.Robin:     return "+" + 36304571854L;   // Not affected by culture. ToString("G") is the default, which doesn't use thousand separators.
                case Caller.RobinLL:   return "+" + 3613399014u;    // -||-
                case Caller.LNemeth:   return PhoneCall.Fix(HQCommon.Utils.SmsNumberLNemeth);
                case Caller.Charmat:   return PhoneCall.Fix(HQCommon.Utils.SmsNumberCharmat);
                case Caller.BLukucz:   return PhoneCall.Fix(HQCommon.Utils.SmsNumberBLukucz);
                default: return null;
            }
        }
    }
    
    public class PhoneCall
    {
        public Caller FromNumber = Caller.GyantalUK;
        public string ToNumber;
        public string Message = "Default message";
        public string ResponseJson, Error;
        public int NRepeatAll = 1;

        /// <summary> Returns true when Twilio's server accepted our request, BEFORE the phone begins to ring!
        /// Returns false if the server rejected our request with error message, or .NET exception occurred </summary>
        public bool InitiateCall()
        {
            return InitiateCallAsync().Result;
        }
        public async System.Threading.Tasks.Task<bool> InitiateCallAsync()
        {
            string caller = CallerExtensions.AsString(FromNumber);
            if (String.IsNullOrEmpty(caller))
                throw new ArgumentException(FromNumber.ToString(), "FromNumber");
            if (String.IsNullOrEmpty(ToNumber))
                throw new ArgumentException(ToNumber ?? "null", "ToNumber");

            // Twilio docs/examples in Azure documentation:
            // http://www.windowsazure.com/en-us/documentation/articles/twilio-dotnet-how-to-use-for-voice-sms/
            // Official API docs:
            // https://www.twilio.com/docs/api/rest

            string url = null;
            if (NRepeatAll <= 1)
                url = "http://twimlets.com/message?Message%5B0%5D=" + Uri.EscapeDataString(Message);
            else
            {
                var say = new System.Xml.XmlDocument().CreateElement("Say");
                say.InnerText = Message;
                string xml = "<Response>" + String.Join("<Pause length=\"2\"/>", Enumerable.Repeat(say.OuterXml, NRepeatAll)) + "</Response>";
                url = "http://twimlets.com/echo?Twiml=" + Uri.EscapeDataString(xml);
            }

            var client = new System.Net.Http.HttpClient();  // System.Net.Http.dll

            // Userid/password are encoded here to make them harder to harvest from program binaries, source code or emails.
            Func<ulong,ulong,string> decode = (u,v) => u.ToString("x16")+v.ToString("x16");
            string sid = "AC" + decode(13660173446429173547ul, 151777637205349229ul), token = decode(16719913942808467888ul, 11897223151226315429ul);
            // string sid = "AC" + decode(0x105ded1ced7efe8eUL, 0x23d85991f30c9829UL), token = decode(0x2ff76464f6df0282UL, 0x43202f59b95e2108UL); // test credentials  See www.twilio.com/docs/api/rest/test-credentials
            client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Basic",
                Convert.ToBase64String(Encoding.ASCII.GetBytes(sid + ":" + token)));
            try
            {
                System.Net.Http.HttpResponseMessage response = await client.PostAsync(
                    "https://api.twilio.com/2010-04-01/Accounts/" + sid + "/Calls.json",  // could be .csv as well, see https://www.twilio.com/docs/api/rest/tips
                    new System.Net.Http.FormUrlEncodedContent(new Dictionary<string, string>() {
                    { "From", Fix(caller) },
                    { "To",   Fix(ToNumber) },
                    { "Method", "GET" },
                    { "Url",  url }
                }));
                string resp = await response.Content.ReadAsStringAsync();
                if (resp.StartsWith("{\"sid\":"))
                    ResponseJson = resp;
                else
                    Error = resp;
            }
            catch (Exception e)
            {
                Error = HQCommon.Utils.ToStringWithoutStackTrace(e);
            }
            return Error == null;
        }
        public static string Fix(string p_number)
        {
            return (String.IsNullOrEmpty(p_number) || p_number[0] == '+' || p_number.StartsWith("00")) ? p_number : ("+" + p_number);
        }
    }
}
