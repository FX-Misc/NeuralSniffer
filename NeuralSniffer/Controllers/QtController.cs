using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Web.Http;

// http://hqacompute.cloudapp.net/q/rtp?s=VXX,^VIX,^VXV,^GSPC,XIV&f=l&o=csv
// http://localhost:52174//rtp?s=VXX,^VIX,^VXV,^GSPC,XIV&f=l&o=csv
namespace NeuralSniffer.Controllers
{
    // Qt = QuickTester
    public class QtController : ApiController       
    {
        // if I have Get(), I cannot have GetAllRtp(), as it will be Duplicate resolve possibility and I got an exception.
        public HttpResponseMessage Get()
        {
            var response = this.Request.CreateResponse(HttpStatusCode.OK);
            try
            {
                string uriQuery = this.Url.Request.RequestUri.Query;    // "?s=VXX,XIV,^vix&f=ab&o=csv" from the URL http://localhost:58213/api/rtp?s=VXX,XIV,^vix&f=ab&o=csv

                if (uriQuery.Length > 8192)
                {//When you try to pass a string longer than 8192 charachters, a faultException will be thrown. There is a solution, but I don't want

                    response.Content = new StringContent(@"{ ""Message"":  ""Error caught by WebApi Get():: uriQuery is longer than 8192: we don't process that. Uri: " + uriQuery + @""" }", Encoding.UTF8, "application/json");
                    return response;
                }

                //ChannelFactory<IStringReverser> httpFactory = new ChannelFactory<IStringReverser>(new BasicHttpBinding(), new EndpointAddress("http://localhost:8000/Reverse"));
                //IStringReverser httpProxy = httpFactory.CreateChannel();
                //string resHttp = httpProxy.ReverseString(uriQuery);

                //ChannelFactory<IWebVBroker> pipeFactory = new ChannelFactory<IWebVBroker>(new NetNamedPipeBinding(), new EndpointAddress("net.pipe://localhost/RealTimePrice"));
                //IWebVBroker pipeProxy = pipeFactory.CreateChannel();
                //string jsonString = pipeProxy.Execute(uriQuery);

                string jsonString = "Bela";


                //string jsonString = @"[{""symbol"": """ + uriQuery + @""", ""Ask"": 41.2, ""Bid"", 41.3 }, {""symbol"": ""XIV"", ""Ask"": 34.5, ""Bid"", 34.6 }]";
                response.Content = new StringContent(jsonString, Encoding.UTF8, "application/json");
                return response;
            }
            catch (Exception e)
            {
                response.Content = new StringContent(@"{ ""Message"":  ""Exception caught by WebApi Get(): " + e.Message + @""" }", Encoding.UTF8, "application/json");
                return response;
            }
        }
    }
}
