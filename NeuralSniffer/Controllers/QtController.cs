using HQCodeTemplates;
using NeuralSniffer.Controllers.Strategies;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Http;

interface Strategies
{
    string GenerateQuickTesterResponse(string p_params);
}

// http://hqacompute.cloudapp.net/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d
// http://localhost:52174/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d
// http://neuralsniffer.azurewebsites.net/q/qt?jsonp=JSON_CALLBACK&strategy=LETFDiscrepancy1&ETFPairs=SRS-URE&rebalanceFrequency=5d
namespace NeuralSniffer.Controllers
{
    // Qt = QuickTester
    public class QtController : ApiController       
    {
        // if I have Get(), I cannot have GetAllRtp(), as it will be Duplicate resolve possibility and I got an exception.
        //// IIS can handle if the return is a Task lst, not a HttpActionResult. It is needed for async SQL examples from Robert
        public async Task<HttpResponseMessage> Get()
        {
            string jsonpCallback = null;
            var response = this.Request.CreateResponse(HttpStatusCode.OK);
            try
            {
                string uriQuery = this.Url.Request.RequestUri.Query;    // "?s=VXX,XIV,^vix&f=ab&o=csv" from the URL http://localhost:58213/api/rtp?s=VXX,XIV,^vix&f=ab&o=csv

                if (uriQuery.Length > 8192)
                {//When you try to pass a string longer than 8192 charachters, a faultException will be thrown. There is a solution, but I don't want
                    throw new Exception("Error caught by WebApi Get():: uriQuery is longer than 8192: we don't process that. Uri: " + uriQuery);
                }

                uriQuery = uriQuery.Substring(1);   // remove '?'
                uriQuery = uriQuery.Replace("%20", " ").Replace("%5E", "^");    // de-coding from URL to normal things

                int ind = -1;
                if (uriQuery.StartsWith("jsonp=", StringComparison.InvariantCultureIgnoreCase))
                {
                    uriQuery = uriQuery.Substring("jsonp=".Length);
                    ind = uriQuery.IndexOf('&');
                    if (ind == -1)
                    {
                        throw new Exception("Error: uriQuery.IndexOf('&') 2. Uri: " + uriQuery);
                    }
                    jsonpCallback = uriQuery.Substring(0, ind);
                    uriQuery = uriQuery.Substring(ind + 1);
                }

                if (!uriQuery.StartsWith("StartDate=", StringComparison.InvariantCultureIgnoreCase))
                {
                    throw new Exception("Error: StartDate= was not found. Uri: " + uriQuery);
                }
                uriQuery = uriQuery.Substring("StartDate=".Length);
                ind = uriQuery.IndexOf('&');
                if (ind == -1)
                {
                    ind = uriQuery.Length;
                }
                string startDateStr = uriQuery.Substring(0, ind);
                if (ind < uriQuery.Length)  // if we are not at the end of the string
                    uriQuery = uriQuery.Substring(ind + 1);
                else
                    uriQuery = "";

                if (!uriQuery.StartsWith("EndDate=", StringComparison.InvariantCultureIgnoreCase))
                {
                    throw new Exception("Error: EndDate= was not found. Uri: " + uriQuery);
                }
                uriQuery = uriQuery.Substring("EndDate=".Length);
                ind = uriQuery.IndexOf('&');
                if (ind == -1)
                {
                    ind = uriQuery.Length;
                }
                string endDateStr = uriQuery.Substring(0, ind);
                if (ind < uriQuery.Length)  // if we are not at the end of the string
                    uriQuery = uriQuery.Substring(ind + 1);
                else
                    uriQuery = "";


                if (!uriQuery.StartsWith("strategy=", StringComparison.InvariantCultureIgnoreCase))
                {
                    throw new Exception("Error: strategy= was not found. Uri: " + uriQuery);
                }
                uriQuery = uriQuery.Substring("strategy=".Length);
                ind = uriQuery.IndexOf('&');
                if (ind == -1)
                {
                    ind = uriQuery.Length;
                }
                string strategyName = uriQuery.Substring(0, ind);
                if (ind < uriQuery.Length)  // if we are not at the end of the string
                    uriQuery = uriQuery.Substring(ind + 1);
                else
                    uriQuery = "";


                string strategyParams = uriQuery;

                DateTime startDate = DateTime.MinValue;
                if (startDateStr.Length != 0)
                {
                    if (!DateTime.TryParse(startDateStr, out startDate))
                        throw new Exception("Error: startDateStr couldn't be converted: " + uriQuery);
                }
                DateTime endDate = DateTime.MaxValue;
                if (endDateStr.Length != 0)
                {
                    if (!DateTime.TryParse(endDateStr, out endDate))
                        throw new Exception("Error: endDateStr couldn't be converted: " + uriQuery);
                }

                GeneralStrategyParameters generalParams = new GeneralStrategyParameters() { startDateUtc = startDate, endDateUtc = endDate };

                string jsonString = (await VXX_SPY_Controversial.GenerateQuickTesterResponse(generalParams, strategyName, strategyParams));
                if (jsonString == null)
                    jsonString = (await LEtfDistcrepancy.GenerateQuickTesterResponse(generalParams, strategyName, strategyParams));
                if (jsonString == null)
                    jsonString = (await TotM.GenerateQuickTesterResponse(generalParams, strategyName, strategyParams));

                if (jsonString == null)
                    throw new Exception("Strategy was not found in the WebApi: " + strategyName);

                return ResponseBuilder(jsonpCallback, jsonString);
            }
            catch (Exception e)
            {
                return ResponseBuilder(jsonpCallback, @"{ ""errorMessage"":  ""Exception caught by WebApi Get(): " + e.Message + @""" }");
            }
        }

        public HttpResponseMessage ResponseBuilder(string p_jsonpCallback, string p_jsonResponse)
        {
            var response = this.Request.CreateResponse(HttpStatusCode.OK);
            StringBuilder responseStrBldr = new StringBuilder();
            if (p_jsonpCallback != null)
                responseStrBldr.Append(p_jsonpCallback + "(\n");
            responseStrBldr.Append(p_jsonResponse);
            if (p_jsonpCallback != null)
                responseStrBldr.Append(");");

            response.Content = new StringContent(responseStrBldr.ToString(), Encoding.UTF8, "application/json");
            return response;
        }

    }
}
