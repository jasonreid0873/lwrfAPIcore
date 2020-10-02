using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace lwrfAPIcore.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class HeatingController : ControllerBase
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // GET: api/Heating
        /// <summary>
        /// Lists the logged heating monitoring events from the API
        /// </summary>
        /// <param name="start">The start datetime to filter the event monitor events from (format YYYY-MM-DDTHH:MM:SS)</param>
        /// <param name="end">The end datetime to filter the event monitor events to (format YYYY-MM-DDTHH:MM:SS)</param>
        /// <param name="serial">If specified will filter the results by the supplied serial number</param>
        /// <returns></returns>
        [HttpGet]
        public JsonResult Get(DateTime start, DateTime end, string serial = "")
        {
            logger.Info("GET start:{0}, end:{1}, serial:{2}", start.ToString(), end.ToString(), serial);
            var result = (SQLite.ReadHeatingTable(start, end, serial, 0));
            return new JsonResult(result);
        }

        // POST: api/Heating
        /// <summary>
        /// Allows for the control of a specific heating device (Thermostats and TRVs)
        /// </summary>
        /// <param name="value">JSON string containing the action, device and temperature to be set</param>
        /// <returns></returns>
        [HttpPost]
        public JsonResult Post([FromBody]Models.HeatingParameters value)
        {
            logger.Info("POST action:{0}, slot:{1}, temp:{2}", value.Action, value.Slot, value.Temp);
            Dictionary<string, string> parameters = new Dictionary<string, string>
                {
                    {"device", value.Slot.ToString() },
                    { "level", value.Temp.ToString() ?? "0" }
                };
            var result = LWRF.QueueRequest("heat_" + value.Action, parameters);

            return new JsonResult(result);
        }
    }
}
