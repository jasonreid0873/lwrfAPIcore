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
    public class SystemController : ControllerBase
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // GET: api/System
        /// <summary>
        /// 
        /// </summary>
        /// <param name="start">The start datetime to filter the event monitor events from (format YYYY-MM-DDTHH:MM:SS)</param>
        /// <param name="end">The end datetime to filter the event monitor events to (format YYYY-MM-DDTHH:MM:SS)</param>
        /// <returns></returns>
        [HttpGet]
        public JsonResult Get(DateTime start, DateTime end)
        {
            logger.Info("GET start:{0}, end:{1}", start.ToString(), end.ToString());
            var result = (SQLite.ReadSystemTable(start, end, 0));
            return new JsonResult(result);
        }

        // POST: api/System
        /// <summary>
        /// Execute a system command to the LightwaveRF Hub
        /// </summary>
        /// <param name="cmd">Commands available [register | deregister | info | ledon | ledoff]</param>
        /// <returns></returns>
        [HttpPost("{cmd}", Name = "PostSystem")]
        public JsonResult Post(string cmd)
        {
            logger.Info("POST cmd:{0}", cmd);
            var result = LWRF.QueueRequest("system_" + cmd);
            return new JsonResult(result);
        }
    }
}
