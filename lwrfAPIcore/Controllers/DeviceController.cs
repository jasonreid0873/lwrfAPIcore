using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Net.Http;

namespace lwrfAPIcore.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class DeviceController : ControllerBase
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // GET: api/Device
        /// <summary>
        /// List or Search details from all known device slots held in the API
        /// </summary>
        /// <param name="scan">If set to true will force a scan of all empty device slots</param>
        /// <param name="serial">If set will filter results by serial number</param>
        /// <returns></returns>
        [HttpGet]
        public JsonResult Get(bool scan = false, string serial = "")
        {
            logger.Info("GET scan:{0}, serial:{1}", scan.ToString(), serial);
            if (scan)
            {
                for (int i = 0; i <= 80; i++)
                {
                    Put(i);
                }
            }
            var result = SQLite.ReadDeviceTable(0, serial);
            return new JsonResult(result);
        }


        // GET: api/Device/5
        /// <summary>
        /// List information on a specific device/slot held in the API
        /// </summary>
        /// <param name="id">The device or slot ID to be queried</param>
        /// <returns></returns>
        [HttpGet("{id}", Name = "GetDevice")]
        public JsonResult Get(int id)
        {
            logger.Info("GET id:{0}", id.ToString());
            var result = SQLite.ReadDeviceTable(id, "");
            return new JsonResult(result);
        }



        // PUT: api/Device/5
        /// <summary>
        /// Set or update device slot information held in the API from the LightwaveRF hub
        /// </summary>
        /// <param name="id">The device or slot ID to be set/updated</param>
        [HttpPut("{id}")]
        public JsonResult Put(int id)
        {
            logger.Info("PUT id:{0}", id.ToString());
            var action = "device_info";
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                { "device", id.ToString() ?? "" }
            };
            var result = LWRF.QueueRequest(action, parameters);
            return new JsonResult(result);
        }

        // DELETE: api/ApiWithActions/5
        /// <summary>
        /// Delete device slot information held in the API
        /// </summary>
        /// <param name="id">The device or slot ID to be deleted</param>
        [HttpDelete("{id}")]
        public void Delete(int id)
        {
            logger.Info("DELETE id:{0}", id.ToString());
            SQLite.DeleteDeviceSlot(id);
        }
    }
}
