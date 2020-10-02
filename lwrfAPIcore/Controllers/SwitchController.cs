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
    public class SwitchController : ControllerBase
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        // POST: api/Switch
        /// <summary>
        /// Allows for the control of a specific power device (Sockets and Lighting)
        /// </summary>
        /// <param name="value">JSON string containing the room, device and level to be set</param>
        /// <returns></returns>
        [HttpPost]
        public JsonResult Post([FromBody] Models.SwitchParameters value)
        {
            logger.Info("POST action:{0}, room:{1}, device:{2}, level:{3}", value.Action, value.Room, value.Device, value.Level);
            Dictionary<string, string> parameters = new Dictionary<string, string>
            {
                {"room", value.Room.ToString() },
                { "device", value.Device.ToString() },
                { "level", value.Level.ToString() ?? "0" }
            };
            var result = LWRF.QueueRequest("switch_" + value.Action, parameters);
            return new JsonResult(result);
        }
    }
}
