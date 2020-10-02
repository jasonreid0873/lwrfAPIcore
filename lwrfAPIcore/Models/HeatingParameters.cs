using System;
using System.ComponentModel.DataAnnotations;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace lwrfAPIcore.Models
{
    /// <summary>
    /// 
    /// </summary>
    public class HeatingParameters
    {
        /// <summary>
        /// <br /> on = set target device on (for TRV full open)<br />
        /// off = set target device off (for TRV full close)<br />
        /// temp = set target device to speicifed target temperature
        /// </summary>
        [Required]
        public string Action { get; set; }
        /// <summary>
        /// <br />Device Slot ID
        /// </summary>
        [Required]
        public int Slot { get; set; }
        /// <summary>
        /// <br />Target temperature
        /// </summary>
        public decimal Temp { get; set; }
    }
}