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
    public class SwitchParameters
    {
        /// <summary>
        /// <br /> on = turn on target device<br />
        /// off = turn off target device<br />
        /// dim = set dim level of target device
        /// </summary>
        [Required]
        public string Action { get; set; }
        /// <summary>
        /// room ID for target device
        /// </summary>
        [Required]
        public int Room { get; set; }
        /// <summary>
        /// device ID for target device
        /// </summary>
        [Required]
        public int Device { get; set; }
        /// <summary>
        /// dim level (0 - 100)
        /// </summary>
        public int Level { get; set; }
    }
}