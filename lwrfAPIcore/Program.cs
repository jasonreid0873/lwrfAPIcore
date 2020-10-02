using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace lwrfAPIcore
{
    public class Program
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public static void Main(string[] args)
        {
            logger.Info("-----------------LightwaveRF RESTful API Starting-----------------");
            SQLite.OpenDatabase();
            LWRF.Start();
            UDPListener.Start(9761);
            UDPSender.Start();
            logger.Info("-----------------LightwaveRF RESTful API Started------------------");
            CreateWebHostBuilder(args).Build().Run();
        }

        public static IWebHostBuilder CreateWebHostBuilder(string[] args) =>
            WebHost.CreateDefaultBuilder(args)
                .UseUrls("http://0.0.0.0:8100")
                .UseStartup<Startup>();
    }
}
