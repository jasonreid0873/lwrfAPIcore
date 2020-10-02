using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Text;
using System.Threading;



namespace lwrfAPIcore
{
    public class LWRF
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private static bool abort = false;
        private struct Request
        {
            internal string action;
            internal Dictionary<string, string> parameters;
        }
        private static Queue<Request> requestQueue = new Queue<Request>();
        static Thread _Thread = new Thread(() => ProcessRequestQueue(requestQueue));

        public static string lwrfHubIP = null;
        public static int lwrfHubRetries;
        public struct Message
        {
            internal int msgId;
            internal string messageText;
            internal int delay;
            internal bool system;
            internal int room;
            internal int temp;
            internal int packet;
            internal int attenpt;
        }
        public static Dictionary<int, Message> messageList = new Dictionary<int, Message>();
        public static Dictionary<int, string> messageAck = new Dictionary<int, string>();

        public static void Start()
        {
            try
            {
                logger.Info("Starting LWRF Request Queue Monitor");
                _Thread.IsBackground = true;
                _Thread.Name = "LWRF Request Queue Montior";
                _Thread.Start();
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public static void Stop()
        {
            try
            {
                logger.Info("Stopping UDP Sender Queue Monitor");
                requestQueue.Clear();
                abort = true;
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public static JToken QueueRequest(string action, Dictionary<string, string> parameters = null)
        {
            try
            {
                Request r = new Request
                {
                    action = action,
                    parameters = parameters
                };
                logger.Info("QueueRequest|Queuing LWRF Request");
                requestQueue.Enqueue(r);
                return JToken.Parse("{slot:0,result:\"queued\"}");
            }
            catch (Exception e)
            {
                logger.Error(e);
                return JToken.Parse("{slot:0,result:\"failed\"}");
            }
        }

        private static void ProcessRequestQueue(Queue<Request> requestQueue)
        {
            try
            {
                while (!abort)
                {
                    if (requestQueue.Count == 0)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }

                    if (requestQueue.Count != 0)
                    {
                        logger.Info("ProcessRequestQueue|Dequeuing LWRF Request");
                        Request r = requestQueue.Dequeue();
                        SendRequest(r.action, r.parameters);
                        Thread.Sleep(100);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static void SendRequest(string action, Dictionary<string, string> parameters = null)
        {
            string room = null;
            string device = null;
            string level = null;

            if (parameters != null)
            {
                parameters.TryGetValue("room", out room);
                parameters.TryGetValue("device", out device);
                parameters.TryGetValue("level", out level);
            }

            //Check that levels are within the correct range for lighting devices
            if (action.ToLower().StartsWith("light") && level != null)
            {
                if (int.Parse(level) < 0) { level = "0"; }
                if (int.Parse(level) > 100) { level = "100"; }
                if (int.Parse(level) == 0) { action = "light_off"; }
            }

            //Check that levels are within the correct range for heating devices
            if (action.ToLower().StartsWith("heat"))
            {
                if (int.Parse(level) < 0) { level = "0"; }
                if (int.Parse(level) > 40) { level = "40"; }
            }

            Message m = new Message();
            switch (action.ToLower())
            {
                case "system_subscribe":
                    logger.Info("SmartThings Subscribe Request");
                    break;
                case "system_register":
                    m.messageText = "!F*p";
                    m.system = true;
                    break;
                case "system_deregister":
                    m.messageText = "!F*xp";
                    m.system = true;
                    break;
                case "system_info":
                    m.messageText = "@H";
                    m.system = true;
                    break;
                case "system_ledon":
                    m.messageText = "@L1";
                    m.system = true;
                    break;
                case "system_ledoff":
                    m.messageText = "@L0";
                    m.system = true;
                    break;
                case "switch_on":
                    m.messageText = "!R" + room + "D" + device + "F1|\0";
                    m.delay = 1;
                    break;
                case "switch_off":
                    m.messageText = "!R" + room + "D" + device + "F0|\0";
                    m.delay = 1;
                    break;
                case "switch_dim":
                    m.messageText = "!R" + room + "D" + device + "FdP" + Math.Round((Convert.ToInt32(level) * 0.32)) + "|\0";
                    m.delay = 1;
                    break;
                case "switch_plock":
                    m.messageText = "!R" + room + "D" + device + "Fl|\0";
                    break;
                case "switch_flock":
                    m.messageText = "!R" + room + "D" + device + "Fk|\0";
                    break;
                case "switch_unlock":
                    m.messageText = "!R" + room + "D" + device + "Fu|\0";
                    break;
                case "heat_temp":
                    m.messageText = "!R" + device + "F*tP" + level + "|\0";
                    m.delay = 5;
                    m.room = int.Parse(device);
                    m.temp = int.Parse(level);
                    break;
                case "heat_on":
                    m.messageText = "!R" + device + "F*tP60|\0";
                    m.delay = 5;
                    m.room = int.Parse(device);
                    m.temp = 60;
                    break;
                case "heat_off":
                    m.messageText = "!R" + device + "F*tP50|\0";
                    m.delay = 5;
                    m.room = int.Parse(device);
                    m.temp = 50;
                    break;
                case "device_info":
                    m.messageText = "@?R" + device;
                    break;
                case "device_link":
                    m.messageText = "!R" + device + "F*L|\0";
                    break;
                case "device_unlink":
                    m.messageText = "!R" + device + "F*xU|\0";
                    break;
                default:
                    m.messageText = "@H";
                    break;
            }
            if (m.messageText != null)
            {
                logger.Info("SendRequest|Queuing UDP Message {0}", m.messageText);
                UDPSender.QueueUdpMessage(m);
            }
            return;
        }
    }

    public class Notify
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        public static string notifyIP = null;
        public static string notifyPort = null;

        public static void SendUpdate(JToken json)
        {
            try
            {
                var url = "/";
                var request = SQLite.ReadDeviceTable(0, json.SelectToken("serial").ToString());
                var client = new HttpClient
                {
                    BaseAddress = new Uri("http://" + notifyIP + ":" + notifyPort)
                };
                logger.Info("Sending Notify update for slot {0} to {1}:{2}", request.SelectToken("slot"), notifyIP, notifyPort);
                var content = new StringContent(request.ToString(), Encoding.UTF8, "application/json");
                var response = client.PostAsync(url, content);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }
    }

    public class UDPListener
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private static int lastTrans = 0;

        public static void Start(int ipPort)
        {
            try
            {
                logger.Info("Starting UDP Listener on port {0}", ipPort.ToString());
                UdpClient client = new UdpClient(ipPort);
                client.BeginReceive(new AsyncCallback(OnUdpData), client);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public static void Stop()
        {
            try
            {
                logger.Info("Stopping UDP Listener");
                LWRF.messageAck.Clear();
                LWRF.messageList.Clear();
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static void OnUdpData(IAsyncResult result)
        {

            UdpClient client = result.AsyncState as UdpClient;
            IPEndPoint source = new IPEndPoint(0, 0);
            byte[] message = client.EndReceive(result, ref source);
            string messageData = Encoding.ASCII.GetString(message);

            try
            {
                if (messageData.StartsWith("*!"))
                {
                    //JSON Message
                    messageData = messageData.Substring(2);
                    JToken jsonData = JToken.Parse(messageData);
                    int trans = Convert.ToInt32(jsonData.SelectToken("trans"));

                    if (trans > lastTrans)
                    {
                        lastTrans = trans;
                        logger.Debug(messageData);

                        //Insert JSON into System Table
                        if (jsonData.SelectToken("pkt").ToString() == "system")
                        {
                            logger.Info("OnUdpData|System JSON message received");
                            SQLite.InsertSystemTable(jsonData);
                        }

                        //Insert JSON into Energy Table
                        if (jsonData.SelectToken("fn").ToString() == "meterData")
                        {
                            logger.Info("OnUdpData|Energy meter JSON message received");
                            SQLite.InsertEnergyTable(jsonData);
                            Notify.SendUpdate(jsonData);
                        }

                        //Insert JSON into Heating Table
                        if (jsonData.SelectToken("fn").ToString() == "statusPush")
                        {
                            logger.Info("OnUdpData|Heating JSON message received");
                            SQLite.InsertHeatingTable(jsonData);
                            Notify.SendUpdate(jsonData);
                        }

                        //Insert JSON into Device Table
                        if (jsonData.SelectToken("pkt").ToString() == "room" && jsonData.SelectToken("fn").ToString() == "read")
                        {
                            SQLite.InsertDeviceTable(jsonData);
                        }

                        //Set Target Confirmation - Confirms LWRF hub has accepted send request and records packet ID and send parameters
                        if (jsonData.SelectToken("fn").ToString() == "setTarget")
                        {
                            logger.Info("OnUdpData|setTarget JSON message received");
                            int room = int.Parse(jsonData.SelectToken("room").ToString());
                            var temp = int.Parse(jsonData.SelectToken("temp").ToString());
                            int packet = int.Parse(jsonData.SelectToken("packet").ToString());

                            foreach (var m in LWRF.messageList)
                            {
                                if (m.Value.room == room && m.Value.temp == temp)
                                {
                                    logger.Debug("OnUdpData|messageList: add msgId {0} packetId {1}", m.Value.msgId, packet);
                                    var stTemp = LWRF.messageList[m.Value.msgId];
                                    stTemp.packet = packet;
                                    LWRF.messageList[m.Value.msgId] = stTemp;
                                    break;
                                }
                            }
                        }

                        //Set Target Acknowledgement - Confirms LWRF device has accepted send request via hub, if failed will attempt to resend request
                        if (jsonData.SelectToken("fn").ToString() == "ack")
                        {
                            int packet = int.Parse(jsonData.SelectToken("packet").ToString());
                            string status = jsonData.SelectToken("status").ToString();

                            logger.Info("OnUdpData|Device ACK JSON message received packetId {0}", packet);

                            foreach (var m in LWRF.messageList)
                            {
                                if (m.Value.packet == packet)
                                {
                                    if (status.ToLower() == "success")
                                    {
                                        logger.Info("OnUdpData|Device Acknowledged {0}", m.Value.messageText);
                                        logger.Debug("OnUdpData|messageList: removing msgId {0} packetId {1}", m.Value.msgId, packet);
                                        LWRF.messageList.Remove(m.Value.msgId);
                                        if (LWRF.messageList.Count == 0)
                                        {
                                            logger.Debug("OnUdpData|messageList: Empty");
                                        }
                                        else
                                        {
                                            foreach (var list in LWRF.messageList)
                                            {
                                                logger.Debug("OnUdpData|messageList: {0} {1}", list.Key, list.Value.msgId);
                                            }
                                        }
                                        break;
                                    }
                                    else
                                    {
                                        logger.Info("OnUdpData|Device Unsuccessful {0}", m.Value.messageText);
                                        if (m.Value.attenpt < LWRF.lwrfHubRetries)
                                        {
                                            logger.Debug("OnUdpData|msgID {0} failure will requeue", m.Value.msgId);
                                            var aTemp = LWRF.messageList[m.Value.msgId];
                                            aTemp.msgId = 0;
                                            aTemp.packet = 0;
                                            aTemp.attenpt += 1;
                                            UDPSender.QueueUdpMessage(aTemp);
                                        }
                                        else
                                        {
                                            logger.Debug("OnUdpData|msgID {0} exceeds maximum requeue count, will cancel", m.Value.msgId);
                                        }
                                        LWRF.messageList.Remove(m.Value.msgId);
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                else
                {
                    //ACK Message
                    messageData = messageData.TrimEnd('\r', '\n');
                    logger.Debug("OnUdpData|messageAck: {0}", messageData);
                    string[] messageParts = messageData.Split(',');
                    logger.Info("OnUdpData|LWRF Hub ACK received msgID {0}", messageParts[0]);
                    LWRF.messageAck.Add(Convert.ToInt32(messageParts[0]), messageParts[1]);
                    logger.Debug("OnUdpData|messageAck: add msgID {0}", messageParts[0]);
                    foreach (var ack in LWRF.messageAck)
                    {
                        logger.Debug("OnUdpData|messageAck: {0} {1}", ack.Key, ack.Value);
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
            finally
            {
                client.BeginReceive(new AsyncCallback(OnUdpData), client);
            }
        }
    }

    public class UDPSender
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private static bool abort = false;
        private static int msgId = 0;
        private static Queue<LWRF.Message> UdpMessageQueue = new Queue<LWRF.Message>();
        static Thread _Thread = new Thread(() => ProcessUdpMessageQueue(UdpMessageQueue));

        public static void Start()
        {
            try
            {
                logger.Info("Starting UDP Sender Queue Monitor");
                _Thread.IsBackground = true;
                _Thread.Name = "UDPSender Queue Montior";
                _Thread.Start();
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public static void Stop()
        {
            try
            {
                logger.Info("Stopping UDP Sender Queue Monitor");
                UdpMessageQueue.Clear();
                abort = true;
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public static void QueueUdpMessage(LWRF.Message m)
        {
            try
            {
                if (m.messageText == null)
                {
                    return;
                }

                msgId++;
                if (msgId > 999) msgId = 1;
                m.msgId = msgId;

                if (LWRF.messageList.Count == 0)
                {
                    logger.Debug("QueueUdpMessage|{0}|messageList: Empty", msgId);
                }
                else
                {
                    foreach (var list in LWRF.messageList)
                    {
                        logger.Debug("QueueUdpMessage|{0}|messageList: {1} {2}", msgId, list.Key, list.Value.room);
                    }
                }

                logger.Info("QueueUdpMessage|{0}|Queueing {1}", msgId, m.msgId.ToString("000") + m.messageText);

                LWRF.messageList.Add(msgId, m);

                UdpMessageQueue.Enqueue(m);
                logger.Debug("QueueUdpMessage|{0}|UdpMessageQueue Count: {1}", msgId, UdpMessageQueue.Count);

            }
            catch (Exception e)
            {
                logger.Error(e);
                logger.Error("QueueUdpMessage|{0}", msgId);
            }
        }

        private static void ProcessUdpMessageQueue(Queue<LWRF.Message> udpMessageQueue)
        {
            try
            {
                while (!abort)
                {
                    if (UdpMessageQueue.Count == 0)
                    {
                        Thread.Sleep(1000);
                        continue;
                    }

                    while (UdpMessageQueue.Count != 0)
                    {
                        LWRF.Message m = UdpMessageQueue.Peek();
                        logger.Info("ProcessUdpMessageQueue|{0}|Dequeueing {1}", m.msgId, m.msgId.ToString("000") + m.messageText);
                        SendUdpMessage(m);
                        logger.Info("ProcessUdpMessageQueue|{0}|Pausing for {1} seconds", m.msgId, m.delay);
                        Thread.Sleep(m.delay * 1000);
                        continue;
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static void SendUdpMessage(LWRF.Message m)
        {
            try
            {
                logger.Info("SendUdpMessage|{0}|Sending: {1} Attempt: {2}", m.msgId, m.msgId.ToString("000") + m.messageText, m.attenpt);

                UDPSocket c = new UDPSocket();
                c.Client(LWRF.lwrfHubIP, 9760);
                c.Send(m.msgId, m.messageText);

                if (m.system)
                {
                    UdpMessageQueue.Dequeue();
                }
                else
                {
                    bool ack = UdpMessageAcknowledged(m.msgId, 10);
                    if (!ack)
                    {
                        if (m.attenpt <= 5)
                        {
                            logger.Info("SendUdpMessage|{0}|Requeueing {1}", m.msgId, m.msgId.ToString("000") + m.messageText);
                        }
                        else
                        {
                            logger.Info("SendUdpMessage|{0}|Cancelling {1}", m.msgId, m.msgId.ToString("000") + m.messageText);
                            UdpMessageQueue.Dequeue();
                        }
                    }
                    else
                    {
                        logger.Info("SendUdpMessage|{0}|LWRF Hub Acknowledged {1}", m.msgId, m.msgId.ToString("000") + m.messageText);
                        UdpMessageQueue.Dequeue();
                    }
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        private static bool UdpMessageAcknowledged(int msgId, int retries = 5)
        {
            string message = null;
            int loopCount = 0;

            do
            {
                logger.Debug("UdpMessageAcknowledged|{0}|messageAck: checking for msgId {0}", msgId);

                if (LWRF.messageAck.Count == 0)
                {
                    logger.Debug("UdpMessageAcknowledged|{0}|messageAck: Empty", msgId);
                }
                else
                {
                    foreach (var ack in LWRF.messageAck)
                    {
                        logger.Debug("UdpMessageAcknowledged|{0}|messageAck: {1} {2}", msgId, ack.Key, ack.Value);
                    }
                }

                if (LWRF.messageAck.ContainsKey(msgId))
                {
                    logger.Debug("UdpMessageAcknowledged|{0}|messageAck: found msgId {0}", msgId);
                    LWRF.messageAck.TryGetValue(msgId, out message);
                    logger.Debug("UdpMessageAcknowledged|{0}|messageAck: removing msgId {0}", msgId);
                    LWRF.messageAck.Remove(msgId);
                    if (LWRF.messageAck.Count == 0)
                    {
                        logger.Debug("UdpMessageAcknowledged|{0}|messageAck: Empty", msgId);
                    }
                    else
                    {
                        foreach (var ack in LWRF.messageAck)
                        {
                            logger.Debug("UdpMessageAcknowledged|{0}|messageAck: {1} {2}", msgId, ack.Key, ack.Value);
                        }
                    }
                    break;
                }
                Thread.Sleep(500);
                loopCount++;
            } while (loopCount < retries);

            if (LWRF.messageList.ContainsKey(msgId))
            {
                if (LWRF.messageList[msgId].room == 0)
                {
                    logger.Debug("UdpMessageAcknowledged|{0}|messageList: removing msgId {0}", msgId);
                    LWRF.messageList.Remove(msgId);
                    if (LWRF.messageList.Count == 0)
                    {
                        logger.Debug("UdpMessageAcknowledged|{0}|messageList: Empty", msgId);
                    }
                    else
                    {
                        foreach (var list in LWRF.messageList)
                        {
                            logger.Debug("UdpMessageAcknowledged|{0}|messageList: {1} {2}", msgId, list.Key, list.Value.room);
                        }
                    }
                }
            }

            if (loopCount < retries && message == "OK")
            {
                logger.Debug("UdpMessageAcknowledged|{0}|True", msgId);
                return true;
            }
            else
            {
                logger.Debug("UdpMessageAcknowledged|{0}|False", msgId);
                return false;
            }
        }
    }

    public class UDPSocket
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        private Socket _socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        private const int bufSize = 8 * 1024;
        private State state = new State();
        private class State
        {
            public byte[] buffer = new byte[bufSize];
        }

        public void Client(string address, int port)
        {
            try
            {
                _socket.Connect(IPAddress.Parse(address), port);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public void Send(int messageId, string text)
        {
            try
            {
                byte[] data = Encoding.ASCII.GetBytes(messageId.ToString("000") + "," + text);
                _socket.BeginSend(data, 0, data.Length, SocketFlags.None, (ar) =>
                {
                    State so = (State)ar.AsyncState;
                    int bytes = _socket.EndSend(ar);
                }, state);
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }
    }

    public class SQLite
    {
        private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        static SQLiteConnection m_dbConnection;

        private static IEnumerable<Dictionary<string, object>> Serialize(SQLiteDataReader reader)
        {
            var results = new List<Dictionary<string, object>>();
            var cols = new List<string>();
            for (var i = 0; i < reader.FieldCount; i++)
                cols.Add(reader.GetName(i));

            while (reader.Read())
                results.Add(SerializeRow(cols, reader));

            return results;
        }

        private static Dictionary<string, object> SerializeRow(IEnumerable<string> cols, SQLiteDataReader reader)
        {
            var result = new Dictionary<string, object>();
            foreach (var col in cols)
                result.Add(col, reader[col]);
            return result;
        }

        private static Int32 GetUnixTime(DateTime dt)
        {
            return (Int32)(dt.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
        }

        public static void CreateNewDatabase()
        {
            SQLiteConnection.CreateFile("MyDatabase.sqlite");
        }

        public static void OpenDatabase()
        {
            try
            {
                logger.Info("Starting SQLite DB");
                string cs = "Data Source=" + System.AppDomain.CurrentDomain.BaseDirectory + "Database/lwrf.db;Version=3;";
                m_dbConnection = new SQLiteConnection(cs);
                m_dbConnection.Open();
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                logger.Error(e);
            }
            
        }

        public static void CloseDatabase()
        {
            logger.Info("Stopping SQLite DB");
            m_dbConnection.Close();
        }

        public static void InsertSystemTable(JToken Json)
        {
            logger.Info("Inserting data into system table");
            var sql = "INSERT INTO system(trans,mac,time,pkt,type,prod,fw,uptime,timeZone,lat,long,tmrs,evns,run,macs,ip,dev) VALUES";
            sql += "(@trans,@mac,@time,@pkt,@type,@prod,@fw,@uptime,@timeZone,@lat,@long,@tmrs,@evns,@run,@macs,@ip,@dev)";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            command.Parameters.AddWithValue("@trans", Json.SelectToken("trans"));
            command.Parameters.AddWithValue("@mac", Json.SelectToken("mac"));
            command.Parameters.AddWithValue("@time", Json.SelectToken("time"));
            command.Parameters.AddWithValue("@pkt", Json.SelectToken("pkt"));
            command.Parameters.AddWithValue("@type", Json.SelectToken("type"));
            command.Parameters.AddWithValue("@prod", Json.SelectToken("prod"));
            command.Parameters.AddWithValue("@fw", Json.SelectToken("fw"));
            command.Parameters.AddWithValue("@uptime", Json.SelectToken("uptime"));
            command.Parameters.AddWithValue("@timeZone", Json.SelectToken("timeZone"));
            command.Parameters.AddWithValue("@lat", Json.SelectToken("lat"));
            command.Parameters.AddWithValue("@long", Json.SelectToken("long"));
            command.Parameters.AddWithValue("@tmrs", Json.SelectToken("tmrs"));
            command.Parameters.AddWithValue("@evns", Json.SelectToken("evns"));
            command.Parameters.AddWithValue("@run", Json.SelectToken("run"));
            command.Parameters.AddWithValue("@macs", Json.SelectToken("macs"));
            command.Parameters.AddWithValue("@ip", Json.SelectToken("ip"));
            command.Parameters.AddWithValue("@dev", Json.SelectToken("dev"));

            try
            {
                int insertResult = 0;
                insertResult = command.ExecuteNonQuery();
                if (insertResult == 1)
                {
                    logger.Debug("trans {0} inserted into system table", Json.SelectToken("trans"));
                }
                else
                {
                    logger.Warn("trans {0} NOT inserted into system table!", Json.SelectToken("trans"));
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
            finally
            {
                command.Dispose();
            }
        }

        public static void InsertEnergyTable(JToken Json)
        {
            logger.Info("Inserting data into energy table");
            var sql1 = "INSERT INTO energy(trans,mac,time,pkt,type,prod,fn,serial,cUse,todUse) VALUES";
            sql1 += "(@trans,@mac,@time,@pkt,@type,@prod,@fn,@serial,@cUse,@todUse)";
            SQLiteCommand command1 = new SQLiteCommand(sql1, m_dbConnection);
            command1.Parameters.AddWithValue("@trans", Json.SelectToken("trans"));
            command1.Parameters.AddWithValue("@mac", Json.SelectToken("mac"));
            command1.Parameters.AddWithValue("@time", Json.SelectToken("time"));
            command1.Parameters.AddWithValue("@pkt", Json.SelectToken("pkt"));
            command1.Parameters.AddWithValue("@type", Json.SelectToken("type"));
            command1.Parameters.AddWithValue("@prod", Json.SelectToken("prod"));
            command1.Parameters.AddWithValue("@fn", Json.SelectToken("fn"));
            command1.Parameters.AddWithValue("@serial", Json.SelectToken("serial"));
            command1.Parameters.AddWithValue("@cUse", Json.SelectToken("cUse"));
            command1.Parameters.AddWithValue("@todUse", Json.SelectToken("todUse"));

            var sql2 = "UPDATE device SET cUse = @cUse, todUse = @todUse, time = @time ";
            sql2 += "WHERE serial = @serial";
            SQLiteCommand command2 = new SQLiteCommand(sql2, m_dbConnection);
            command2.Parameters.AddWithValue("@time", Json.SelectToken("time"));
            command2.Parameters.AddWithValue("@serial", Json.SelectToken("serial"));
            command2.Parameters.AddWithValue("@cUse", Json.SelectToken("cUse"));
            command2.Parameters.AddWithValue("@todUse", Json.SelectToken("todUse"));

            try
            {
                int insertResult = 0;
                insertResult = command1.ExecuteNonQuery();
                if (insertResult == 1)
                {
                    logger.Debug("trans {0} inserted into energy table", Json.SelectToken("trans"));
                }
                else
                {
                    logger.Error("trans NOT inserted into energy table!", Json.SelectToken("trans"));
                }
                insertResult = 0;
                insertResult = command2.ExecuteNonQuery();
                if (insertResult == 1)
                {
                    logger.Debug("device {0} updated in device table", Json.SelectToken("serial"));
                }
                else
                {
                    logger.Debug("device {0} NOT updated into device table!", Json.SelectToken("serial"));
                }
            }
            catch (Exception e)
            {
                logger.Error("Unable to insert record into energy table!");
                logger.Debug(e);
            }
            finally
            {
                command1.Dispose();
                command2.Dispose();
            }
        }

        public static void InsertHeatingTable(JToken Json)
        {
            logger.Info("Inserting data into heating & device table");
            var sql1 = "INSERT INTO heating(trans,mac,time,pkt,type,prod,fn,serial,batt,ver,state,cTemp,cTarg,output,nTarg,nSlot,prof) VALUES";
            sql1 += "(@trans,@mac,@time,@pkt,@type,@prod,@fn,@serial,@batt,@ver,@state,@cTemp,@cTarg,@output,@nTarg,@nSlot,@prof)";
            SQLiteCommand command1 = new SQLiteCommand(sql1, m_dbConnection);
            command1.Parameters.AddWithValue("@trans", Json.SelectToken("trans"));
            command1.Parameters.AddWithValue("@mac", Json.SelectToken("mac"));
            command1.Parameters.AddWithValue("@time", Json.SelectToken("time"));
            command1.Parameters.AddWithValue("@pkt", Json.SelectToken("pkt"));
            command1.Parameters.AddWithValue("@type", Json.SelectToken("type"));
            command1.Parameters.AddWithValue("@prod", Json.SelectToken("prod"));
            command1.Parameters.AddWithValue("@fn", Json.SelectToken("fn"));
            command1.Parameters.AddWithValue("@serial", Json.SelectToken("serial"));
            command1.Parameters.AddWithValue("@batt", Json.SelectToken("batt"));
            command1.Parameters.AddWithValue("@ver", Json.SelectToken("ver"));
            command1.Parameters.AddWithValue("@state", Json.SelectToken("state"));
            command1.Parameters.AddWithValue("@cTemp", Json.SelectToken("cTemp"));
            command1.Parameters.AddWithValue("@cTarg", Json.SelectToken("cTarg"));
            command1.Parameters.AddWithValue("@output", Json.SelectToken("output"));
            command1.Parameters.AddWithValue("@nTarg", Json.SelectToken("nTarg"));
            command1.Parameters.AddWithValue("@nSlot", Json.SelectToken("nSlot"));
            command1.Parameters.AddWithValue("@prof", Json.SelectToken("prof"));

            var sql2 = "UPDATE device SET cTemp = @cTemp, cTarg = @cTarg, output = @output, state = @state, batt = @batt, time = @time ";
            sql2 += "WHERE serial = @serial";
            SQLiteCommand command2 = new SQLiteCommand(sql2, m_dbConnection);
            command2.Parameters.AddWithValue("@time", Json.SelectToken("time"));
            command2.Parameters.AddWithValue("@serial", Json.SelectToken("serial"));
            command2.Parameters.AddWithValue("@batt", Json.SelectToken("batt"));
            command2.Parameters.AddWithValue("@state", Json.SelectToken("state"));
            command2.Parameters.AddWithValue("@cTemp", Json.SelectToken("cTemp"));
            command2.Parameters.AddWithValue("@cTarg", Json.SelectToken("cTarg"));
            command2.Parameters.AddWithValue("@output", Json.SelectToken("output"));

            try
            {
                int insertResult = 0;
                insertResult = command1.ExecuteNonQuery();
                if (insertResult == 1)
                {
                    logger.Debug("trans {0} inserted into heating table", Json.SelectToken("trans"));
                }
                else
                {
                    logger.Warn("trans {0} NOT inserted into heating table!", Json.SelectToken("trans"));
                }
                insertResult = 0;
                insertResult = command2.ExecuteNonQuery();
                if (insertResult == 1)
                {
                    logger.Debug("device {0} updated in device table", Json.SelectToken("serial"));
                }
                else
                {
                    logger.Warn("device {0} NOT updated into device table!", Json.SelectToken("serial"));
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
            finally
            {
                command1.Dispose();
                command2.Dispose();
            }
        }

        public static void InsertDeviceTable(JToken Json)
        {
            try
            {
                logger.Info("Inserting data into device table");
                int sqlResult = 0;
                string sqlFunction = null;
                string sql = null;

                if (CheckDeviceExists(int.Parse(Json.SelectToken("slot").ToString())))
                {
                    sqlFunction = "updated";
                    sql = "UPDATE device SET [serial] = @serial, [prod] = @prod ";
                    sql += "WHERE [slot] = @slot";
                }
                else
                {
                    sqlFunction = "inserted";
                    sql = "INSERT INTO device(slot, serial, prod) VALUES";
                    sql += "(@slot, @serial, @prod)";
                }

                SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                command.Parameters.AddWithValue("@slot", Json.SelectToken("slot"));
                command.Parameters.AddWithValue("@serial", Json.SelectToken("serial"));
                command.Parameters.AddWithValue("@prod", Json.SelectToken("prod"));
                sqlResult = command.ExecuteNonQuery();
                command.Dispose();

                if (sqlResult == 1)
                {
                    logger.Debug("device {0} {1} into device table", Json.SelectToken("serial"), sqlFunction);
                }
                else
                {
                    logger.Warn("device {0} NOT {1} into device table!", Json.SelectToken("serial"), sqlFunction);
                }

            }
            catch (Exception e)
            {
                logger.Error(e);
            }
        }

        public static JToken ReadSystemTable(DateTime start, DateTime end, int rows)
        {
            var sql = "SELECT * FROM system";
            sql += " WHERE time BETWEEN @start AND @end";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            command.Parameters.AddWithValue("@start", GetUnixTime(start));
            command.Parameters.AddWithValue("@end", GetUnixTime(end));
            try
            {
                SQLiteDataReader reader = command.ExecuteReader();
                var r = Serialize(reader);
                var j = JArray.Parse(JsonConvert.SerializeObject(@r));
                return j;
            }
            catch (Exception e)
            {
                logger.Error(e);
                return null;
            }
            finally
            {
                command.Dispose();
            }
        }

        public static JToken ReadEnergyTable(DateTime start, DateTime end, int rows)
        {
            var sql = "SELECT * FROM energy";
            sql += " WHERE time BETWEEN @start AND @end";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            command.Parameters.AddWithValue("@start", GetUnixTime(start));
            command.Parameters.AddWithValue("@end", GetUnixTime(end));
            try
            {
                SQLiteDataReader reader = command.ExecuteReader();
                var r = Serialize(reader);
                var j = JArray.Parse(JsonConvert.SerializeObject(@r));
                return j;
            }
            catch (Exception e)
            {
                logger.Error(e);
                return null;
            }
            finally
            {
                command.Dispose();
            }
        }

        public static JToken ReadHeatingTable(DateTime start, DateTime end, string serial, int rows)
        {
            var sql = "SELECT * FROM heating";
            sql += " WHERE ";
            if (serial != "")
            {
                sql += "serial = @serial AND ";
            }
            sql += "time BETWEEN @start AND @end";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            command.Parameters.AddWithValue("@start", GetUnixTime(start));
            command.Parameters.AddWithValue("@end", GetUnixTime(end));
            command.Parameters.AddWithValue("@serial", serial);
            try
            {
                SQLiteDataReader reader = command.ExecuteReader();
                var r = Serialize(reader);
                var j = JArray.Parse(JsonConvert.SerializeObject(@r));
                return j;
            }
            catch (Exception e)
            {
                logger.Error(e);
                return null;
            }
            finally
            {
                command.Dispose();
            }
        }

        public static JToken ReadDeviceTable(int id, string serial)
        {
            var sql = "SELECT * FROM device";
            if (id > 0)
            {
                sql += " WHERE slot = " + id.ToString();
            }
            if (id == 0 && serial != "")
            {
                sql += " WHERE serial = '" + serial + "'";
            }
            sql += " ORDER BY slot";
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            try
            {
                SQLiteDataReader reader = command.ExecuteReader();
                var r = Serialize(reader);
                if (id > 0 || serial != "")
                {
                    var j = JArray.Parse(JsonConvert.SerializeObject(@r)).FirstOrDefault();
                    return j;
                }
                else
                {
                    var j = JArray.Parse(JsonConvert.SerializeObject(@r));
                    return j;
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
                return null;
            }
            finally
            {
                command.Dispose();
            }
        }

        public static void DeleteDeviceSlot(int id)
        {
            var sql = "DELETE FROM device WHERE slot = " + id.ToString();
            SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
            try
            {
                int insertResult = 0;
                insertResult = command.ExecuteNonQuery();
                if (insertResult == 1)
                {
                    //
                }
                else
                {
                    logger.Warn("Row NOT deleted from device table");
                }
            }
            catch (Exception e)
            {
                logger.Error(e);
            }
            finally
            {
                command.Dispose();
            }
        }

        public static bool CheckDeviceExists(int id)
        {
            try
            {
                var sql = "SELECT * FROM device";
                if (id > 0)
                {
                    sql += " WHERE slot = " + id.ToString();
                }
                SQLiteCommand command = new SQLiteCommand(sql, m_dbConnection);
                SQLiteDataReader reader = command.ExecuteReader();
                var result = reader.HasRows;
                logger.Debug("Slot {0} Exists: {1}", id, result);
                return result;
            }
            catch (Exception e)
            {
                logger.Error(e);
                return false;
            }
        }
    }
}
