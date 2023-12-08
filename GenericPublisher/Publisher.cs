using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using Definitions;
using NLog;
using System;
using System.Net;
using WebSocketServer;
using System.Net.Sockets;
using Websocket.Client;
using System.Text;
using OtpNet;
using System.Net.WebSockets;

namespace GenericPublisher
{

    public class Publisher : IProcessor  
    {
        private Logger _logger;
        private string _id;
        private string _PublisherId;
        private string _name;
        private string _host;
        private int _port;
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private Dictionary<string, object> _config = new Dictionary<string, object>();
        private Server server;
        private bool ModuleNoOut = true;
        private List<string> _mssgTypes = new List<string>();
        private WebsocketClient _webSocket = null;
        private string _uri;

        private static Mutex mut = new Mutex();
        private int totpStep;
        private int totpSize;
        private bool forceDisconetClient = false;


        public void init(IConfigurator configurator, IGenericLogger logger, string id)
        {
            try
            {
                _id = id;
                _PublisherId = Guid.NewGuid().ToString();
                Dictionary<string, object> config = configurator.getMap("processors", _id);

                object value;
                if (!config.TryGetValue("name", out value))
                {
                    throw new Exception("No se encontro el parametro 'name' en la configuracion del procesador");
                }
                _name = value.ToString();
                if (_name is null || _name == "")
                    _logger = (Logger)logger.init("PublisherWS");
                else
                    _logger = (Logger)logger.init(_name);



                if (!config.TryGetValue("totp", out value))
                {
                    throw new Exception("No se encontro el parametro 'totp' en la configuracion del procesador");
                }
                Dictionary<string, object> dTotp = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                if (!dTotp.TryGetValue("step", out value))
                {
                    throw new Exception("No se encontro el parametro 'step' en la configuracion del procesador");
                }
                totpStep = Convert.ToInt32(value.ToString());
                if (!dTotp.TryGetValue("size", out value))
                {
                    throw new Exception("No se encontro el parametro 'size' en la configuracion del procesador");
                }
                totpSize = Convert.ToInt32(value.ToString());

                if (!config.TryGetValue("serversBalancer", out value))
                {
                    throw new Exception("No se encontro el parametro 'serversBalancer' en la configuracion del procesador");
                }
                Dictionary<string, object> dirserverBalancer = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dirserverBalancer.TryGetValue("host", out value))
                {
                    throw new Exception("No se encontro el parametro 'host' en la configuracion 'serversBalancer' del procesador");
                }
                string host = value.ToString();
                if (!dirserverBalancer.TryGetValue("port", out value))
                {
                    throw new Exception("No se encontro el parametro 'port' en la configuracion 'serversBalancer' del procesador");
                }
                string port = value.ToString();

                _uri = "ws://" + host + ":" + port + "/";
                _logger.Debug("URL del Balancer '{0}'", _uri);

                var url = new Uri(_uri);

                if (config.TryGetValue("mssgTypes", out value))
                {
                    //throw new Exception("No se encontro el parametro 'mssgTypes' en la configuracion del procesador");
                    ModuleNoOut = false;
                    List<object> tempList = System.Text.Json.JsonSerializer.Deserialize<List<object>>(value.ToString());
                    if (tempList.Count < 1)
                    {
                        throw new Exception($"No se configuro ningun 'mssgType' para usar como parametro en la salida del modulo");
                    }
                    for (int index = 0; index < tempList.Count; index++)
                    {
                        Dictionary<string, object> currentPub = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(tempList[index].ToString());

                        if (!currentPub.TryGetValue("id", out value))
                        {
                            throw new Exception($"No se encontro el parametro 'id' en la configuracion del mssgType '{index + 1}'");
                        }

                        _mssgTypes.Add(value.ToString());
                    }
                    tempList.Clear();
                }

                if (!config.TryGetValue("serversPublisher", out value))
                {
                    throw new Exception("No se encontro el parametro 'serversPublisher' en la configuracion del procesador");
                }
                Dictionary<string, object> dirserverPublisher = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dirserverPublisher.TryGetValue("host", out value))
                {
                    throw new Exception("No se encontro el parametro 'host' en la configuracion 'serversPublisher' del procesador");
                }
                Dictionary<string, object> dirHost = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dirHost.TryGetValue("enableDinamic", out value))
                {
                    throw new Exception("No se encontro el parametro 'enableDinamic' en la configuracion 'serversPublisher' del procesador");
                }
                bool enableDinamic = Convert.ToBoolean(value.ToString());
                string ip;
                _port = FreeTcpPort();

                if (!enableDinamic) //host para el servidor WebSocket asignado en el fichero de configuracion
                {
                    if (!dirHost.TryGetValue("ip", out value))
                    {
                        throw new Exception("No se encontro el parametro 'ip' en la configuracion 'serversPublisher' del procesador");
                    }
                    ip = value.ToString();

                    _logger.Debug($"Servidor WebSocketClient en IP asignado estaticamente en {ip}");
                    _logger.Trace("Antes de crear el Server");
                    this.server = new Server(new IPEndPoint(IPAddress.Parse(ip), _port));
                    _logger.Trace("Despues de crear el Server");

                    _host = ip;
                }
                else  //host para el servidor WebSocket gestionado dinamicamnete
                {
                    _logger.Debug($"Servidor WebSocketClient en IP asignado dinamicamente");
                    this.server = new Server(new IPEndPoint(IPAddress.Any, _port));
                    _host = Dns.GetHostEntry(Dns.GetHostName()).HostName;
                }

                _webSocket = new WebsocketClient(url);
                _webSocket.ReconnectTimeout = null;
                _webSocket.ErrorReconnectTimeout = TimeSpan.FromSeconds(10);
                _webSocket.IsReconnectionEnabled = true;
                _webSocket.Start(); 

                initEvent();

            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        
        static int FreeTcpPort()
        {
            TcpListener l = new TcpListener(IPAddress.Loopback, 0);
            l.Start();
            int port = ((IPEndPoint)l.LocalEndpoint).Port;
            l.Stop();
            return port;
        }

        public void addSerializer(ISerializer serializer)
        {
            try
            {
                _logger.Trace("Inicio");
                _listSerializer.Add(serializer);
                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool proccess(object payload, object metadata = null)
        {
            try
            {
                _logger.Trace("Inicio");

                //toDo: Logica del procesador
                Dictionary<string, object> dPayLoad = (Dictionary<string, object>)payload;                

                object value;

                if (!dPayLoad.TryGetValue("data", out value))
                {
                    _logger.Error("Falta el atributo 'data' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string, object> data = new Dictionary<string, object>();
                try
                {
                    data = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                }
                catch (Exception ex)
                {
                    _logger.Error("Error deserializando el contenido de 'data'");
                    return false;
                }
                Dictionary<string, object> dStrData = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                if (!dPayLoad.TryGetValue("metadata", out value))
                {
                    _logger.Error("Falta el atributo 'metadata' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string, object> meta = new Dictionary<string, object>();
                try
                {
                    meta = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                }
                catch (Exception ex)
                {
                    _logger.Error("Error deserializando el contenido de 'meta'");
                    return false;
                }
                string PublisherWebSocketServerCnxId;
                string PublisherWebSocketServerId;
                string requestId;
                string ackCode;
                string mssgCode;


                if (!meta.TryGetValue("publisherWebSocketServerCnxId", out value))
                {
                    _logger.Error("Falta el atributo 'publisherWebSocketServerCnxId' en el mensaje para procesar");
                    return false;
                }
                PublisherWebSocketServerCnxId = value.ToString();

                if (!meta.TryGetValue("publisherWebSocketServerId", out value))
                {
                    _logger.Error("Falta el atributo 'publisherWebSocketServerCnxId' en el mensaje para procesar");
                    return false;
                }
                PublisherWebSocketServerId = value.ToString();

                if (!meta.TryGetValue("requestId", out value))
                {
                    _logger.Error("Falta el atributo 'requestId' en el mensaje para procesar");
                    return false;
                }
                requestId = value.ToString();

                if (!meta.TryGetValue("errorCode", out value))
                {
                    _logger.Error("Falta el atributo 'errorCode' en el mensaje para procesar");
                    return false;
                }
                ackCode = value.ToString();

                if (!meta.TryGetValue("errorDescription", out value))
                {
                    _logger.Error("Falta el atributo 'errorDescription' en el mensaje para procesar");
                    return false;
                }
                mssgCode = value.ToString();

                if (_PublisherId == PublisherWebSocketServerId)
                {
                    mut.WaitOne();
                    Client client = server.GetConnectedClient(PublisherWebSocketServerCnxId);
                    if (client is null)
                    {
                        mut.ReleaseMutex();
                        _logger.Error($"No se encontro la conexion '{PublisherWebSocketServerCnxId}' asociada a ningun cliente");
                        return false;
                    }
                    mut.ReleaseMutex();

                    Dictionary<string, object> dicMssg = new Dictionary<string, object>();
                    dicMssg.Add("requestId", requestId);
                    dicMssg.Add("errorCode", ackCode);
                    dicMssg.Add("errorDescription", mssgCode);
                    dicMssg.Add("response", dStrData);

                    string strMssg = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg);
                    server.SendMessage(client, strMssg);
                }

                _logger.Trace("Fin");
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool reConfig()
        {
            try
            {
                return false;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        
        private void initEvent()
        {
            server.OnClientConnected += (object sender, OnClientConnectedHandler e) =>
            {
                _logger.Debug($"Nueva Conexion desde: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");

                //Respuesta del Publicador al cliente web al conectarse
                //{ "code": "202","mssgCode":"Conexion establecida, en espera de parametros de autenticacion"}
                Dictionary<string, string> dicMssg = new Dictionary<string, string>();
                Dictionary<string, object> dicMssg1 = new Dictionary<string, object>();
                dicMssg1.Add("errorCode", "202");
                dicMssg1.Add("mssgCode", "Conexión establecida, en espera de parámetros de autenticación");
                string strMssg1 = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg1);

                server.SendMessage(e.GetClient(), strMssg1);

            };

            server.OnClientDisconnected += (object sender, OnClientDisconnectedHandler e) =>
            {
                if (forceDisconetClient is false)
                {
                    _logger.Debug($"Conexion cerrada desde: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");

                    //Desconexion en el publicador x
                    //{ "Status":{ "ConnectionsCount "CpuUsage "RamUsage "NetworkBandwidthUsage "EventType":"closeConnexion"}

                    Dictionary<string, object> dataOut = new Dictionary<string, object>();
                    Dictionary<string, object> Status = new Dictionary<string, object>();

                    Status.Add("connectionsCount", server.GetConnectedClientCount());
                    Status.Add("cpuUsage", 0.0);
                    Status.Add("ramUsage", 0.0);
                    Status.Add("networkBandwidthUsage", 0.0);
                    dataOut.Add("eventType", "closeConnexion");
                    dataOut.Add("Status", Status);
                    string strDataOut = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dataOut);
                    if (_webSocket.IsRunning)
                        _webSocket.Send(strDataOut);
                    else
                    {
                        _logger.Error("Se perdio la conexion con el Balancer");
                    }
                }
                forceDisconetClient = false;
            };

            server.OnMessageReceived += (object sender, OnMessageReceivedHandler e) =>
            {
                _logger.Debug($"Mensaje recivido: '{e.GetMessage()}' desde el cliente: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
                //si 'code' = 200, el cliente envia al publicador//
                //{"AutorizationId": "", "TotpCode": ""}

                Dictionary<string, string> dicMssg = new Dictionary<string, string>();
                Dictionary<string, object> dicMssg1 = new Dictionary<string, object>();
                string strMssg1 = null;

                Dictionary<string, object> dMssg = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(e.GetMessage());

                object value;
                bool error = false;

                if (!dMssg.TryGetValue("totp", out value))
                {
                    error = true;
                    _logger.Error("Falta el parametro 'totp' en la solicitud de conexion desde el cliente");
                    dicMssg1.Add("errorCode", "400");
                    dicMssg1.Add("errorDescription", "Petición errónea. El mensaje enviado no satisface el esquema esperado");
                    strMssg1 = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg1);
                    server.SendMessage(e.GetClient(), strMssg1);
                    forceDisconetClient = true;
                    server.ClientDisconnect(e.GetClient());
                    return;
                }
                string sTotp = value.ToString();

                byte[] bytes = Encoding.ASCII.GetBytes(this._PublisherId);

                if (this.IsTotpValid(bytes, sTotp))
                {

                    dicMssg.Add("publisherWebSocketServerCnxId", e.GetClient().GetGuid());
                    dicMssg.Add("publisherWebSocketServerId", this._PublisherId);
                    
                    dicMssg1.Add("response", dicMssg);
                    dicMssg1.Add("errorCode", "200");
                    dicMssg1.Add("errorDescription", "Petición completada con éxito");

                    strMssg1 = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg1);
                    server.SendMessage(e.GetClient(), strMssg1);

                    Dictionary<string, object> dataOut = new Dictionary<string, object>();
                    Dictionary<string, object> Status = new Dictionary<string, object>();

                    Status.Add("connectionsCount", server.GetConnectedClientCount());
                    Status.Add("cpuUsage", 0.0);
                    Status.Add("ramUsage", 0.0);
                    Status.Add("networkBandwidthUsage", 0.0);

                    dataOut.Add("eventType", "newConnexion");
                    dataOut.Add("Status", Status);
                    string strDataOut = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dataOut);

                    if (_webSocket.IsRunning)
                        _webSocket.Send(strDataOut);
                    else
                    {
                        _logger.Error("Se perdio la conexion con el Balanceador");
                    }
                }
                else
                {
                    //{ "code": "401","mssgCode":"Cliente no autorizado"}
                    dicMssg1.Add("errorCode", "401");
                    dicMssg1.Add("errorDescription", "No está autorizado");
                    strMssg1 = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg1);
                    server.SendMessage(e.GetClient(), strMssg1);
                    forceDisconetClient = true;
                    server.ClientDisconnect(e.GetClient());
                }
            };

            server.OnSendMessage += (object sender, OnSendMessageHandler e) =>
            {
                _logger.Debug($"Mensaje enviado: '{e.GetMessage()}' al cliente: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };

            _webSocket.ReconnectionHappened.Subscribe(info =>
            {
                _logger.Debug("Intento de reconexion con el Balanceador, type: " + info.Type);
            });

            _webSocket.MessageReceived.Subscribe(msg =>
            {
                _logger.Debug("Mensaje recibido desde el Balanceador: " + msg);

                Dictionary<string, object> dataOut = new Dictionary<string, object>();
                Dictionary<string, object> Status = new Dictionary<string, object>();
                Dictionary<string, object> Params = new Dictionary<string, object>();

                Params.Add("host", _host);
                Params.Add("port", _port);
                Params.Add("publisherWebSocketServerId", this._PublisherId);

                Status.Add("connectionsCount", server.GetConnectedClientCount());
                Status.Add("cpuUsage", 0.0);
                Status.Add("ramUsage", 0.0);
                Status.Add("networkBandwidthUsage", 0.0);

                dataOut.Add("eventType", "addServer");
                dataOut.Add("Status",Status);
                dataOut.Add("publicWebSocketParameters", Params);

                string strDataOut = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dataOut);

                if (msg.ToString().ToLower() == "connected")
                {
                    if (_webSocket.IsStarted)
                        _webSocket.Send(strDataOut);
                    else
                    {
                        _logger.Error("Se perdio la conexion con el Balanceador");
                    }
                }

            });

            _webSocket.DisconnectionHappened.Subscribe(info =>
            {
                _logger.Error("Conexion perdida con el Balanceador, type: " + info.Type);
            });
        
        }

        private bool IsTotpValid(byte[] bytes, string totpCode)
        {
            try
            {
                _logger.Trace("Inicio");

                var totp = new Totp(bytes, step: totpStep, mode: OtpHashMode.Sha1, totpSize: totpSize);
                long windows;
                _logger.Trace("Fin");
                return totp.VerifyTotp(totpCode, out windows);
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}