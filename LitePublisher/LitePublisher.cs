using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using NLog;
using System.Net;
using WebSocketServer;


namespace LitePublisher
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
        private List<string> _mssgTypes = new List<string>();
        private List<string> _mssgTypesName = new List<string>();


        private static Mutex mut = new Mutex();
        private bool forceDisconetClient = false;
        private Dictionary<string, string> _dRecipient = new Dictionary<string, string>();


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
                    _logger = (Logger)logger.init("LitePublisherWS");
                else
                    _logger = (Logger)logger.init(_name);


                if (!config.TryGetValue("serverForWebClients", out value))
                {
                    throw new Exception("No se encontro el parametro 'serverForWebClients' en la configuracion del procesador");
                }
                Dictionary<string, object> dirserverPublisher = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dirserverPublisher.TryGetValue("host", out value))
                {
                    throw new Exception("No se encontro el parametro 'host' en la configuracion 'serversBalancer' del procesador");
                }
                _host = value.ToString();
                if (!dirserverPublisher.TryGetValue("port", out value))
                {
                    throw new Exception("No se encontro el parametro 'port' en la configuracion 'serversBalancer' del procesador");
                }
                _port = Convert.ToInt32(value.ToString());


                if (config.TryGetValue("mssgtypes", out value))
                {
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

                        if (!currentPub.TryGetValue("name", out value))
                        {
                            throw new Exception($"No se encontro el parametro 'name' en la configuracion del mssgType '{index + 1}'");
                        }

                        _mssgTypesName.Add(value.ToString());
                    }
                    tempList.Clear();
                }
                if (!config.TryGetValue("recipientsNames", out value))
                {
                    throw new Exception("No se encontro el parametro 'recipientsNames' en la configuracion del procesador");
                }
                _dRecipient = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(value.ToString());

                if (!_dRecipient.ContainsKey("publish"))
                {
                    throw new Exception("No se encontro el parametro 'publish' en la configuracion 'recipientsNames' del procesador");
                }
                if (!_dRecipient.ContainsKey("clearConnections"))
                {
                    throw new Exception("No se encontro el parametro 'clearConnections' en la configuracion 'recipientsNames' del procesador");
                }

                this.server = new Server(new IPEndPoint(IPAddress.Parse(_host), _port));

                initEvent();

            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        
        public void addSerializer(ISerializer serializer)
        {
            try
            {
                _logger.Trace("Inicio");
                _listSerializer.Add(serializer);

                Dictionary<string, object> dataOut = new Dictionary<string, object>();

                string strdata = "{\"action\":\"resetConections\"}";
                string strmetadata = "{\"typeTask\":\"publisher\"}";

                dataOut.Add("data", strdata);
                dataOut.Add("metadata", strmetadata);

                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypesName.Count; indexTM++)
                    {
                        if (_mssgTypesName[indexTM] == "UpdateConnection")
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut, "HIGH");
                        }
                    }
                }

                _logger.Trace("Fin");
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        public bool proccess(object payload, object metadata)
        {
            try
            {
                _logger.Trace("Inicio");

                Dictionary<string, object> dMetadata = (Dictionary<string, object>)metadata;
                string recipientName = dMetadata["recipientName"].ToString();

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

                if (_dRecipient["publish"].ToString() == recipientName)    //procesar mensajes para publicar
                {

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
                    string requestId;
                    string ackCode;
                    string mssgCode;

                    Dictionary<string, object> dicMssg = new Dictionary<string, object>();

                    if (!meta.TryGetValue("publisherWebSocketServerCnxId", out value))
                    {
                        _logger.Error("Falta el atributo 'publisherWebSocketServerCnxId' en el mensaje para procesar");
                        return false;
                    }
                    PublisherWebSocketServerCnxId = value.ToString();

                    if (meta.TryGetValue("requestId", out value))
                    {
                        requestId = value.ToString();
                        dicMssg.Add("requestId", requestId);
                    }

                    if (meta.TryGetValue("errorCode", out value))
                    {
                        ackCode = value.ToString();
                        dicMssg.Add("errorCode", ackCode);
                    }

                    if (meta.TryGetValue("errorDescription", out value))
                    {
                        mssgCode = value.ToString();
                        dicMssg.Add("errorDescription", mssgCode);
                    }

                    mut.WaitOne();
                    Client client = server.GetConnectedClient(PublisherWebSocketServerCnxId);
                    if (client is null)
                    {
                        mut.ReleaseMutex();
                        _logger.Error($"No se encontro la conexion '{PublisherWebSocketServerCnxId}' asociada a ningun cliente");
                        return false;
                    }
                    mut.ReleaseMutex();

                    dicMssg.Add("response", dStrData);

                    string strMssg = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg);
                    server.SendMessage(client, strMssg);

                    _logger.Trace("Fin");
                    return true;
                }
                
                else if (_dRecipient["clearConnections"].ToString() == recipientName)  //procesar mensajes limpiar toda loas conexiones websockets
                {
                    if(data.ContainsKey("action"))
                    {
                        mut.WaitOne();
                        for (int i= 0; i< server.GetConnectedClientCount(); i++)
                        {
                            server.ClientDisconnect(server.GetConnectedClient(i));
                        }
                        mut.ReleaseMutex();
                        return true;
                    }
                }
                return false;
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

                Dictionary<string, string> dicMssg = new Dictionary<string, string>();
                Dictionary<string, object> dicMssg1 = new Dictionary<string, object>();
                dicMssg.Add("publisherWebSocketServerCnxId", e.GetClient().GetGuid());
                dicMssg1.Add("errorCode", "200");
                dicMssg1.Add("mssgCode", "OK");
                dicMssg1.Add("response", dicMssg);
                string strMssg1 = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg1);

                server.SendMessage(e.GetClient(), strMssg1);
            };

            server.OnClientDisconnected += (object sender, OnClientDisconnectedHandler e) =>
            {
                if (forceDisconetClient is false)
                {
                    _logger.Debug($"Conexion cerrada desde: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
                }
                forceDisconetClient = false;

                Dictionary<string, object> dataOut = new Dictionary<string, object>();

                string strdata = "{\"action\":\"clientDisconected\",\"client\":\"" + e.GetClient().GetGuid() + "\"}";
                string strmetadata = "{\"typeTask\":\"publisher\"}";

                dataOut.Add("data", strdata);
                dataOut.Add("metadata", strmetadata);

                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypesName.Count; indexTM++)
                    {
                        if (_mssgTypesName[indexTM] == "RemoveUser")
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut,"HIGH");
                        }
                    }
                }
            };

            server.OnMessageReceived += (object sender, OnMessageReceivedHandler e) =>
            {
                _logger.Debug($"Mensaje recivido: '{e.GetMessage()}' desde el cliente: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };

            server.OnSendMessage += (object sender, OnSendMessageHandler e) =>
            {
                _logger.Debug($"Mensaje enviado: '{e.GetMessage()}' al cliente: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };
        }
    }
}