using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using NLog;
using System;
using System.Net;
using WebSocketServer;
using OtpNet;
using System.Text;

namespace BalancerWSserversMultiClient
{
    public class Publisher
    {
        public string host;
        public string port;
        public string id;
        public string serverGuid;
        public int countConexions;
        public void SetcountConexions(int value) { countConexions = value; }
        public void SetHost(string value) { host = value; }
        public void Setport(string value) { port = value; }

    }
    public class BalancerOneClient : IProcessor
    {
        private Logger _logger;
        private string _id;
        private string _name;
        private int _portForWebClients;
        private string _hostForWebClients;
        private string _isSecure;
        private int _portForWebPublishers;
        private string _hostForWebPublishers;
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private Dictionary<string, object> _config = new Dictionary<string, object>();
        private List<Publisher> _serversFromBalance = new List<Publisher>();
        private List<string> _mssgTypes = new List<string>();
        private bool ModuleNoOut = true;
        private Server serverForWebClients;
        private Server serverForPublishers;
        private int totpStep;
        private int totpSize;


        private static Mutex mut = new Mutex();
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

        public void init(IConfigurator configurator, IGenericLogger logger, string id)
        {
            try
            {
                _id = id;
                _isSecure = "false";
                Dictionary<string, object> config = configurator.getMap("processors", _id);

                object value;
                if (!config.TryGetValue("name", out value))
                {
                    throw new Exception("No se encontro el parametro 'name' en la configuracion del procesador");
                }
                _name = value.ToString();

                if (_name is null || _name == "")
                    _logger = (Logger)logger.init("BalancerServersWS");
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
                
                if (!config.TryGetValue("serverForWebClients", out value))
                {
                    throw new Exception("No se encontro el parametro 'serverForWebClients' en la configuracion del procesador");
                }
                Dictionary<string, object> dServerForClients = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                if (!dServerForClients.TryGetValue("port", out value))
                {
                    throw new Exception("No se encontro el parametro 'port' en la configuracion 'serverForWebClients' del procesador");
                }
                string sPort = value.ToString();
                _portForWebClients = Convert.ToInt32(sPort);
                if (!dServerForClients.TryGetValue("host", out value))
                {
                    throw new Exception("No se encontro el parametro 'host' en la configuracion 'serverForWebClients' del procesador");
                }
                Dictionary<string, object> dirHost = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dirHost.TryGetValue("enableDinamic", out value))
                {
                    throw new Exception("No se encontro el parametro 'enableDinamic' en la configuracion 'serverForWebClients' del procesador");
                }
                bool enableDinamicServClient = Convert.ToBoolean(value.ToString());
                if (!enableDinamicServClient) //host para el servidor WebSocket asignado en el fichero de configuracion
                {
                    if (!dirHost.TryGetValue("ip", out value))
                    {
                        throw new Exception("No se encontro el parametro 'ip' en la configuracion 'serverForWebClients' del procesador");
                    }
                    _hostForWebClients = value.ToString();
                }
                if (!dServerForClients.TryGetValue("isSecure", out value))
                {
                    throw new Exception("No se encontro el parametro 'isSecure' en la configuracion 'serverForWebClients' del procesador");
                }
                _isSecure = value.ToString();



                if (!config.TryGetValue("serverForWebPublishers", out value))
                {
                    throw new Exception("No se encontro el parametro 'serverForWebPublishers' en la configuracion del procesador");
                }
                Dictionary<string, object> dServerForPublishers = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                if (!dServerForPublishers.TryGetValue("port", out value))
                {
                    throw new Exception("No se encontro el parametro 'port' en la configuracion 'serverForWebPublishers' del procesador");
                }
                sPort = value.ToString();
                _portForWebPublishers = Convert.ToInt32(sPort);
                if (!dServerForPublishers.TryGetValue("host", out value))
                {
                    throw new Exception("No se encontro el parametro 'host' en la configuracion 'serverForWebPublishers' del procesador");
                }
                dirHost = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dirHost.TryGetValue("enableDinamic", out value))
                {
                    throw new Exception("No se encontro el parametro 'enableDinamic' en la configuracion 'serverForWebPublishers' del procesador");
                }
                bool enableDinamicServPublisher = Convert.ToBoolean(value.ToString());
                if (!enableDinamicServPublisher) //host para el servidor WebSocket asignado en el fichero de configuracion
                {
                    if (!dirHost.TryGetValue("ip", out value))
                    {
                        throw new Exception("No se encontro el parametro 'ip' en la configuracion 'serverForWebPublishers' del procesador");
                    }
                    _hostForWebPublishers = value.ToString();
                }

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

                if (enableDinamicServClient)
                {
                    _logger.Debug($"Servidor WebSocketClient en IP asignado dinamicamente");
                    _logger.Trace("Antes de crear el Server");
                    this.serverForWebClients = new Server(new IPEndPoint(IPAddress.Any, _portForWebClients));
                    _logger.Trace("Despues de crear el Server");
                }
                else
                {
                    _logger.Debug($"Servidor WebSocketClient en IP asignado estaticamente en {IPAddress.Parse(_hostForWebClients).ToString()} y puerto {_portForWebClients}");
                    _logger.Trace("Antes de crear el Server");
                    this.serverForWebClients = new Server(new IPEndPoint(IPAddress.Parse(_hostForWebClients), _portForWebClients));
                    _logger.Trace("Despues de crear el Server");
                }
                if (enableDinamicServPublisher)
                {
                    _logger.Debug($"Servidor WebSocketPublisher en IP asignado dinamicamente");
                    _logger.Trace("Antes de crear el Server");
                    this.serverForPublishers = new Server(new IPEndPoint(IPAddress.Any, _portForWebPublishers));
                    _logger.Trace("Despues de crear el Server");
                }
                else
                {
                    _logger.Debug($"Servidor WebSocketPublisher en IP asignado estaticamente en {IPAddress.Parse(_hostForWebPublishers).ToString()} y puerto {_portForWebClients}");
                    _logger.Trace("Antes de crear el Server");
                    this.serverForPublishers = new Server(new IPEndPoint(IPAddress.Parse(_hostForWebPublishers), _portForWebPublishers));
                    _logger.Trace("Despues de crear el Server");
                }

                initEvent();
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

                //ToDo: Procesar dato de entrada

                _logger.Trace("Fin");
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

        private int makeBalance()
        {
            try
            {
                _logger.Trace("Inicio");
                int iServer = -1;
                if (_serversFromBalance.Count == 1)
                {
                    iServer = 0;
                    return iServer;
                }
                else
                {
                    mut.WaitOne();
                    uint bestServer = 60000;
                    for (int i = 0; i < _serversFromBalance.Count; i++)
                    {
                        if (_serversFromBalance[i].countConexions < bestServer)
                        {
                            bestServer = (uint)_serversFromBalance[i].countConexions;
                            iServer = i;
                        }
                    }
                    mut.ReleaseMutex();
                }

                _logger.Trace("Fin");
                return iServer;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }

        private bool SearchPublisher(string serverId, out int iterator)
        {
            try
            {
                if (_serversFromBalance.Count > 1)
                {
                    for (int index = 0; index < _serversFromBalance.Count; index++)
                    {
                        if (_serversFromBalance[index].id == serverId)
                        {
                            iterator = index;
                            return true;
                        }
                    }
                }
                iterator = -1;
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
            serverForWebClients.OnClientConnected += (object sender, OnClientConnectedHandler e) =>
            {
                _logger.Debug($"Nueva Conexion desde el cliente web: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };

            serverForWebClients.OnClientDisconnected += (object sender, OnClientDisconnectedHandler e) =>
            {
                _logger.Debug($"Conexion cerrada desde el cliente web: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };

            serverForWebClients.OnMessageReceived += (object sender, OnMessageReceivedHandler e) =>
            {
                _logger.Debug($"Mensaje recivido: '{e.GetMessage()}' desde el cliente: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
                
                //ToDo: Procesar el mensaje recibido desde el cliente.

                int index = makeBalance();
                Dictionary<string, object> dicMssg = new Dictionary<string, object>();

                if (index == -1)
                {
                    _logger.Error($"Solicitud de conexion desde {e.GetClient().GetSocket().RemoteEndPoint.ToString()}, pero no hay publicadores instanciados aun");
                    dicMssg.Add("errorCode", "404");
                    dicMssg.Add("errorDescription", "No se encontraron Publisher WebSocket Server disponibles, intente nuevamente en unos minutos o pongase en contacto con los administradores si el problema persiste");
                }
                else
                {
                    dicMssg.Add("errorCode", "200");
                    dicMssg.Add("errorDescription", "Petición completada con éxito");
                    Dictionary<string, object> dicData = new Dictionary<string, object>();
                    dicData.Add("host", _serversFromBalance[index].host);
                    dicData.Add("port", _serversFromBalance[index].port);
                    dicData.Add("isSecure", "false");

                    byte[] bytes = Encoding.ASCII.GetBytes(_serversFromBalance[index].serverGuid);
                    string strTotp = GetTotpValue(bytes);
                    dicData.Add("totp", strTotp);
                    dicMssg.Add("response", dicData);
                }
                string strMssg = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicMssg);
                serverForWebClients.SendMessage(e.GetClient(), strMssg);
            };

            serverForWebClients.OnSendMessage += (object sender, OnSendMessageHandler e) =>
            {
                _logger.Debug($"Mensaje enviado: '{e.GetMessage()}' al cliente: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };

            serverForPublishers.OnClientConnected += (object sender, OnClientConnectedHandler e) =>
            {
                _logger.Debug($"Nueva Conexion desde el publicador: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
                serverForPublishers.SendMessage(e.GetClient(), "connected");
            };

            serverForPublishers.OnClientDisconnected += (object sender, OnClientDisconnectedHandler e) =>
            {
                string serverId = null;
                int index;
                mut.WaitOne();
                for (int iterator = 0; iterator < serverForPublishers.GetConnectedClientCount(); iterator++)
                {
                    if (SearchPublisher( serverForPublishers.GetConnectedClient(iterator).GetGuid(), out index))
                    {
                        _logger.Debug($"Removido el publicador {_serversFromBalance[index].id}");
                        _serversFromBalance.RemoveAt(index);
                        break;
                    }
                }
                mut.ReleaseMutex();
            };

            serverForPublishers.OnMessageReceived += (object sender, OnMessageReceivedHandler e) =>
            {
                _logger.Debug($"Mensaje recivido : '{e.GetMessage()}' desde el publicador: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");


                Dictionary<string, object> dPayLoad = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(e.GetMessage());
                object value;
                if (!dPayLoad.TryGetValue("Status", out value))
                {
                    _logger.Error("Falta el atributo 'Status' en el mensaje para procesar");
                }
                Dictionary<string, object> dStatus = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dPayLoad.TryGetValue("eventType", out value))
                {
                    _logger.Error("Falta el atributo 'eventType' en el mensaje para procesar");
                }
                string eventType = value.ToString();
                string serverId = e.GetClient().GetGuid();


                #region ProcesarAddOrRemoveConnexion
                if (eventType == "newConnexion" || eventType == "closeConnexion")
                {
                    if (!dStatus.TryGetValue("connectionsCount", out value))
                    {
                        _logger.Error("Falta el atributo 'connectionsCount' en el mensaje para procesar");
                        return;
                    }
                    string sConnexions = value.ToString();
                    int iConnexions = Convert.ToInt32(sConnexions);
                    bool findServer = false;
                    
                    //ToDo: procesar el resto de los parametros en 'Status'

                    mut.WaitOne();
                    if (_serversFromBalance.Count < 1)
                    {
                        _logger.Error($"No hay Publicadores registrados para balancear");
                        return;
                    }
                    for (int i = 0; i < _serversFromBalance.Count; i++)
                    {
                        if (_serversFromBalance[i].id == serverId)
                        {
                            findServer = true;
                            if (eventType == "newConnexion")
                            {
                                _serversFromBalance[i].SetcountConexions(iConnexions);
                                _logger.Debug($"Nueva conexion en servidor con id {_serversFromBalance[i].serverGuid}");
                            }
                            else
                            {
                                _serversFromBalance[i].SetcountConexions(iConnexions);
                                _logger.Debug($"Removida una conexion en servidor con id {_serversFromBalance[i].serverGuid}");
                            }
                        }
                    }
                    _logger.Debug($"Cantidad de servidores -> {_serversFromBalance.Count}");
                    for (int i = 0; i < _serversFromBalance.Count; i++)
                    {
                        _logger.Debug($"Servidor {_serversFromBalance[i].serverGuid} -> {_serversFromBalance[i].countConexions}");
                    }
                    mut.ReleaseMutex();
                    if (!findServer)
                    {
                        _logger.Debug($"La conexion '{serverId}' no esta registrada en el Balanceador");
                    }
                }
                #endregion ProcesarAddOrRemoveConnexion

                #region ProcesarAddServer 
                else if (eventType == "addServer")
                {
                    if (!dPayLoad.TryGetValue("publicWebSocketParameters", out value))
                    {
                        _logger.Error("Falta el atributo 'publicWebSocketParameters' en el mensaje para procesar");
                    }
                    Dictionary<string, object> publicWebSocketParameters = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                    if (!publicWebSocketParameters.TryGetValue("host", out value))
                    {
                        _logger.Error("Falta el atributo 'host' en el mensaje para procesar");
                        return;
                    }
                    string sHost = value.ToString();
                    if (!publicWebSocketParameters.TryGetValue("port", out value))
                    {
                        _logger.Error("Falta el atributo 'port' en el mensaje para procesar");
                        return;
                    }
                    string sPort = value.ToString();
                    if (!publicWebSocketParameters.TryGetValue("publisherWebSocketServerId", out value))
                    {
                        _logger.Error("Falta el atributo 'publisherWebSocketServerId' en el mensaje para procesar");
                        return;
                    }
                    string sGuid = value.ToString();

                    //ToDo: procesar el resto de los parametros en 'Status'

                    mut.WaitOne();
                    if (_serversFromBalance.Count < 1)
                    {
                        Publisher newServer = new Publisher();
                        newServer.id = serverId;
                        newServer.host = sHost;
                        newServer.port = sPort;
                        newServer.serverGuid = sGuid;
                        newServer.countConexions = 0;
                        _serversFromBalance.Add(newServer);
                        _logger.Debug($"Agregado el publicador con id '{sGuid}'");
                    }
                    else if (_serversFromBalance.Count > 1)
                    {
                        bool isNew = true;
                        for (int index = 0; index < _serversFromBalance.Count; index++)
                        {
                            if (_serversFromBalance[index].id == serverId)
                            {
                                isNew = false;
                                _logger.Debug($"El publicador con id '{sGuid}' ya estaba previamente registrado");
                                break;
                            }
                        }
                        if (isNew)
                        {
                            Publisher newServer = new Publisher();
                            newServer.id = serverId;
                            newServer.host = sHost;
                            newServer.port = sPort;
                            newServer.serverGuid = sGuid;
                            newServer.countConexions = 0;
                            _serversFromBalance.Add(newServer);
                            _logger.Debug($"Agregado el publicador con id '{sGuid}'");
                        }
                    }
                    mut.ReleaseMutex();
                }
                #endregion ProcesarAddServer             
            };

            serverForPublishers.OnSendMessage += (object sender, OnSendMessageHandler e) =>
            {
                _logger.Debug($"Mensaje enviado: '{e.GetMessage()}' al publicador: {e.GetClient().GetSocket().RemoteEndPoint.ToString()}");
            };

        }

        private string GetTotpValue(byte[] ClientId)
        {
            try
            {
                _logger.Trace("Inicio");

                var totp = new Totp(ClientId, step: totpStep, mode: OtpHashMode.Sha1, totpSize: totpSize);

                _logger.Trace("Fin");
                return totp.ComputeTotp();
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
    }
}