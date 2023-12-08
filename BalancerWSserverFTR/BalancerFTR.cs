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
using System.Diagnostics;

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
        private int _portForWebPublishers;
        private IConfigurator _configurator;
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private Dictionary<string, object> _config = new Dictionary<string, object>();
        private List<Publisher> _serversFromBalance = new List<Publisher>();
        private List<string> _mssgTypes = new List<string>();
        private bool ModuleNoOut = true;
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

        private void getConfig()
        {
            try
            {
                _logger.Trace("Inicio");
                Dictionary<string, object> config = _configurator.getMap("processors", _id);

                object value;
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

                if (!config.TryGetValue("serverForWebPublishers", out value))
                {
                    throw new Exception("No se encontro el parametro 'serverForWebPublishers' en la configuracion del procesador");
                }
                Dictionary<string, object> dServerForPublishers = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                if (!dServerForPublishers.TryGetValue("port", out value))
                {
                    throw new Exception("No se encontro el parametro 'port' en la configuracion 'serverForWebPublishers' del procesador");
                }
                string sPort = value.ToString();
                _portForWebPublishers = Convert.ToInt32(sPort);

                if (!config.TryGetValue("mssgtypes", out value))
                {
                    throw new Exception("No se encontro el parametro 'mssgtypes' en la configuracion del procesador");
                }
                List<Dictionary<string, object>> MssgsTypes = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(value.ToString());

                int index = 0;
                foreach (Dictionary<string, object> keyValuePairs in MssgsTypes)
                {
                    object valueId;
                    if (keyValuePairs.TryGetValue("id", out valueId))
                    {
                        this._mssgTypes.Add(valueId.ToString());
                    }
                    else
                    {
                        throw new Exception($"No se encontro el parametro 'mssgtypes[{index}].id' en la configuracion del procesador");
                    }
                    index++;
                }
                _logger.Trace("Fin");
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        public void init(IConfigurator configurator, IGenericLogger logger, string id)
        {
            try
            {
                _id = id;
                _configurator = configurator;
                Dictionary<string, object> config = configurator.getMap("processors", _id);

                object value;
                if (!config.TryGetValue("name", out value))
                {
                    throw new Exception("No se encontro el parametro 'name' en la configuracion del procesador");
                }
                _name = value.ToString();

                if (_name is null || _name == "")
                    _logger = (Logger)logger.init("BalancerServersFTR");
                else
                    _logger = (Logger)logger.init(_name);

                getConfig();

                this.serverForPublishers = new Server(new IPEndPoint(IPAddress.Any, _portForWebPublishers));

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

                Dictionary<string, object> dPayLoad = (Dictionary<string, object>)payload;
                object value;

                if (dPayLoad.TryGetValue("adminCommand", out value)) //comandos de administracion, 'reconfig', 'kill' 
                {
                    string adminCommand = value.ToString();
                    if (adminCommand == "reconfig")
                    {
                        _logger.Debug($"Comando de administracion '{adminCommand}' ejecutado");
                        this.reConfig();
                    }
                    else if (adminCommand == "kill")
                    {
                        _logger.Debug($"Comando de administracion '{adminCommand}' ejecutado");
                        var currentProcess = Process.GetCurrentProcess();
                        currentProcess.Kill();
                    }
                    else
                    {
                        _logger.Error($"Comando de administracion '{adminCommand}' no reconocido");
                    }
                    return true;
                }

                if (!dPayLoad.TryGetValue("metadata", out value))
                {
                    _logger.Error("Falta el atributo 'metadata' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string, object> meta = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                if (!dPayLoad.TryGetValue("data", out value))
                {
                    _logger.Error("Falta el atributo 'data' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string, object> data = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                string strData = value.ToString();

                int indexPublisherFTR = makeBalance();

                data.Clear();
                if (indexPublisherFTR == -1)
                {
                    data.Add("errorCode", "404");
                    data.Add("errorDescription", "No se encontraron Publicadores de FTR disponibles, intente nuevamente en unos minutos o pongase en contacto con los administradores si el problema persiste");
                }
                else
                {
                    data.Add("errorCode", "200");
                    data.Add("errorDescription", "Petición completada con éxito");
                    Dictionary<string, object> dicData = new Dictionary<string, object>();
                    dicData.Add("host", _serversFromBalance[indexPublisherFTR].host);
                    dicData.Add("port", _serversFromBalance[indexPublisherFTR].port);
                    dicData.Add("isSecure", "false");

                    byte[] bytes = Encoding.ASCII.GetBytes(_serversFromBalance[indexPublisherFTR].serverGuid);
                    string strTotp = GetTotpValue(bytes);
                    dicData.Add("totp", strTotp);
                    data.Add("response", dicData);
                }
                Dictionary<string, object> dataOut = new Dictionary<string, object>();

                dataOut.Add("data", data);
                dataOut.Add("metadata", meta);

                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                    {
                        _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                    }
                }
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
                if (_configurator.reLoad())
                {
                    mut.WaitOne();
                    getConfig();
                    mut.ReleaseMutex();
                    return true;
                }
                return false;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
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