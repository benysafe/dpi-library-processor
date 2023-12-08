using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using NLog;
using System.Linq;

namespace XymaAlarmCenterProcessorFTR
{

    public class ProcessorFTR : IProcessor  
    {
        private Logger _logger;
        private string _id;
        private string _name;
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private IConfigurator _configurator;
        private Dictionary<string, object> _config = new Dictionary<string, object>();
        private List<string> _mssgTypes = new List<string>();
        private List<string> _mssgTypesName = new List<string>();


        private static Mutex mut = new Mutex();
        private Dictionary<string, string> _dRecipient = new Dictionary<string, string>();

        private Dictionary<string,List<string>> _dClientsConnected = new Dictionary<string, List<string>>();


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
                    _logger = (Logger)logger.init("XymaAlarmCenterProcessor");
                else
                    _logger = (Logger)logger.init(_name);

                getConfig();
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
                string strmetadata = "{\"typeTask\":\"prcesorFtr\"}";

                dataOut.Add("data", strdata);
                dataOut.Add("metadata", strmetadata);

                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypesName.Count; indexTM++)
                    {
                        if (_mssgTypesName[indexTM] == "start")
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

                //Procesar los mensajes de las alertas FTR
                if (_dRecipient["process"].ToString() == recipientName)        
                {
                    _logger.Debug("Procesar los mensajes de las alertas FTR");
                    Dictionary<string, object> dStrData = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                    string area;
                    string entity;
                    string mssgOut;

                    
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

                    if (!meta.TryGetValue("areaId", out value))
                    {
                        _logger.Error("Falta el atributo 'areaId' en el metadato del mensaje para procesar");
                        return false;
                    }
                    area = value.ToString();
                    meta.Remove("areaId");

                    List<string> lSendClients = new List<string>();
                    Dictionary<string, object> dicMssg = new Dictionary<string, object>();
                    dicMssg.Add("data", dStrData);


                    mut.WaitOne();
                    if(_dClientsConnected.Count()>0)
                    {
                        if(GetClientsAutorized(area, ref lSendClients))
                        {
                            for (int index = 0; index < _listSerializer.Count; index++)
                            {
                                for (int indexTM = 0; indexTM < _mssgTypesName.Count; indexTM++)
                                {
                                    if (_mssgTypesName[indexTM] == "process")
                                    {
                                        for (int indexClient = 0; indexClient < lSendClients.Count(); indexClient++)
                                        {
                                            meta.Remove("publisherWebSocketServerCnxId");
                                            meta.Add("publisherWebSocketServerCnxId",lSendClients[indexClient]);
                                            dicMssg.Remove("metadata");
                                            dicMssg.Add("metadata", meta);
                                            _listSerializer[index].serialize(_mssgTypes[indexTM], dicMssg);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    mut.ReleaseMutex();

                    _logger.Trace("Fin");
                    return true;
                }
                
                //Limpiar el listado de conexiones, porque se reinicio el publicador websocket
                else if (_dRecipient["clearConnections"].ToString() == recipientName)
                {
                    _logger.Debug("Limpiar el listado de conexiones porque se reinicio el publicador websocket");
                    if (data["action"] == "resetConections")
                    {
                        mut.WaitOne();
                        foreach (List<string> valueZones in _dClientsConnected.Values)
                        {
                            valueZones.Clear();
                        }
                        _dClientsConnected.Clear();
                        mut.ReleaseMutex();
                    }
                    _logger.Debug("Limpiado todo el listado de clientes conectados y sus permisos");
                    return true;
                }

                //Remover una conexion espesifica del listado de clientes conectados
                else if (_dRecipient["removeConnection"].ToString() == recipientName)
                {
                    _logger.Debug("Remover una conexion espesifica del listado de clientes conectados");
                    if (data["action"].ToString() == "clientDisconected")
                    {
                        string client = data["client"].ToString();
                        if(_dClientsConnected.ContainsKey(client))
                        {
                            for (int i =0; i< _dClientsConnected[client].Count; i++)
                            {
                                _dClientsConnected[client].Clear();        //limpia las zonas de una entidad
                            }
                            _dClientsConnected.Remove(client); //Remueve el cliente con sus entidades asociadas

                            _logger.Debug($"Removida la conexion con cliente '{client}' y su listado de permisos asociados");
                            return true;
                        }
                        _logger.Debug($"No existe registro del cliente '{client}' al que remover");
                    }
                    return true;
                }
                
                //Agrega una conexion y sus permisos asociados al listado de clientes conectados
                else if (_dRecipient["addConnection"].ToString() == recipientName)
                {
                    _logger.Debug("Agrega una conexion y sus permisos asociados al listado de clientes conectados");
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

                    if (!meta.TryGetValue("publisherWebSocketServerCnxId", out value))
                    {
                        _logger.Error("Falta el atributo 'publisherWebSocketServerCnxId' en el dato del mensaje para procesar proveniente del recipiente asignado a 'addConnection'");
                        return false;
                    }
                    string strIdConection = value.ToString();
                    if (!data.TryGetValue("areasIds", out value))
                    {
                        _logger.Error("Falta el atributo 'areasIds' en el dato del mensaje para procesar proveniente del recipiente asignado a 'addConnection'");
                        return false;
                    }
                    List<string> lAreas = System.Text.Json.JsonSerializer.Deserialize<List<string>>(value.ToString());
                    

                    mut.WaitOne();
                    _dClientsConnected.Add(strIdConection, lAreas);
                    mut.ReleaseMutex();

                    return true;
                }
                
                else
                {
                    _logger.Error($"El nombre del recipiente '{recipientName}' no está definido en la configuracion");
                    return false;
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
                mut.WaitOne();
                getConfig();
                mut.ReleaseMutex();
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        
        private bool GetClientsAutorized(string area, ref List<string> lAutorized)
        {
            _logger.Trace("Inicio");
            lAutorized.Clear();
            if (this._dClientsConnected.Count() < 1)
            {
                return false;
            }
            else
            {
                for (int index = 0; index < this._dClientsConnected.Count(); index++)
                {
                    if(_dClientsConnected.ElementAt(index).Value.Contains(area))
                    {
                        lAutorized.Add(_dClientsConnected.ElementAt(index).Key);
                        _logger.Debug($"Id de conexion: '{_dClientsConnected.ElementAt(index).Key}' autorizado");
                    }
                    else
                    {
                        _logger.Debug($"Id de conexion: '{_dClientsConnected.ElementAt(index).Key}' no autorizado a recibir eventos desde el area '{area}'");
                    }
                }
            }
            _logger.Trace("Fin");
            if(lAutorized.Count < 1)
                return false;
            return true;
        }

        private void getConfig()
        {
            try
            {
                _logger.Trace("Inicio");
                Dictionary<string, object> config = _configurator.getMap("processors", _id);

                object value;
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
                else
                {
                    _dRecipient = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(value.ToString());

                    if (!_dRecipient.ContainsKey("process"))
                    {
                        throw new Exception("No se encontro el parametro 'process' en la configuracion 'recipientsNames' del procesador");
                    }
                    if (!_dRecipient.ContainsKey("clearConnections"))
                    {
                        throw new Exception("No se encontro el parametro 'clearConnections' en la configuracion 'recipientsNames' del procesador");
                    }
                    if (!_dRecipient.ContainsKey("removeConnection"))
                    {
                        throw new Exception("No se encontro el parametro 'removeConnection' en la configuracion 'recipientsNames' del procesador");
                    }
                    if (!_dRecipient.ContainsKey("addConnection"))
                    {
                        throw new Exception("No se encontro el parametro 'addConnection' en la configuracion 'recipientsNames' del procesador");
                    }
                }            
                _logger.Trace("Fin");
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
    }
}