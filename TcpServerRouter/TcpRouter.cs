using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using NLog;

namespace TcpServerRouter
{
    public class TcpRouter : IProcessor
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
        private string _ip;
        private string _port;
        private string _preRecipent;

        private Dictionary<string, string> dRouting = new Dictionary<string, string>();

        private Server serverTcp;

        public void addSerializer(ISerializer serializer)
        {
            try
            {
                _logger.Trace("Inicio");
                _listSerializer.Add(serializer);
                serverTcp.AddSerializer(serializer);
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
                    _logger = (Logger)logger.init("TcpServerRouter");
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

        private bool reConfig()
        {
            try
            {
                mut.WaitOne();
                serverTcp.StopServer();
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

                //routingToDevice
                if (config.TryGetValue("routingToDevice", out value))
                {
                    List<object> tempList = System.Text.Json.JsonSerializer.Deserialize<List<object>>(value.ToString());
                    if (tempList.Count < 1)
                    {
                        throw new Exception($"No se configuro ningun 'routingToDevice' para usar como parametro en la salida del modulo");
                    }
                    for (int index = 0; index < tempList.Count; index++)
                    {
                        Dictionary<string, object> currentPub = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(tempList[index].ToString());

                        string deviceID = currentPub.First().Key;
                        object ip = currentPub.First().Value;

                        if (!dRouting.TryAdd(deviceID,ip.ToString()))
                        {
                            throw new Exception($"Error parametrizando la combinación -> "+deviceID+": "+ip.ToString());
                        }
                    }
                    tempList.Clear();
                }

                if (!config.TryGetValue("serverIp", out value))
                {
                    throw new Exception("No se encontro el parametro 'serverIp' en la configuracion del procesador");
                }
                _ip = value.ToString();

                if (!config.TryGetValue("serverPort", out value))
                {
                    throw new Exception("No se encontro el parametro 'serverPort' en la configuracion del procesador");
                }
                _port = value.ToString();

                if (!config.TryGetValue("prefixRecipent", out value))
                {
                    throw new Exception("No se encontro el parametro 'prefixRecipent' en la configuracion del procesador");
                }
                _preRecipent = value.ToString();

                serverTcp = new(_ip, _port, _preRecipent, _logger, mut);

                _logger.Trace("Fin");
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }

        public bool proccess(object payload, object metadataModule = null)
        {
            try
            {
                _logger.Trace("Inicio");

                if (_configurator.hasNewConfig(_id))
                {
                    reConfig();
                    _logger.Debug("Reconfiguracion exitosa");
                }

                //toDo: Logica del procesador
                Dictionary<string, object> dPayLoad = (Dictionary<string, object>)payload;

                object value;

                if (!dPayLoad.TryGetValue("device_id", out value))
                {
                    _logger.Error("Falta el atributo 'device_id' en el mensaje para procesar");
                    return false;
                }
                string deviceId = value.ToString();

                if (!string.IsNullOrEmpty(deviceId))
                {
                    if (!dRouting.ContainsKey(deviceId))
                    {
                        _logger.Error("No se encontro ningun dispositivo a rutear con el id: '" + deviceId + "' en la configuracion");
                        return false;
                    }
                    string dataOut = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dPayLoad);
                    if (!serverTcp.SendToTcpCliente(dRouting[deviceId], dataOut))
                    {
                        _logger.Error("No se pudo enviar al dispositivo '" + deviceId + "' con ip '" + dRouting[deviceId] + "' el dato: " + dataOut);
                        return false;
                    }     
                }
                else
                {
                    _logger.Error("Falta el valor para el atributo 'device_id' en el mensaje para procesar");
                    return false;
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
    }
}
