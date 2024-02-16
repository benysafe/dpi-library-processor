using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using Definitions;
using NLog;
using System.Diagnostics;

namespace CzmlBuilder
{
    
    public class CzmlBuilder : IProcessor
    {
        private Logger _logger;
        private string _id;
        private string _name;
        IConfigurator _configurator;
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private Dictionary<string, object> _config = new Dictionary<string, object>();
        private bool ModuleNoOut = true;
        private List<string> _mssgTypes = new List<string>();
        private static Mutex mut = new Mutex();


        private void getConfig()
        {
            try
            {
                _logger.Trace("Inicio");
                Dictionary<string, object> config = _configurator.getMap("processors", _id);

                object value;
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
        public void addSerializer(ISerializer serializer)
        {
            try
            {
                _logger.Trace("Inicio");
                _listSerializer.Add(serializer);
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
                    _logger = (Logger)logger.init("CzmlBuilder");
                else
                    _logger = (Logger)logger.init(_name);

                getConfig();
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }

        public bool proccess(object payload, object metadata = null)
        {
            try
            {
                _logger.Trace("Inicio");

                Dictionary<string, object> dPayLoad =(Dictionary<string, object>)payload;
                object value;

                if(dPayLoad.TryGetValue("adminCommand", out value)) //comandos de administracion, 'reconfig', 'kill' 
                {
                    string adminCommand = value.ToString();
                    if(adminCommand == "reconfig")
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
                
                Dictionary<string, object> dataOut = new Dictionary<string, object>();

                if ( data.ContainsKey("entities"))
                {
                    Dictionary<string, object> dirHead = new Dictionary<string, object>();
                    dirHead.Add("id", "document");
                    dirHead.Add("name", "");
                    dirHead.Add("version", "1.0");

                    Czml czml = new Czml();
                    czml.TrySetHeader(dirHead);

                    if (!data.TryGetValue("entities", out value))
                    {
                        _logger.Error("Falta el atributo 'entities' en el mensaje para procesar");
                        return false;
                    }
                    List<Dictionary<string, object>> entities = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(value.ToString());

                    /*
                     * if(!data.TryGetValue("type", out value))
                    {
                        _logger.Error("Falta el atributo 'type' en el mensaje para procesar");
                        return false;
                    }
                    string sType = value.ToString();
                    */

                    int indexBody = 0;
                    foreach (Dictionary<string, object> entity in entities)
                    {
                        string id = entity["id"].ToString();
                        string name = entity["name"].ToString();
                        string description = null;
                        if (entity.ContainsKey("description"))
                        {
                            description = entity["description"].ToString();
                        }
                        List<int> intervalsOffSets = System.Text.Json.JsonSerializer.Deserialize<List<int>>(entity["intervalsOffsets"].ToString());

                        Dictionary<string, object> commonGeometryProperties = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(entity["commonGeometryProperties"].ToString());
                        Dictionary<string, object> defaultBillboard = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(commonGeometryProperties["billboard"].ToString());
                        List<Dictionary<string, object>> instances = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(entity["instances"].ToString());

                        foreach (Dictionary<string, object> instance in instances)
                        {
                            Dictionary<string, object> properties = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(instance["properties"].ToString());
                            Dictionary<string, object> currentGeometryPropertiesInstance = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(instance["currentGeometryProperties"].ToString());
                            Dictionary<string, object> billboardInstance = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(currentGeometryPropertiesInstance["billboard"].ToString());
                            string adquireAt = properties["adquireAt"].ToString();
                            if (czml.TrySetBodyElement(indexBody, instance, defaultBillboard, id, name, description, intervalsOffSets))
                            {
                                //Dictionary<string, object> subBody = new Dictionary<string, object>();
                                //czml.TryGetBodyElement(indexBody, out subBody);
                                //_logger.Debug($"BodyElement[{indexBody}] generado:  {System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(subBody)}");
                                indexBody++;
                            }
                            else
                            {
                                _logger.Error($"Error intentando generar el 'BodyElement' para la instancia {indexBody} dedo el mensaje: '{System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dPayLoad)}'");
                            }
                        }
                    }
                    data.Add("czmls", czml.GetCzml());
                    //data.Add("type", sType);
                    data.Remove("entities");
                }
                dataOut.Add("data", data);
                dataOut.Add("metadata",meta);

                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0;indexTM < _mssgTypes.Count; indexTM++)
                    {
                        _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                    }
                }

                _logger.Trace("Fin");
                return true;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                return false;
                throw e;
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
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
    }
}