using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using Definitions;
using NLog;
using System.Diagnostics;

namespace AdapterPlateSearch
{
    public struct Icon
    {
        public double[] scale;
        public string[] images;
        public double[] scaleByDistance;
        public string[] horizontalOrigin;
        public string[] verticalOrigin;
        public int[] intervalsOffSet;
    }

    public struct Camera
    {
        public string XSVServerId;
        public string ChannelId;
        public double Latitude;
        public double Longitude;
        public double altitude;
        public string name;
    }

    public class EventCompare : IComparer<Event>
    {
        public int Compare(Event? x, Event? y)
        {
            return x.detectedAt.CompareTo(y.detectedAt);
        }
    }
    public class Adapter : IProcessor
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

        private Icon medio;
        private Icon groupBox;

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
                    _logger = (Logger)logger.init("AdapterPlateSearch");
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

        public bool proccess(object payload, object metadataModule = null)
        {
            _logger.Trace("Inicio");
            Dictionary<string, object> dPayLoad = (Dictionary<string, object>)payload;

            object value;
            if (!dPayLoad.TryGetValue("metadata", out value))
            {
                _logger.Error("Falta el atributo 'metadata' en el mensaje para procesar");
                return false;
            }
            Dictionary<string, object> meta = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
            try
            {
                _logger.Trace("Inicio");

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

                if (!dPayLoad.TryGetValue("data", out value))
                {
                    _logger.Error("Falta el atributo 'data' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string, object> data = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                string strData = value.ToString();

                if (!dPayLoad.TryGetValue("metadata", out value))
                {
                    _logger.Error("Falta el atributo 'metadata' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string, object> dataOut = new Dictionary<string, object>();

                //toDo: logica de procesamiento
                if (!data.TryGetValue("entities", out value))
                {
                    _logger.Error("Falta el atributo 'entidades' en el mensaje para procesar");
                    return false;
                }
                List<object> lEntidades = System.Text.Json.JsonSerializer.Deserialize<List<object>>(value.ToString());
                if (!data.TryGetValue("interval", out value))
                {
                    _logger.Error("Falta el atributo 'interval' en el mensaje para procesar");
                    return false;
                }
                Dictionary<string,object> dIntervals = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!dIntervals.TryGetValue("begin", out value))
                {
                    _logger.Error("Falta el atributo 'begin' en el mensaje para procesar");
                    return false;
                }
                DateTime dtBegin = toDateTime(value.ToString()).ToUniversalTime();

                if (!dIntervals.TryGetValue("end", out value))
                {
                    _logger.Error("Falta el atributo 'end' en el mensaje para procesar");
                    return false;
                }
                DateTime dtEnd = toDateTime(value.ToString()).ToUniversalTime();

                List<Camera> Channels = new List<Camera>();
                List<Event> Events = new List<Event>();

                if (GetEventsAndChannels(lEntidades, Events, Channels)) //Si se detecto algun evento
                {
                    Events.Sort(new EventCompare());
                    int CountRequests = Events.Count + 1;
                    List<object> lEntities = new List<object>();

                    #region MensajeDeTodasLasCamaras
                    //toDo: construir le listado de entidades asociados a los canales de camaras para poner en 'data'

                    List<uint> OffSetsGroupBox = MakeIntervalsOffSet(groupBox, dtBegin, dtEnd);
                    foreach(Camera channel in Channels)
                    {
                        lEntities.Add((object)MakeChannelEntity(OffSetsGroupBox, channel, dtBegin));
                    }
                    data.Remove("entities");
                    data.Add("entities", lEntities);
                    data.Add("totalsRequest", CountRequests);
                    data.Add("numberRequest", 1);
                    data.Add("type", "channel");

                    meta.Add("errorCode", "200");
                    meta.Add("errorDescription", "Ok. Para responder a la solicitud se enviaran multiples respuestas asincronicamente");

                    dataOut.Add("metadata", meta);
                    dataOut.Add("data", data);
                    for (int index = 0; index < _listSerializer.Count; index++)
                    {
                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                        }
                    }
                    #endregion

                    #region MensajeAsociadoACadaEvento
                    List<uint> OffSetsMedios = MakeIntervalsOffSet(medio, dtBegin, dtEnd);

                    meta.Remove("errorDescription");
                    meta.Add("errorDescription", "Ok");
                    dataOut.Remove("data");
                    dataOut.Remove("metadata");

                    dataOut.Add("metadata", meta);
                    int countEvent = 2;
                    foreach (Event element in Events)
                    {
                        lEntities.Clear();
                        //toDo: construir le listado de la entidada asociada al evento para poner en 'data'
                        lEntities.Add((object) MakeEventEntity(OffSetsMedios,element));
                        data.Remove("entities");
                        data.Remove("numberRequest");
                        data.Remove("type");
                        data.Add("entities", lEntities);
                        data.Add("numberRequest", countEvent);
                        data.Add("type", "event");
                        
                        dataOut.Remove("data");
                        dataOut.Add("data", data);

                        countEvent++;

                        for (int index = 0; index < _listSerializer.Count; index++)
                        {
                            for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                            {
                                _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                            }
                        }
                    }
                    #endregion
                }
                else //Si no se detecto ningun evento 
                {
                    meta.Remove("errorCode");
                    meta.Remove("errorDescription");
                    meta.Add("errorCode", "204");
                    meta.Add("errorDescription", "No hay registros asociados a la solicitud");

                    dataOut.Remove("data");
                    dataOut.Remove("metadata");

                    dataOut.Add("data", "");
                    dataOut.Add("metadata", meta);

                    for (int index = 0; index < _listSerializer.Count; index++)
                    {
                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                        }
                    }
                }
                _logger.Trace("Fin");
                return true;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());

                Dictionary<string, object> dataOut = new Dictionary<string, object>();
                Dictionary<string, object> data = new Dictionary<string, object>();
                meta.Remove("errorCode");
                meta.Remove("errorDescription");

                meta.Add("errorCode", "500");
                meta.Add("errorDescription", "Error interno del servidor procesando la solicitud");

                dataOut.Add("data", data);
                dataOut.Add("metadata", meta);

                try
                {
                    for (int index = 0; index < _listSerializer.Count; index++)
                    {
                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                        }
                    }
                    _logger.Trace("Fin");
                    return true;
                }
                catch (Exception ex)
                {
                    _logger.Error(ex.ToString());
                    throw ex;
                }
                throw e;
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

                if (!config.TryGetValue("toBillboard", out value))
                {
                    throw new Exception("No se encontro el parametro 'toBillboard' en la configuracion del procesador");
                }
                Dictionary<string, object> toBillboard = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());
                if (!toBillboard.TryGetValue("medio", out value))
                {
                    throw new Exception("No se encontro el parametro 'medio' en la configuracion del procesador asiciada al parametro 'toBillboard'");
                }
                Dictionary<string, object> medio = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                this.medio = GetIconParameters(medio);

                if (!toBillboard.TryGetValue("groupBox", out value))
                {
                    throw new Exception("No se encontro el parametro 'groupBox' en la configuracion del procesador asiciada al parametro 'toBillboard'");
                }
                Dictionary<string, object> groupBox = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(value.ToString());

                this.groupBox = GetIconParameters(groupBox);

                _logger.Trace("Fin");
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        private Icon GetIconParameters(Dictionary<string,object> dIcon)
        {
            try
            {
                Icon icon;
                object value;
                if (!dIcon.TryGetValue("scale", out value))
                {
                    throw new Exception("No se encontro el parametro 'scale' en la configuracion del procesador");
                }
                double[] tempDouble = new double[1];
                tempDouble[0] = Convert.ToDouble(value.ToString());
                icon.scale = tempDouble;
                
                if (!dIcon.TryGetValue("image", out value))
                {
                    throw new Exception("No se encontro el parametro 'image' en la configuracion del procesador");
                }
                icon.images = System.Text.Json.JsonSerializer.Deserialize<List<string>>(value.ToString()).ToArray();
                
                if (!dIcon.TryGetValue("scaleByDistance", out value))
                {
                    throw new Exception("No se encontro el parametro 'scaleByDistance' en la configuracion del procesador");
                }
                icon.scaleByDistance = System.Text.Json.JsonSerializer.Deserialize<List<double>>(value.ToString()).ToArray();
               
                if (!dIcon.TryGetValue("horizontalOrigin", out value))
                {
                    throw new Exception("No se encontro el parametro 'horizontalOrigin' en la configuracion del procesador");
                }
                string[] tempstringH = new string[1];
                tempstringH[0] = value.ToString();
                icon.horizontalOrigin = tempstringH;

                if (!dIcon.TryGetValue("verticalOrigin", out value))
                {
                    throw new Exception("No se encontro el parametro 'verticalOrigin' en la configuracion del procesador");
                }
                string[] tempstringV = new string[1];
                tempstringV[0] = value.ToString();
                icon.verticalOrigin = tempstringV;

                if (!dIcon.TryGetValue("intervalsOffSet", out value))
                {
                    throw new Exception("No se encontro el parametro 'intervalsOffSet' en la configuracion del procesador");
                }
                icon.intervalsOffSet = System.Text.Json.JsonSerializer.Deserialize<List<int>>(value.ToString()).ToArray();
                if(icon.images.Length != icon.intervalsOffSet.Length )
                {
                    throw new Exception("La cantidad de 'intervalsOffSet' debe coincidir con la cantidad de 'image' definidas");
                }
                return icon;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        private DateTime toDateTime(string sDateTime)
        {
            _logger.Trace("Inicio");
            try
            {
                int year = Convert.ToInt32(sDateTime.Substring(0, 4));
                int month = Convert.ToInt32(sDateTime.Substring(5, 2));
                int day = Convert.ToInt32(sDateTime.Substring(8, 2));
                int hours = Convert.ToInt32(sDateTime.Substring(11, 2));
                int minutes = Convert.ToInt32(sDateTime.Substring(14, 2));
                int seconds = Convert.ToInt32(sDateTime.Substring(17, 2));
                int milis = Convert.ToInt32(sDateTime.Substring(20, 3));

                DateTime beginDt = new DateTime(year, month, day, hours, minutes, seconds, milis, DateTimeKind.Local);
                _logger.Trace("Fin");
                return beginDt;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                throw ex;
            }
        }
        private bool GetEventsAndChannels(List<object> entities, List<Event> Events, List<Camera> Channels)
        {
            _logger.Trace("Inicio");
            try
            {
                bool EventFined = false;
                if (entities.Count == 0)
                {
                    return false;
                }
                for (int iEntities = 0; iEntities < entities.Count; iEntities++)
                {
                    Dictionary<string, object> dEntity = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(entities[iEntities].ToString());
                    string sPlate = dEntity["plate"].ToString();
                    string sEntityId = dEntity["plate"].ToString();//Guid.NewGuid().ToString();
                    List<object> lXSVServers = System.Text.Json.JsonSerializer.Deserialize<List<object>>(dEntity["XSVServers"].ToString());
                    for (int iServers = 0; iServers < lXSVServers.Count; iServers++)
                    {
                        Dictionary<string, object> dServer = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(lXSVServers[iServers].ToString());
                        string sServerId = dServer["id"].ToString();
                        List<object> lChannels = System.Text.Json.JsonSerializer.Deserialize<List<object>>(dServer["channels"].ToString());
                        Camera camera;
                        for (int iChannel = 0; iChannel < lChannels.Count; iChannel++)
                        {
                            camera = new Camera();
                            Dictionary<string, object> dChannel = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(lChannels[iChannel].ToString());
                            camera.ChannelId = dChannel["id"].ToString();
                            camera.name = dChannel["name"].ToString();
                            camera.XSVServerId = sServerId;
                            Dictionary<string, object> dProperties = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(dChannel["properties"].ToString());
                            Dictionary<string, object> dPosition = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(dProperties["position"].ToString());
                            double number;
                            if(!double.TryParse(dPosition["latitude"].ToString(), out number))
                            {
                                _logger.Error("Error en el atributo 'latitude' en el mensaje para procesar");
                                throw new Exception("Error en el atributo 'latitude' en el mensaje para procesar");
                            }
                            camera.Latitude = number;
                            if (!double.TryParse(dPosition["longitude"].ToString(), out number))
                            {
                                _logger.Error("Error en el atributo 'longitude' en el mensaje para procesar");
                                throw new Exception("Error en el atributo 'longitude' en el mensaje para procesar");
                            }
                            camera.Longitude = number;
                            if (!double.TryParse(dPosition["altitude"].ToString(), out number))
                            {
                                _logger.Error("Error en el atributo 'altitude' en el mensaje para procesar");
                                throw new Exception("Error en el atributo 'altitude' en el mensaje para procesar");
                            }
                            camera.altitude = number;


                            List<object> lEvents = System.Text.Json.JsonSerializer.Deserialize<List<object>>(dChannel["events"].ToString());
                            if (lEvents.Count > 0)
                            {
                                Channels.Add(camera);
                                EventFined = true;
                                Event evento;
                                for (int iEvent = 0; iEvent < lEvents.Count; iEvent++)
                                {
                                    Dictionary<string, object> dEvento = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(lEvents[iEvent].ToString());
                                    evento = new Event();

                                    if (!double.TryParse(dEvento["azimut"].ToString(), out number))
                                    {
                                        evento.azimut = 0;
                                        //_logger.Error("Error en el atributo 'azimut' en el mensaje para procesar");
                                        //throw new Exception("Error en el atributo 'azimut' en el mensaje para procesar");
                                    }
                                    else
                                    {
                                        evento.azimut = number;
                                    }
                                    if (!double.TryParse(dEvento["speed"].ToString(), out number))
                                    {
                                        _logger.Error("Error en el atributo 'speed' en el mensaje para procesar");
                                        throw new Exception("Error en el atributo 'speed' en el mensaje para procesar");
                                    }
                                    evento.speed = number;
                                    evento.IdLog = dEvento["idLog"].ToString();
                                    evento.detectedAt = toDateTime(dEvento["detected_at"].ToString()).ToUniversalTime();
                                    evento.SetCourse(dProperties["otrosDatos"].ToString());
                                    evento.plate = sPlate;
                                    evento.entityId = sPlate;
                                    evento.ChannelId = camera.ChannelId;
                                    evento.XSVServerId = sServerId;
                                    evento.latitude = camera.Latitude;
                                    evento.longitude = camera.Longitude;
                                    evento.altitude = camera.altitude;

                                   Events.Add(evento);
                                }
                            }
                        }
                    }
                }
                _logger.Trace("Fin");
                return EventFined;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        private List<uint> MakeIntervalsOffSet(Icon sEntityIcons, DateTime dtBegin, DateTime dtEnd)
        {
            List<uint> OffSets = new List<uint>();

            if (sEntityIcons.intervalsOffSet[0] != -1)
            {
                for (int index = 0; index < sEntityIcons.intervalsOffSet.Length; index++)
                {
                    OffSets.Add((uint)sEntityIcons.intervalsOffSet[index]);
                }
            }
            else
            {
                var result = dtEnd - dtBegin;
                uint allIntervalsMin = Convert.ToUInt32(result.TotalMinutes);
                OffSets.Add(allIntervalsMin);
            }
            return OffSets;
        }
        private Dictionary<string, object> MakeChannelEntity(List<uint> intervalsOffSet, Camera Channel, DateTime dtBegin)
        {
            try
            {
                Dictionary<string, object> dEntity = new Dictionary<string, object>();
                dEntity.Add("id", Channel.XSVServerId + Channel.ChannelId);
                dEntity.Add("name", Channel.name);
                dEntity.Add("intervalsOffsets", intervalsOffSet);
                Dictionary<string, object> dInstance = MakeChannelInstance(Channel, dtBegin);
                List<object> lInstances = new List<object>();
                lInstances.Add(dInstance);
                dEntity.Add("instances", lInstances);
                Dictionary<string, object> dcommonGeometry = new Dictionary<string, object>();
                Dictionary<string, object> dcommonBillboard = new Dictionary<string, object>();
                dcommonBillboard.Add("image", groupBox.images);
                dcommonBillboard.Add("scale", groupBox.scale);
                dcommonBillboard.Add("scaleByDistance", groupBox.scaleByDistance);
                dcommonBillboard.Add("horizontalOrigin", groupBox.horizontalOrigin);
                dcommonBillboard.Add("verticalOrigin", groupBox.verticalOrigin);
                dcommonGeometry.Add("billboard", dcommonBillboard);
                dEntity.Add("commonGeometryProperties", dcommonGeometry);
                return dEntity;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        private Dictionary<string, object> MakeChannelInstance(Camera camara, DateTime dtBegin)
        {
            try
            {

                Dictionary<string, object> dInstance = new Dictionary<string, object>();

                #region Properties
                Dictionary<string, object> dProperties = new Dictionary<string, object>
                {
                    { "instanceId", camara.XSVServerId + camara.ChannelId},
                    { "adquireAt", dtBegin.ToString("yyyy-MM-ddTHH:mm:ss.fffZ")}
                };
                Dictionary<string, object> dOtherProperties = new Dictionary<string, object>
                {
                    { "XSVServerId", camara.XSVServerId }
                };
                dProperties.Add("otherProperties", dOtherProperties);
                #endregion

                #region Position
                Dictionary<string, object> dPosition = new Dictionary<string, object>();
                dPosition.Add("latitude", camara.Latitude);
                dPosition.Add("longitude", camara.Longitude);
                dPosition.Add("altitude", camara.altitude);
                #endregion

                #region CurrentGeometry
                Dictionary<string, object> dCurrentGeometry = new Dictionary<string, object>();
                Dictionary<string, object> dCurrentBillboardGeometry = new Dictionary<string, object>();
                dCurrentBillboardGeometry.Clear();
                dCurrentGeometry.Add("billboard", dCurrentBillboardGeometry);
                #endregion

                dInstance.Add("properties", dProperties);
                dInstance.Add("position", dPosition);
                dInstance.Add("currentGeometryProperties", dCurrentGeometry);

                return dInstance;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        private Dictionary<string, object> MakeEventEntity(List<uint> intervalsOffSet, Event evento)
        {
            try
            {
                Dictionary<string, object> dEntity = new Dictionary<string, object>();
                dEntity.Add("id", evento.entityId);
                dEntity.Add("name", evento.plate);
                dEntity.Add("intervalsOffsets", intervalsOffSet);
                Dictionary<string, object> dInstance = MakeEventInstance(evento);
                List<object> lInstances = new List<object>();
                lInstances.Add(dInstance);
                dEntity.Add("instances", lInstances);
                Dictionary<string, object> dcommonGeometry = new Dictionary<string, object>();
                Dictionary<string, object> dcommonBillboard = new Dictionary<string, object>();
                List<object> lRotations = new List<object>();
                lRotations.Add(evento.course);
                dcommonBillboard.Add("image", medio.images);
                dcommonBillboard.Add("scale", medio.scale);
                dcommonBillboard.Add("scaleByDistance", medio.scaleByDistance);
                dcommonBillboard.Add("horizontalOrigin", medio.horizontalOrigin);
                dcommonBillboard.Add("verticalOrigin", medio.verticalOrigin);
                dcommonBillboard.Add("rotation", lRotations);
                dcommonGeometry.Add("billboard", dcommonBillboard);
                dEntity.Add("commonGeometryProperties", dcommonGeometry);
                return dEntity;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        private Dictionary<string, object> MakeEventInstance(Event evento)
        {
            try
            {
                Dictionary<string, object> dInstance = new Dictionary<string, object>();

                #region Properties
                Dictionary<string, object> dProperties = new Dictionary<string, object>();
                dProperties.Add("instanceId", evento.plate);
                dProperties.Add("adquireAt", evento.detectedAt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ"));
                Dictionary<string, object> dotherProperties = new Dictionary<string, object>
                {
                    { "speed", evento.speed },
                    { "idLog", evento.IdLog },
                    { "ChannelId", evento.ChannelId },
                    { "XSVServerId", evento.XSVServerId }
                };
                dProperties.Add("otherProperties", dotherProperties);

                #endregion

                #region Position
                Dictionary<string, object> dPosition = new Dictionary<string, object>();
                dPosition.Add("latitude", evento.latitude);
                dPosition.Add("longitude", evento.longitude);
                dPosition.Add("altitude", evento.altitude);
                #endregion

                #region CurrentGeometry
                Dictionary<string, object> dCurrentGeometry = new Dictionary<string, object>();
                Dictionary<string, object> dCurrentBillboardGeometry = new Dictionary<string, object>();
                dCurrentBillboardGeometry.Clear();
                dCurrentGeometry.Add("billboard", dCurrentBillboardGeometry);
                #endregion

                dInstance.Add("properties", dProperties);
                dInstance.Add("position", dPosition);
                dInstance.Add("currentGeometryProperties", dCurrentGeometry);

                return dInstance;
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }


    }
}