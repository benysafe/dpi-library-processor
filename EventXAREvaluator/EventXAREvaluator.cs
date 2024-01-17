using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using Definitions;
using NLog;
using System.Diagnostics;
using System.Threading;
using System.Net;
using System.Text;
using NLog.Config;
using System.Threading.Channels;
using System.Data;
using Microsoft.VisualBasic;

namespace EventXAREvaluator
{
    public struct WebService
    {
        public string name;
        public string uri;
        public Dictionary<string, object> dParameters; 
    };

    public class EventXAREvaluator : IProcessor
    {
        const uint timeOutConexion = 15;  // 15 tips del times = 150 seg de timeOut del keepAlive desde la aplicacion Operador
        const string dateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";
        private Logger _logger;
        private string _id;
        private string _name;
        private uint _minuteTimerOffSet;
        private uint _countGlobalTimer;
        IConfigurator _configurator;
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private Dictionary<string, object> _config = new Dictionary<string, object>();
        private bool ModuleNoOut = true;
        private List<string> _mssgTypes = new List<string>();
        private static Mutex mut = new Mutex();
        private Dictionary<string, WebService> _dWebServices = new Dictionary<string, WebService>();
        Dictionary<string, string> dLoginDataForXVision = new Dictionary<string, string>();


        private string selfSessionId;
        private System.Timers.Timer aTimer = new System.Timers.Timer(10000);

        private Dictionary<string,Operator> dOperators = new Dictionary<string, Operator>();

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
                    _logger = (Logger)logger.init("EvaluetorAction");
                else
                    _logger = (Logger)logger.init(_name);

                // Hook up the Elapsed event for the timer. 
                aTimer.Elapsed += OnTimedEvent;

                // Have the timer fire repeated events (true is the default)
                aTimer.AutoReset = true;
                
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
            try
            {
                _logger.Trace("Inicio");

                if (_configurator.hasNewConfig(_id))
                {
                    reConfig();
                    _logger.Debug("Reconfiguracion exitosa");
                }

                Dictionary<string, object> dPayLoad = (Dictionary<string, object>)payload;
                object value;

                if (!dPayLoad.TryGetValue("sessionId", out value))
                {
                    _logger.Error("Falta el atributo 'sessionId' en el mensaje para procesar");
                    return false;
                }
                string sSessionId = value.ToString();

                if (!dPayLoad.TryGetValue("actionId", out value))
                {
                    _logger.Error("Falta el atributo 'actionId' en el mensaje para procesar");
                    return false;
                }
                string sActionId = value.ToString();

                if (!dPayLoad.TryGetValue("timestamp", out value))
                {
                    _logger.Error("Falta el atributo 'timestamp' en el mensaje para procesar");
                    return false;
                }
                string sTimeStamp = value.ToString();

                if (!dPayLoad.TryGetValue("userName", out value))
                {
                    _logger.Error("Falta el atributo 'userName' en el mensaje para procesar");
                    return false;
                }
                string sUserName = value.ToString();

                if (!dPayLoad.TryGetValue("ip", out value))
                {
                    _logger.Error("Falta el atributo 'ip' en el mensaje para procesar");
                    return false;
                }
                string ip = value.ToString();

                if (!dPayLoad.TryGetValue("mac", out value))
                {
                    _logger.Error("Falta el atributo 'mac' en el mensaje para procesar");
                    return false;
                }
                string mac = value.ToString();

                if (!dPayLoad.TryGetValue("pcName", out value))
                {
                    _logger.Error("Falta el atributo 'pcName' en el mensaje para procesar");
                    return false;
                }
                string pcname = value.ToString();


                if (sActionId == "UserLogin")        //UserLogin
                {
                    if (!dPayLoad.TryGetValue("param1", out value))
                    {
                        _logger.Error("Falta el atributo 'param1' en el mensaje para procesar");
                        return false;
                    }
                    string sParam1 = value.ToString();

                    Dictionary<string, object> dataOut = new Dictionary<string, object>();
                    switch (sParam1)
                    {
                        case "1":   //logIn
                            Operator operador = new Operator(sUserName, sSessionId);
                            if (!dOperators.ContainsKey(sSessionId))
                            {
                                mut.WaitOne();
                                dOperators.Add(sSessionId, operador);
                                mut.ReleaseMutex();

                                //Send UserLogin
                                dataOut = GenerateUserLogin(sUserName, sSessionId, "1" ,ip , mac, pcname, sTimeStamp);
                                for (int index = 0; index < _listSerializer.Count; index++)
                                {
                                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                    {
                                        _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                    }
                                }
                            }
                            else 
                            {
                                _logger.Debug($"Para el operador {sUserName} ya tiene la sesion '{sSessionId}' registrado");
                                return false;
                            }

                            break;
                        case "0":   //LogOut
                            if (dOperators.ContainsKey(sSessionId))
                            {
                                mut.WaitOne();
                                dOperators[sSessionId].channels.Clear();
                                dOperators.Remove(sSessionId);
                                mut.ReleaseMutex();

                                //Send Auto UserLogin
                                dataOut = GenerateUserLogin(sUserName, sSessionId, "0", null, null, null, sTimeStamp);
                                for (int index = 0; index < _listSerializer.Count; index++)
                                {
                                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                    {
                                        _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                    }
                                }
                            }
                            else
                            {
                                _logger.Debug($"Para el operador {sUserName} la sesion '{sSessionId}' no esta registrada al momento de solicitar un LogOut");
                                return false;
                            }
  
                            break;
                    }
                }

                else if (sActionId == "Heartbeat")       //KeepAlive
                {
                    Dictionary<string,object> dataOut = new Dictionary<string,object>();
                    if (!dOperators.ContainsKey(sSessionId))
                    {
                        Operator operador = new Operator(sUserName, sSessionId);
                        mut.WaitOne();
                        dOperators.Add(sSessionId, operador);
                        mut.ReleaseMutex();

                        //Send UserLogin
                        dataOut = GenerateUserLogin(sUserName, sSessionId, "1", ip, mac, pcname, sTimeStamp);
                        for (int index = 0; index < _listSerializer.Count; index++)
                        {
                            for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                            {
                                _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                            }
                        }
                    }
                    else
                    {
                        if (dPayLoad.ContainsKey("channelId") && dPayLoad.ContainsKey("channelDescription"))
                        {
                            if (!dPayLoad.TryGetValue("channelId", out value))
                            {
                                _logger.Error("Falta el atributo 'channelId' en el mensaje para procesar");
                                return false;
                            }
                            string sChannelId = value.ToString();

                            if (!dPayLoad.TryGetValue("channelDescription", out value))
                            {
                                _logger.Error("Falta el atributo 'channelDescription' en el mensaje para procesar");
                                return false;
                            }
                            string sChannelDescription = value.ToString();
                            mut.WaitOne();
                            dOperators[sSessionId].CounterReset();
                            for (int i = 0; i < dOperators[sSessionId].channels.Count; i++)
                            {
                                if (dOperators[sSessionId].channels[i].id == sChannelId)
                                {
                                    dOperators[sSessionId].channels[i].CounterKeepAliveReset();
                                    break;
                                }
                            }
                            mut.ReleaseMutex();
                        }
                        else
                        {
                            mut.WaitOne();
                            dOperators[sSessionId].CounterReset();
                            mut.ReleaseMutex();
                        }
                    }
                }

                else
                {
                    if (!dPayLoad.TryGetValue("channelId", out value))
                    {
                        _logger.Error("Falta el atributo 'channelId' en el mensaje para procesar");
                        return false;
                    }
                    string sChannelId = value.ToString();

                    if (!dPayLoad.TryGetValue("channelDescription", out value))
                    {
                        _logger.Error("Falta el atributo 'channelDescription' en el mensaje para procesar");
                        return false;
                    }
                    string sChannelDescription = value.ToString();

                    if (sActionId == "RemoteConnection")        //RemoteConnection
                    {
                        if (!dPayLoad.TryGetValue("param1", out value))
                        {
                            _logger.Error("Falta el atributo 'param1' en el mensaje para procesar");
                            return false;
                        }
                        string sParam1 = value.ToString();
                        Dictionary<string, object> dData = new Dictionary<string, object>();
                        switch (sParam1)
                        {
                            case "1":   //connect
                                Channel channel = new Channel(sChannelId, sChannelDescription);
                                if (dOperators.ContainsKey(sSessionId))
                                {                                  
                                    mut.WaitOne();
                                    dOperators[sSessionId].channels.Add(channel);
                                    mut.ReleaseMutex();

                                    _logger.Debug($"Agregado el canal '{sChannelId}' para el operador '{sSessionId}'");
                                }
                                else
                                {
                                    _logger.Debug($"El operador '{sSessionId}' no estaba registrado y se registro ahora");
                                    Operator operador = new Operator(sUserName, sSessionId);
                                    mut.WaitOne(); 
                                    dOperators.Add(sSessionId, operador);
                                    dOperators[sSessionId].channels.Add(channel);
                                    mut.ReleaseMutex();

                                    //Send Auto UserLogin
                                    Dictionary<string, object> dataOut = GenerateUserLogin(sUserName, sSessionId,"1", ip, mac, pcname,sTimeStamp);
                                    for (int index = 0; index < _listSerializer.Count; index++)
                                    {
                                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                        {
                                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                        }
                                    }

                                    _logger.Debug($"Agregado el canal '{sChannelId}' para el operador '{sSessionId}'");
                                }

                                dData = GenerateRemoteConnection(sUserName, sSessionId, sChannelId, sChannelDescription, channel.conectionId, "1", sTimeStamp);
                                for (int index = 0; index < _listSerializer.Count; index++)
                                {
                                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                    {
                                        _listSerializer[index].serialize(_mssgTypes[indexTM], dData);
                                    }
                                }

                                break;
                            case "0":   //disconnect
                                if (dOperators.ContainsKey(sSessionId))
                                {
                                    Channel tempChannel = new Channel(sChannelId, sChannelDescription);
                                    mut.WaitOne();
                                    if (dOperators[sSessionId].RemoveChannel(sChannelId,out tempChannel))
                                    {
                                        string currentInactId;
                                        string currentConnectionId;
                                        if(tempChannel.StopInactivity(out currentInactId, out currentConnectionId)) //Finaliza la inactividad en caso de exixtir
                                        {
                                            Dictionary<string, object> data = GenerateInactivity(sUserName, sSessionId, sChannelId, sChannelDescription, currentConnectionId, currentInactId, "0");
                                            for (int index = 0; index < _listSerializer.Count; index++)
                                            {
                                                for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                                {
                                                    _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                                }
                                            }
                                        }
                                        
                                        _logger.Debug($"Removido el canal '{sChannelId}' para el operador '{sSessionId}'");

                                        //Send RemoteConnection
                                        Dictionary<string, object> dataOut = GenerateRemoteConnection( sUserName, sSessionId, sChannelId, sChannelDescription, currentConnectionId, "0", sTimeStamp);
                                        for (int index = 0; index < _listSerializer.Count; index++)
                                        {
                                            for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                            {
                                                _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                            }
                                        }
                                    }
                                    else
                                    {
                                        _logger.Debug($"No se encontro el canal '{sChannelId}' para el operador '{sSessionId}'");
                                    }
                                    mut.ReleaseMutex();
                                }
                                else
                                {
                                    _logger.Debug($"El operador '{sSessionId}' no estaba registrado y se registro ahora");
                                    Operator operador = new Operator(sUserName, sSessionId);
                                    mut.WaitOne(); 
                                    dOperators.Add(sSessionId, operador); 
                                    mut.ReleaseMutex();
                                    //Send Auto UserLogin
                                    Dictionary<string, object> dataOut = GenerateUserLogin(sUserName, sSessionId, "1", ip, mac, pcname,sTimeStamp);
                                    for (int index = 0; index < _listSerializer.Count; index++)
                                    {
                                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                        {
                                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                        }
                                    }
                                }
                                break;
                        }
                    }                  
                    else if(sActionId == "RemotePower")     //RemotePower
                    {
                        //No hacer nada, no se concidera acción, reservado para uso futuro
                    }                    
                    else if (sActionId == "AppClosed")     //AppClosed
                    {
                        _logger.Debug($"Aplicacion de operatoria cerrada por el usuario {sUserName}");
                        List<string> lOperatorToRemove = new List<string>();
                        Dictionary<string, object> data =new Dictionary<string, object>();
                        mut.WaitOne();
                        if (dOperators.Count != 0)
                        {
                            foreach (KeyValuePair<string, Operator> entry in dOperators)
                            {
                                if (entry.Value.channels.Count != 0)
                                {
                                    List<Channel> lChannelToRemove = new List<Channel>();

                                    foreach (Channel channel in entry.Value.channels)
                                    {
                                        string currentInactId;
                                        string currentConnectioId;
                                        data = new Dictionary<string, object>();
                                        if (channel.StopInactivity(out currentInactId, out currentConnectioId))
                                        {
                                            //Send AutoInactivity
                                            data = GenerateInactivity(entry.Value.userName, entry.Value.sessionId, channel.id, channel.description, currentConnectioId, currentInactId, "0");
                                            for (int index = 0; index < _listSerializer.Count; index++)
                                            {
                                                for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                                {
                                                    _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                                }
                                            }
                                        }
                                        lChannelToRemove.Add(channel);
                                    }
                                    //remueve el canal
                                    for (int i = 0; i < lChannelToRemove.Count; i++)
                                    {
                                        if (entry.Value.channels.Remove(lChannelToRemove[i]))
                                        {
                                            //Send AutoRemoteConnection
                                            data = GenerateRemoteConnection(entry.Value.userName, entry.Value.sessionId, lChannelToRemove[i].id, lChannelToRemove[i].description, lChannelToRemove[i].conectionId, "0");
                                            for (int index = 0; index < _listSerializer.Count; index++)
                                            {
                                                for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                                {
                                                    _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                                }
                                            }
                                        }
                                    }
                                }
                                lOperatorToRemove.Add(sSessionId);
                            }
                        }
                        mut.ReleaseMutex();
                        if (lOperatorToRemove.Any())
                        {
                            foreach (var element in lOperatorToRemove)
                            {
                                dOperators.Remove(element);
                                //Send AutoUserLogin
                                data = GenerateUserLogin(sUserName, sSessionId, "0",null, null, null, sTimeStamp);
                                for (int index = 0; index < _listSerializer.Count; index++)
                                {
                                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                    {
                                        _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                    }
                                }
                            }
                        }
                    }
                    else if (sActionId == "RemoteKeyboard")     //RemoteKeyboard
                    {
                        //No hacer nada, no se concidera acción, reservado para uso futuro
                    }
                    else if (sActionId == "RemoteTrackball")     //RemoteTrackball
                    {
                        //No hacer nada, no se concidera acción, reservado para uso futuro
                    }
                    else 
                    {
                        mut.WaitOne();
                        if (dOperators.ContainsKey(sSessionId))
                        {
                            if (dOperators[sSessionId].GetChannel(sChannelId) != null)
                            {
                                string currentInactId;
                                string currentConnectioId;

                                if (dOperators[sSessionId].GetChannel(sChannelId).StopInactivity(out currentInactId, out currentConnectioId))
                                {
                                    //Send AutoRemoteConnection
                                    Dictionary<string, object> data = GenerateInactivity(sUserName, sSessionId, sChannelId, sChannelDescription, currentConnectioId, currentInactId, "0");
                                    for (int index = 0; index < _listSerializer.Count; index++)
                                    {
                                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                        {
                                            _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                        }
                                    }
                                }
                            }
                            else
                            {
                                _logger.Debug($"Canal no estaba registrado para el operador '{sSessionId}' y se registro ahora");
                                Channel channel = new Channel(sChannelId, sChannelDescription);
                                dOperators[sSessionId].channels.Add(channel);
                                
                                //Send AutoRemoteConnection
                                Dictionary<string,object> dataOut = GenerateRemoteConnection(sUserName, sSessionId, sChannelId, sChannelDescription, channel.conectionId, "1", sTimeStamp);
                                for (int index = 0; index < _listSerializer.Count; index++)
                                {
                                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                    {
                                        _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                    }
                                }
                            }
                        }
                        else
                        {
                            _logger.Debug($"El operador '{sSessionId}' no estaba registrado y se registro ahora");
                            Operator operador = new Operator(sUserName, sSessionId);
                            dOperators.Add(sSessionId, operador);
                            Channel channel = new Channel(sChannelId, sChannelDescription);
                            dOperators[sSessionId].channels.Add(channel);

                            //Send Auto UserLogin
                            Dictionary<string, object> dataOut = GenerateUserLogin(sUserName, sSessionId,"1", ip, mac, pcname, sTimeStamp);
                            for (int index = 0; index < _listSerializer.Count; index++)
                            {
                                for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                {
                                    _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                }
                            }

                            //Send AutoRemoteConnection
                            dataOut.Clear();
                            dataOut = GenerateRemoteConnection(sUserName, sSessionId, sChannelId, sChannelDescription, channel.conectionId, "1", sTimeStamp);
                            for (int index = 0; index < _listSerializer.Count; index++)
                            {
                                for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                {
                                    _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                }
                            }
                        }
                        mut.ReleaseMutex();
                        //Send AnyOther Action
                        Channel tempChannel = dOperators[sSessionId].GetChannel(sChannelId);
                        if (tempChannel != null)
                        {
                            Dictionary<string, object> dPayLoadOut = UpdateTimeStamp(dPayLoad,tempChannel.conectionId);
                            for (int index = 0; index < _listSerializer.Count; index++)
                            {
                                for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                {
                                    _listSerializer[index].serialize(_mssgTypes[indexTM], dPayLoadOut);
                                }
                            }
                        }
                    } 
                }
                
                _logger.Trace("Fin");
                return true;
            }
            catch (Exception ex)
            {
                _logger.Error(ex.ToString());
                return false;
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
                this._mssgTypes.Clear();
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
                if (!config.TryGetValue("webService", out value))
                {
                    throw new Exception("No se encontro el parametro 'webService' en la configuracion del procesador");
                }
                List<Dictionary<string, object>> lWebServices = System.Text.Json.JsonSerializer.Deserialize<List<Dictionary<string, object>>>(value.ToString());
                index = 0;
                this._dWebServices.Clear();
                
                foreach (Dictionary<string, object> keyValuePairs in lWebServices)
                {
                    object valueService;
                    WebService service = new WebService();
                    if (!keyValuePairs.TryGetValue("name", out valueService))
                    {
                        throw new Exception($"No se encontro el parametro 'webService[{index}].name' en la configuracion del procesador");
                    }
                    service.name = valueService.ToString();
                    
                    if (!keyValuePairs.TryGetValue("uri", out valueService))
                    {
                        throw new Exception($"No se encontro el parametro 'webService[{index}].uri' en la configuracion del procesador");
                    }
                    service.uri = valueService.ToString();

                    if (keyValuePairs.ContainsKey("parameters"))
                    {
                        if (!keyValuePairs.TryGetValue("parameters", out valueService))
                        {
                            throw new Exception($"No se encontro el parametro 'webService[{index}].parameters' en la configuracion del procesador");
                        }
                        service.dParameters = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(valueService.ToString());
                    }
                    index++;
                    this._dWebServices.Add(service.name, service);
                }

                if (!config.TryGetValue("minutesTimer", out value))
                {
                    throw new Exception("No se encontro el parametro 'minutesTimer' en la configuracion del procesador");
                }
                this._minuteTimerOffSet = Convert.ToUInt32(value.ToString());
                if (this._minuteTimerOffSet < 1) //menos de 1 min
                {
                    throw new Exception("El parametros 'minutesTimer' debe ser al menos 1 minuto");
                }
                this._countGlobalTimer = _minuteTimerOffSet * 6; //El time se desborda cada 10 segundos, por tanto 6 veces por minuto.

                //detiene el timer
                aTimer.Enabled = false;

                //obtener del servicio 'login' la sessionId que se usara en la llamada al serviocio para generar alerta de canal .
                //Implementar la llamada al servicio "login"
                dLoginDataForXVision.Clear();
                dLoginDataForXVision.Add("user", _dWebServices["login"].dParameters["user"].ToString());
                dLoginDataForXVision.Add("pass", _dWebServices["login"].dParameters["password"].ToString());
                dLoginDataForXVision.Add("localIP", "");
                string sPostDataIn = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, string>>(dLoginDataForXVision);

                _logger.Debug("'Login' a XymaVision -> " + sPostDataIn);
                string tempSessionId = null;

                if (!RequestPOST(_dWebServices["login"].uri, sPostDataIn, ref tempSessionId))
                {
                    _logger.Error($"Error en la llamada al servicio 'login' ->  '{_dWebServices["login"].uri}'");
                    throw new Exception($"Error en la llamada al servicio 'login' ->  '{_dWebServices["login"].uri}'");
                }
                _logger.Debug($"Respuesta al login: {tempSessionId}");

                tempSessionId = tempSessionId.Replace("\"", "");
                if (tempSessionId == "USER_ERROR")
                {
                    _logger.Error("El usuario y/o la contraseña definidos para autenticarse en XymaVision es incorrecto");
                    throw new Exception("El usuario y/o la contraseña definidos para autenticarse en XymaVision es incorrecto");
                }
                else if (tempSessionId == "ERROR" || tempSessionId == "SERVICEDISABLED_ERROR" || tempSessionId == "PARAM_ERROR" ||
                    tempSessionId == "SERVICEDISABLED_ERROR" || tempSessionId == "CONFIG_ERROR" || tempSessionId == "LICX_ERROR" ||
                    tempSessionId == "NOCHANNELS_ERROR" || tempSessionId == "NORECORD_ERROR" || tempSessionId == "CONNECTION_LOST")

                {
                    _logger.Error($"El servicio '{_dWebServices["login"].uri}' retornó '{tempSessionId}'");
                    throw new Exception($"El servicio '{_dWebServices["login"].uri}' retornó '{tempSessionId}'");
                }
                else
                {
                    selfSessionId = tempSessionId;
                }

                // Start the timer
                aTimer.Enabled = true;


                _logger.Trace("Fin");
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        
        private void OnTimedEvent(Object source, System.Timers.ElapsedEventArgs e)
        {
            try
            {
                _logger.Trace("Inicio");
                List<string> lOperatorToRemove = new List<string>();
                mut.WaitOne();
                if (dOperators.Count != 0)
                {
                    foreach (KeyValuePair<string, Operator> entry in dOperators)
                    {
                        if (entry.Value.channels.Count != 0)
                        {
                            List<Channel> lChannelToRemove = new List<Channel>();

                            foreach (Channel channel in entry.Value.channels)
                            {
                                if (channel.CounterKeepAliveOver(timeOutConexion))
                                {
                                    string alertMssg = $"Alerta!! Desconexion abrupta del operador '{entry.Value.userName}' en la sesion '{entry.Value.sessionId}' que operaba el radar: '{channel.description}'";

                                    //Llamada al servicio "Enviar alarma sobre un canal"
                                    Dictionary<string, object> dPostDataIn = new Dictionary<string, object>();
                                    dPostDataIn.Add("sessionID", selfSessionId);
                                    dPostDataIn.Add("channelNo", Convert.ToInt32(channel.id));
                                    dPostDataIn.Add("text", alertMssg);
                                    //dPostDataIn.Add("imgCode", GenerateImage(alertMssg));
                                    dPostDataIn.Add("imgCode", "");

                                    string sPostDataIn = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dPostDataIn);
                                    string result = "";
                                    _logger.Debug("'Alarma' a XymaVision -> " + sPostDataIn);
                                    Dictionary<string, object> temp = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(sPostDataIn);

                                    if (!RequestPOST(_dWebServices["sendAlarm"].uri, sPostDataIn, ref result))
                                    {
                                        _logger.Error($"Error en la llamada al servicio 'sendAlarm' ->  '{_dWebServices["sendAlarm"].uri}'");
                                        throw new Exception($"Error en la llamada al servicio 'sendAlarm' ->  '{_dWebServices["sendAlarm"].uri}'");
                                    }

                                    lChannelToRemove.Add(channel);
                                    _logger.Debug($"Perdida de conexion con la aplicacion que esta operando el usuario {entry.Value.userName} en el radar: '{channel.description}'");

                                    string currentInactId;
                                    string currentConnectionId;
                                    Dictionary<string, object> data = new Dictionary<string, object>();
                                    if (channel.StopInactivity(out currentInactId, out currentConnectionId))
                                    {
                                        //Send AutoInactivity
                                        data = GenerateInactivity(entry.Value.userName, entry.Value.sessionId, channel.id, channel.description, currentConnectionId, currentInactId, "-1");
                                        for (int index = 0; index < _listSerializer.Count; index++)
                                        {
                                            for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                            {
                                                _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                            }
                                        }
                                    }
                                }

                                if (channel.CounterOver(_countGlobalTimer))
                                {
                                    string alertMssg = $"Alerta!! Inactividad del operador '{entry.Value.userName}' en el radar: {channel.description}";

                                    if (channel.StartInactivity()) //verifica si se inicio o es continuacion de una inactividad
                                    {
                                        Dictionary <string,object> dataOut = GenerateInactivity(entry.Value.userName, entry.Value.sessionId, channel.id, channel.description,channel.conectionId, channel.currentInactId, "1");
                                        for (int index = 0; index < _listSerializer.Count; index++)
                                        {
                                            for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                            {
                                                _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                                            }
                                        }
                                    }

                                    //Llamada al servicio "Enviar alarma sobre un canal"
                                    Dictionary<string, object> dPostDataIn = new Dictionary<string, object>();
                                    dPostDataIn.Add("sessionID", selfSessionId);
                                    dPostDataIn.Add("channelNo", Convert.ToInt32(channel.id));
                                    dPostDataIn.Add("text", alertMssg);
                                    //dPostDataIn.Add("imgCode", GenerateImage(alertMssg));
                                    dPostDataIn.Add("imgCode", "");

                                    string sPostDataIn = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dPostDataIn);
                                    string result = "";
                                    _logger.Debug("'Alarma' a XymaVision -> " + sPostDataIn);
                                    Dictionary<string, object> temp = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(sPostDataIn);

                                    if (!RequestPOST(_dWebServices["sendAlarm"].uri, sPostDataIn, ref result))
                                    {
                                        _logger.Error($"Error en la llamada al servicio 'sendAlarm' ->  '{_dWebServices["sendAlarm"].uri}'");
                                        throw new Exception($"Error en la llamada al servicio 'sendAlarm' ->  '{_dWebServices["sendAlarm"].uri}'");
                                    }
                                    _logger.Debug($"Respuesta a la alerta: {result}");
                                    channel.CounterReset();
                                }
                            }
                            for (int i = 0; i < lChannelToRemove.Count; i++)
                            {
                                if (entry.Value.channels.Remove(lChannelToRemove[i]))
                                {
                                    _logger.Debug($"Perdida de conexion con la aplicacion que esta operando el usuario {entry.Value.userName} en el radar {lChannelToRemove[i].description}");
                                    //Send AutoRemoteConnection
                                    Dictionary<string, object> data = GenerateRemoteConnection(entry.Value.userName, entry.Value.sessionId, lChannelToRemove[i].id, lChannelToRemove[i].description,lChannelToRemove[i].conectionId, "-1");
                                    for (int index = 0; index < _listSerializer.Count; index++)
                                    {
                                        for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                        {
                                            _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                        }
                                    }
                                }
                            }
                        }
                        if (entry.Value.CounterOver(timeOutConexion))
                        {
                            if (entry.Value.channels.Count == 0)
                            {
                                lOperatorToRemove.Add(entry.Value.sessionId);
                                _logger.Debug($"Perdida de conexion con la aplicacion que esta operando el usuario {entry.Value.userName} en la sesion {entry.Value.sessionId}");
                                //Send AutoUserLogin
                                Dictionary<string,object> data = GenerateUserLogin(entry.Value.userName, entry.Value.sessionId, "-1");
                                for (int index = 0; index < _listSerializer.Count; index++)
                                {
                                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                                    {
                                        _listSerializer[index].serialize(_mssgTypes[indexTM], data);
                                    }
                                }
                            }
                        }
                    }
                    if (lOperatorToRemove.Any())
                    {
                        foreach (var element in lOperatorToRemove)
                            dOperators.Remove(element);
                    }
                }
                lOperatorToRemove.Clear();
                mut.ReleaseMutex();

                #region keepAlive contra XymaVision
                Dictionary<string, object> dPostDataOut = new Dictionary<string, object>();
                dPostDataOut.Add("sessionID", selfSessionId);

                string sPostDataOut = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dPostDataOut);
                string resultOut = "";
                _logger.Info("'KeepAlive' a XymaVision -> " + sPostDataOut);
                Dictionary<string, object> tempOut = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(sPostDataOut);

                if (!RequestPOST(_dWebServices["keepAlive"].uri, sPostDataOut, ref resultOut))
                {
                    _logger.Error($"Error en la llamada al servicio 'keepAlive' ->  '{_dWebServices["keepAlive"].uri}'");
                    throw new Exception($"Error en la llamada al servicio 'keepAlive' ->  '{_dWebServices["keepAlive"].uri}'");
                }
                if (!resultOut.ToLower().Equals("ok") && !resultOut.ToLower().Equals("true"))
                {
                    _logger.Debug($"la sección expiró, el modulo debe autenticarse nuevamente, Respuesta: {resultOut}");

                    string sDataIn = System.Text.Json.JsonSerializer.Serialize<Dictionary<string, string>>(dLoginDataForXVision);
                    _logger.Debug("'Login' a XymaVision -> " + sDataIn);

                    string tempSessionId = null;

                    if (!RequestPOST(_dWebServices["login"].uri, sDataIn, ref tempSessionId))
                    {
                        _logger.Error($"Error en la llamada al servicio 'login' ->  '{_dWebServices["login"].uri}'");
                        throw new Exception($"Error en la llamada al servicio 'login' ->  '{_dWebServices["login"].uri}'");
                    }
                    _logger.Debug($"Respuesta al login: {tempSessionId}");
                    tempSessionId = tempSessionId.Replace("\"", "");
                    if (tempSessionId == "USER_ERROR")
                    {
                        _logger.Error("El usuario y/o la contraseña definidos para autenticarse en XymaVision es incorrecto");
                        throw new Exception("El usuario y/o la contraseña definidos para autenticarse en XymaVision es incorrecto");
                    }
                    else if (tempSessionId == "ERROR" || tempSessionId == "SERVICEDISABLED_ERROR" || tempSessionId == "PARAM_ERROR" || 
                        tempSessionId == "SERVICEDISABLED_ERROR" || tempSessionId == "CONFIG_ERROR" || tempSessionId == "LICX_ERROR" ||
                        tempSessionId == "NOCHANNELS_ERROR" || tempSessionId == "NORECORD_ERROR" ||  tempSessionId == "CONNECTION_LOST" )

                    {
                        _logger.Error($"El servicio '{_dWebServices["login"].uri}' retornó '{tempSessionId}'");
                        throw new Exception($"El servicio '{_dWebServices["login"].uri}' retornó '{tempSessionId}'");
                    }
                    else
                    {
                        selfSessionId = tempSessionId;
                    }

                }
                #endregion
                _logger.Trace("Fin");
            }
            catch(Exception)
            { 

            }
        }
        
        private bool RequestPOST(string url, string postData, ref string result)
        {
            WebResponse response = null;
            Stream dataStream = null;

            try
            {
                WebRequest request = WebRequest.Create(url);
                request.Method = "POST";
                byte[] byteArray = Encoding.UTF8.GetBytes(postData);
                request.ContentType = "application/json";
                request.ContentLength = byteArray.Length;
                dataStream = request.GetRequestStream();
                dataStream.Write(byteArray, 0, byteArray.Length);
                dataStream.Close();
                response = request.GetResponse();
                using (dataStream = response.GetResponseStream())
                {
                    StreamReader reader = new StreamReader(dataStream);
                    string responseFromServer = reader.ReadToEnd();
                    result = responseFromServer;
                }
            }
            catch (Exception e)
            {
                result = "";
                _logger.Error( e.Message.ToLower());
                return false;
            }
            finally
            {
                response?.Close();
                dataStream?.Close();
            }
            return true;
        }
       
        private Dictionary<string,object> GenerateUserLogin( string userName, string sessionId, string param1, string ip = null, string mac = null, string pcname = null, string timestampOrigen = null)
        {
            try
            {
                Dictionary<string, object> dOut = new Dictionary<string, object>();

                string timestampServer = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                string actionId = "UserLogin";

                dOut.Add("actionId", actionId);
                dOut.Add("timestamp", timestampServer);
                dOut.Add("param1", param1);
                dOut.Add("userName", userName);
                dOut.Add("sessionId", sessionId);

                if (timestampOrigen != null) 
                    dOut.Add("timestampOrigen", timestampOrigen);
                if(ip != null)
                    dOut.Add("ip", ip);
                if (mac != null) 
                    dOut.Add("mac", mac);
                if (pcname != null) 
                    dOut.Add("pcName", pcname);

                return dOut;
            }
            catch (Exception e)
            {
                _logger.Error(e.Message.ToLower());
                return null;
            }
        }

        private Dictionary<string, object> GenerateRemoteConnection( string userName, string sessionId, string channelId, string channelDescription, string connetionId, string param1, string timestampOrigen = null)
        {
            try
            {
                Dictionary<string, object> dOut = new Dictionary<string, object>();

                string timestampServer = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                string actionId = "RemoteConnection";

                dOut.Add("actionId", actionId);
                dOut.Add("timestamp", timestampServer);
                if(timestampOrigen != null) 
                    dOut.Add("timestampOrigen", timestampOrigen);
                dOut.Add("param1", param1);
                dOut.Add("userName", userName);
                dOut.Add("sessionId", sessionId);
                dOut.Add("channelId", channelId);
                dOut.Add("channelDescription", channelDescription);
                dOut.Add("connectionId", connetionId);

                return dOut;
            }
            catch (Exception e)
            {
                _logger.Error(e.Message.ToLower());
                return null;
            }
        }
    
        private Dictionary<string, object> GenerateInactivity(string userName, string sessionId, string channelId, string channelDescription, string connexionId, string inactivityId, string param1)
        {
            try
            {
                Dictionary<string, object> dOut = new Dictionary<string, object>();

                string timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff");
                string actionId = "Inactivity";


                dOut.Add("actionId", actionId);
                dOut.Add("timestamp", timestamp);
                dOut.Add("param1", param1);
                dOut.Add("userName", userName);
                dOut.Add("sessionId", sessionId);
                dOut.Add("channelId", channelId);
                dOut.Add("channelDescription", channelDescription);
                dOut.Add("connectionId", connexionId);
                dOut.Add("inactivityId", inactivityId);

                return dOut;
            }
            catch (Exception e)
            {
                _logger.Error(e.Message.ToLower());
                return null;
            }
        }
        
        private Dictionary<string, object> UpdateTimeStamp(Dictionary<string, object> dataIn, string connectionId) 
        {
            try
            {
                Dictionary<string, object> dataOut = dataIn;
                if (dataOut.ContainsKey("timestamp"))
                {
                    string timestampOrigen = dataOut["timestamp"].ToString();
                    dataOut.Add("timestampOrigen", timestampOrigen);
                }
                string timestampServer = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
                dataOut["timestamp"] = timestampServer;
                dataOut.Add("connectionId", connectionId);
                
                return dataOut;
            }
            catch (Exception e)
            {
                _logger.Error(e.Message.ToLower());
                return null;
            }
        }
    }
}