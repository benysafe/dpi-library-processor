using InterfaceLibraryConfigurator;
using InterfaceLibraryLogger;
using InterfaceLibraryProcessor;
using InterfaceLibrarySerializer;
using NLog;
using System;
using System.Collections.Generic;
using System.Threading;

namespace ProcessorModel
{
    public class ProModel : IProcessor
    {
        #region variables obligadas
        //Variables necesarias en todo procesador independientemente de la funcionalidad
        private Logger _logger;
        private string _id;
        private string _name;
        private List<ISerializer> _listSerializer = new List<ISerializer>(); //listado que contine todos los serializadores que usa el procesador para la salida de su(s) resultado(s)
        private List<string> _mssgTypes = new List<string>();           //listado que contiene los id de los mssgtypes establecidos en la configuracion
        private IConfigurator _configurator;
        #endregion variables obligadas

        #region variables opcionales segun el funcionamiento de las entradas y salidad del modulo
        private List<string> _mssgTypesName = new List<string>();                               //listado que contiene los nombres de los mssgtypes establecidos en la configuracion
        private Dictionary<string, string> _dRecipient = new Dictionary<string, string>();      //Diccionario que contiene los recipientes definidos en la configuracion,
        private static Mutex mut = new Mutex();                                                 //Variable tipo 'mutex' usada para evitar las posibles concurrencias segun la funcionalidad del procesador
        #endregion

        #region variables espesificas 
        //Variables propias de la funcionalidad espesifica del modulo procesador

        #endregion variables espesificas

        public void addSerializer(ISerializer serializer)
        {
            try
            {
                _logger.Trace("Inicio");

                _listSerializer.Add(serializer); //Agrega una nueva serializacion para su posible uso como mecanismo de salida de los resultados del procesamiento 

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
                _configurator = configurator;

            #region Obtencion de la configuracion
                //Obtener la configuracion espesifica del Procesador usando alguna de las siguentes variantes, la que se use debe estan en corcondancia con la biblioteca Deserializer usada en la conformacion del ejecutable del modulo.
                Dictionary<string, object> config = _configurator.getMap("processors", _id);
                object value;
                if (!config.TryGetValue("name", out value))
                {
                    throw new Exception("No se encontro el parametro 'name' en la configuracion del procesador");
                }
                _name = value.ToString();

                //nombre del registrador, para loggear, debe coincidir con el usado en el fichero de configuracion de log
                if (_name is null || _name == "")
                    _logger = (Logger)logger.init("LoggerName");
                else
                    _logger = (Logger)logger.init(_name);

                getConfig();
            #endregion Obtencion de la configuracion
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

                #region Reconfiguracion 'en caliente'
                if (_configurator.hasNewConfig())
                {
                    reConfig();
                }

                /*Si el módulo requiere parametros de configuracion externos al fichero funcional de configuracion, es necesario implementar
                un mecanismo para chequear y recargar esa configuracion.                                    
                 */
                #endregion Reconfiguracion 'en caliente'

                #region Logica del procesamiento
                //ToDo: Implementar la logica de procesamiento

                //Ejemplo: La salida de este procesador es un mensaje estructurado contenido en un Diccionario
                //{"Data": "procesador de prueba"}
                Dictionary<string, object> dataOut = new Dictionary<string, object>();
                string str = "procesador de prueba";
                dataOut.Add("Data", str as object);

                #region salida del procesador, en dependencia del tipo de mensaje al que se asocie 'dataOut' 
                //Variante 1: si 'dataOut' es de tipo 'mssgtype_1'
                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypesName.Count; indexTM++)
                    {
                        if (_mssgTypesName[indexTM] == "mssgtype_1")
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                        }
                    }
                }
                //Variante 2: si 'dataOut' es de tipo 'mssgtype_2'
                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypesName.Count; indexTM++)
                    {
                        if (_mssgTypesName[indexTM] == "mssgtype_2")
                        {
                            _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                        }
                    }
                }
                //Variante 3: si solo esta definido un 'mssgType' como salida del procesador
                for (int index = 0; index < _listSerializer.Count; index++)
                {
                    for (int indexTM = 0; indexTM < _mssgTypes.Count; indexTM++)
                    {
                        _listSerializer[index].serialize(_mssgTypes[indexTM], dataOut);
                    }
                }
                #endregion

                #endregion Logica del procesamiento

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
                _logger.Trace("Inicio");

                mut.WaitOne();
                getConfig();
                mut.ReleaseMutex();

                _logger.Trace("Fin");
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

                //Obtener la configuracion espesifica del Procesador usando alguna de las siguentes variantes, la que se use debe estan en corcondancia con la biblioteca Deserializer usada en la conformacion del ejecutable del modulo.
                Dictionary<string, object> config = _configurator.getMap("processors", _id);
                object value;

                //seccion para obtener los parametros relacionados con el atributo 'mssgtypes' de la configuraion
                if (config.TryGetValue("mssgTypes", out value))
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

                //seccion para obtener los parametros relacionados con el atributo 'recipientsNames' de la configuraion
                if (!config.TryGetValue("recipientsNames", out value))
                {
                    throw new Exception("No se encontro el parametro 'recipientsNames' en la configuracion del procesador");
                }
                else
                {
                    _dRecipient = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, string>>(value.ToString());

                    if (!_dRecipient.ContainsKey("recipient_1"))
                    {
                        throw new Exception("No se encontro el parametro 'recipient_1' en la configuracion 'recipientsNames' del procesador");
                    }
                    if (!_dRecipient.ContainsKey("mssgtype_2"))
                    {
                        throw new Exception("No se encontro el parametro 'mssgtype_2' en la configuracion 'recipientsNames' del procesador");
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
