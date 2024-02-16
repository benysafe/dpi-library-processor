using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NLog;
using InterfaceLibrarySerializer;
using System.Net.Sockets;
using System.Net;
using System.Data;

namespace TcpServerRouter
{
    internal class Server
    {
        private List<ISerializer> _listSerializer = new List<ISerializer>();
        private Logger _logger;
        private static Mutex mut = new Mutex();
        private string _preFixBroker = null;
        private string _ip = null;
        private string _port = null;

        TcpListener server = null;

        private Dictionary<string,TcpClient> dTcpClients = new Dictionary<string,TcpClient>();    

        public Server(string ip,string port,string preFixBroker, Logger log, Mutex mutex) 
        {
            try
            {
                _logger = log;
                mut = mutex;
                _preFixBroker = preFixBroker;
                _ip = ip;
                _port = port;

                StartServer();
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }

        public void StartServer() 
        {
            try
            {
                IPAddress localAddr = IPAddress.Parse(_ip);
                server = new TcpListener(localAddr, Convert.ToInt32(_port));
                server.Start();

                Thread threadServer = new Thread(this.StartListener);
                threadServer.IsBackground = false;
                threadServer.Start();

            }
            catch(Exception e) 
            {
                _logger.Error("Error en los parámetros del servidor");
                throw e;
            }
        }

        public void AddSerializer(ISerializer serializer)
        {
            try
            {
                _listSerializer.Add(serializer);
            }
            catch (Exception e)
            {
                _logger.Error(e.ToString());
                throw e;
            }
        }
        public void StopServer() 
        {
            try
            {
                //implementar la deteccion del servidor
                server.Stop();                
                foreach (KeyValuePair<string, TcpClient> pair in dTcpClients)
                {
                    pair.Value.Close();
                }
                dTcpClients.Clear();
                _listSerializer.Clear();
            }    
            catch (Exception e)
            {

            }
        }
        
        public bool SendToTcpCliente(string ip,string dataout)
        {
            try
            {
                if(!dTcpClients.ContainsKey(ip))
                {
                    _logger.Debug("Cliente '"+ip+"' no encontrado en los clientes registrados");
                    return false;
                }
                TcpClient client = dTcpClients[ip]; 
                var stream = client.GetStream();

                Byte[] reply = System.Text.Encoding.ASCII.GetBytes(dataout);
                stream.Write(reply, 0, reply.Length);

                return true; 
            }
            catch (Exception e) 
            {
                _logger.Error(e);
                return false;
            }
        }

        public void StartListener()
        {
            try
            {
                while (true)
                {
                    TcpClient client = server.AcceptTcpClient();

                    Thread t = new Thread(new ParameterizedThreadStart(HandleDeivce));
                    t.Start(client);

                    // Obtiene el IPEndPoint del host remoto
                    IPEndPoint remoteEndPoint = client.Client.RemoteEndPoint as IPEndPoint;

                    // Obtiene la dirección IP del host remoto
                    string ip = remoteEndPoint.Address.ToString();

                    if (!dTcpClients.TryAdd(ip, client))
                    {
                        _logger.Error("No se pudo agregar el cliente " + ip + " a la lista de clientes registrados");
                    }
                    else
                    {
                        _logger.Debug("Nuevo client TCP Connected: "+ip);
                    }
                }

            }
            catch (Exception e)
            {
                _logger.Error(e.Message);
                server.Stop();
                throw e;
            }
        }
                
        private void HandleDeivce(Object obj)
        {
            TcpClient client = (TcpClient)obj;
            var stream = client.GetStream();


            // Obtiene el IPEndPoint del host remoto
            IPEndPoint remoteEndPoint = client.Client.RemoteEndPoint as IPEndPoint;

            // Obtiene la dirección IP del host remoto
            string ip = remoteEndPoint.Address.ToString();

            string data = null;
            Byte[] bytes = new Byte[256];
            int i;
            // Obtener la fecha y hora actuales
            DateTime now = DateTime.Now.ToUniversalTime();
            try
            {
                while ((i = stream.Read(bytes, 0, bytes.Length)) != 0)
                {
                    //string hex = BitConverter.ToString(bytes);
                    data = Encoding.ASCII.GetString(bytes, 0, i);
                    _logger.Debug("Received: "+data+" :ip: "+ip);

                    //Aqui va la logica para enviar al serializar
                    Dictionary<string, object> dPayLoad = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(data);
                    dPayLoad.Add("timestamp", now.ToString("yy-MM-ddTHH:mm:ssZ"));

                    object idDevice = null;
                    if (!dPayLoad.TryGetValue("device_id", out idDevice))
                    {
                        _logger.Error("Error en el Json enviado por el dispositivo '" + ip +"'");
                    }
                    else
                    {
                        string sRecipent = _preFixBroker + "_" + idDevice.ToString();
                        for (int index = 0; index < _listSerializer.Count; index++)
                        {
                            _listSerializer[index].dynamicSerialize(sRecipent, dPayLoad);
                        }
                    }
                }
            }
            catch (Exception e)
            {
                _logger.Error("Exception: "+ e.ToString()+" desde");
                client.Close();
            }
        }
    }
}
