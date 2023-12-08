using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AdapterPlateSearch
{
    public class Event 
    {
        public string IdLog = "";
        public string XSVServerId = "";
        public string ChannelId = "";
        public double latitude;
        public double longitude;
        public double altitude;
        public double speed;
        public double azimut;
        public double course;
        public DateTime detectedAt;
        public string plate ="";
        public string entityId = "";

        public void SetCourse(string subJson)
        {
            if (subJson != "")
            {
                try
                {
                    Dictionary<string, object> otrosDatos = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(subJson);
                    object value;
                    if (!otrosDatos.TryGetValue("course", out value))
                    {
                        if (azimut != 0)
                            this.course = azimut * (-1);
                        else
                            this.course = 0;
                    }
                    else
                    {
                        if (value.ToString() == "go")
                        {
                            if (azimut != 0)
                                this.course = azimut;
                            else
                                this.course = 0;
                        }
                        else if (value.ToString() == "come")
                        {
                            if (azimut != 0)
                                this.course = azimut * (-1);
                            else
                                this.course = 0;
                        }
                        else
                        {
                            if (azimut != 0)
                                this.course = azimut * (-1);
                            else
                                this.course = 0;
                        }
                    }
                    return;
                }
                catch
                {
                    this.course = azimut * (-1);
                }
            }
            return;
        }       
    }
}
