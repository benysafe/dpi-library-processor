using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace CzmlBuilder
{

    public class BodyElementCzml
    {
        public BodyElementCzml()
        {
            BodyElement = new Dictionary<string, object>();
        }
        public BodyElementCzml(Dictionary<string, object> dic)
        {
            this.BodyElement = new Dictionary<string, object>(dic);
        }
        public BodyElementCzml(BodyElementCzml bodyElement)
        {
            this.BodyElement = bodyElement.toDictionary();
        }
        private List<string> Intervals = new List<string>();
        private string FullInterval { get; set; }

        const string EndInfinityInterval = "2200-12-01T00:01:01.000Z";
        
        private Dictionary<string, object> BodyElement;
        public bool TryMakeIntervals(string begin, List<int> minutesOffSets)
        {
            if (begin is null || minutesOffSets is null || minutesOffSets.Count == 0)
            {
                return false;
            }
            int year = Convert.ToInt32(begin.Substring(0, 4));
            int month = Convert.ToInt32(begin.Substring(5, 2));
            int day = Convert.ToInt32(begin.Substring(8, 2));
            int hours = Convert.ToInt32(begin.Substring(11, 2));
            int minutes = Convert.ToInt32(begin.Substring(14, 2));
            int seconds = Convert.ToInt32(begin.Substring(17, 2));
            int milis = Convert.ToInt32(begin.Substring(20, 3));

            DateTime beginDt = new DateTime(year, month, day, hours, minutes, seconds, milis, DateTimeKind.Utc);        //Todas las Fechas-Horas son en UTC
            int totalMinutesOffSet = 0;
            string indexIntervalbegin = beginDt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            string indexIntervalend;
            this.FullInterval = indexIntervalbegin + "/";

            DateTime iteratorDt = beginDt;

            for (int index = 0; index < minutesOffSets.Count; index++)
            {
                if (index == minutesOffSets.Count - 1 && minutesOffSets[index] == -1)
                {
                    indexIntervalend = EndInfinityInterval;
                }
                else
                {
                    TimeSpan minutesTs = TimeSpan.FromMinutes((double)minutesOffSets[index]);
                    DateTime indexDt = iteratorDt.Add(minutesTs);
                    iteratorDt = indexDt;
                    indexIntervalend = indexDt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");

                    totalMinutesOffSet = totalMinutesOffSet + minutesOffSets[index];
                }
                string interval = indexIntervalbegin + "/" + indexIntervalend;
                this.Intervals.Add(interval);

                indexIntervalbegin = indexIntervalend;
            }
            if (minutesOffSets.Last() ==  -1)
            {
                this.FullInterval = this.FullInterval + EndInfinityInterval;
            }
            else
            {
                TimeSpan minutesTotal = TimeSpan.FromMinutes((double)totalMinutesOffSet);
                DateTime totalDt = beginDt.Add(minutesTotal);
                this.FullInterval = this.FullInterval + totalDt.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            }
            return true;

        }
        public Dictionary<string, object> MakeBillboard(Billboard defaultData, Billboard currentIterationData)
        {
            Dictionary<string, object> billboard = new Dictionary<string, object>();
           
            #region image
            try
            {
                List<object> list = currentIterationData.image.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                for (int index = 0; index < list.Count; index++)
                {
                    Dictionary<string, object> element = new Dictionary<string, object>();
                    element.Add("interval", Intervals[index]);
                    element.Add("uri", list[index].ToString());
                    elements.Add(element);
                }
                billboard.Add("image", elements);
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.image.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    for (int index = 0; index < list.Count; index++)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.Intervals[index]);
                        element.Add("uri", list[index].ToString());
                        elements.Add(element);
                    }
                    billboard.Add("image", elements);

                }
                catch
                {
                    throw new Exception($"El paraetro 'image' es obligatorio en el billboard");
                }
            }
            #endregion

            #region show
            try
            {
                List<object> list = currentIterationData.show.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                if (list.Count == 1)
                {
                    billboard.Add("show", (bool)list[0]);
                }
                else
                {
                    for (int index = 0; index < list.Count; index++)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", Intervals[index]);
                        element.Add("value", (bool)list[index]);
                        elements.Add(element);
                    }
                    billboard.Add("show", elements);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.show.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    if (list.Count == 1)
                    {
                        billboard.Add("show", (bool)list[0]);
                    }
                    else
                    {
                        for (int index = 0; index < list.Count; index++)
                        {
                            Dictionary<string, object> element = new Dictionary<string, object>();
                            element.Add("interval", this.Intervals[index]);
                            element.Add("value", (bool)list[index]);
                            elements.Add(element);
                        }
                        billboard.Add("show", elements);
                    }
                }
                catch
                { }
            }            
            #endregion

            #region color
            try
            {
                List<object> list = currentIterationData.color.ToList();
                Dictionary<string, object> color = new Dictionary<string, object>();
                if (list.Count == 4)
                {
                    color.Add("rgba", list);
                    billboard.Add("color", color);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.color.ToList();
                    Dictionary<string, object> color = new Dictionary<string, object>();
                    if (list.Count == 4)
                    {
                        color.Add("rgba", list);
                        billboard.Add("color", color);
                    }
                }
                catch
                { }
            }
            #endregion

            #region eyeOffset

            #endregion
            
            #region horizontalOrigin
            try
            {
                List<object> list = currentIterationData.horizontalOrigin.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                if (list.Count == 1)
                {
                    billboard.Add("horizontalOrigin", list[0].ToString());
                }
                else
                {
                    for (int index = 0; index < list.Count; index++)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.Intervals[index]);
                        element.Add("value", list[index].ToString());
                        elements.Add(element);
                    }
                    billboard.Add("horizontalOrigin", elements);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.horizontalOrigin.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    if (list.Count == 1)
                    {
                        billboard.Add("horizontalOrigin",list[0].ToString());
                    }
                    else
                    {
                        for (int index = 0; index < list.Count; index++)
                        {
                            Dictionary<string, object> element = new Dictionary<string, object>();
                            element.Add("interval", this.Intervals[index]);
                            element.Add("value", list[index].ToString());
                            elements.Add(element);
                        }
                        billboard.Add("horizontalOrigin", elements);
                    }
                }
                catch
                { }
            }
            #endregion

            #region verticalOrigin
            try
            {
                List<object> list = currentIterationData.verticalOrigin.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                if (list.Count == 1)
                {
                    billboard.Add("verticalOrigin", list[0].ToString());
                }
                else
                {
                    for (int index = 0; index < list.Count; index++)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.Intervals[index]);
                        element.Add("value", list[index].ToString());
                        elements.Add(element);
                    }
                    billboard.Add("verticalOrigin", elements);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.verticalOrigin.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    if (list.Count == 1)
                    {
                        billboard.Add("verticalOrigin", list[0].ToString());
                    }
                    else
                    {
                        for (int index = 0; index < list.Count; index++)
                        {
                            Dictionary<string, object> element = new Dictionary<string, object>();
                            element.Add("interval", this.Intervals[index]);
                            element.Add("value", list[index].ToString());
                            elements.Add(element);
                        }
                        billboard.Add("verticalOrigin", elements);
                    }
                }
                catch
                { }
            }
            #endregion

            #region pixelOffset

            #endregion

            #region scale
            try
            {
                List<object> list = currentIterationData.scale.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                if (list.Count == 1)
                {
                    Dictionary<string, object> element = new Dictionary<string, object>();
                    element.Add("interval", this.FullInterval);
                    element.Add("number", Convert.ToDouble(list[0].ToString()));
                    billboard.Add("scale", element);
                }
                else
                {
                    for (int index = 0; index < list.Count; index++)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.Intervals[index]);
                        element.Add("value", Convert.ToDouble(list[0].ToString()));
                        elements.Add(element);
                    }
                    billboard.Add("scale", elements);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.scale.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    if (list.Count == 1)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.FullInterval);
                        element.Add("number", Convert.ToDouble(list[0].ToString()));
                        billboard.Add("scale", element);
                    }
                    else
                    {
                        for (int index = 0; index < list.Count; index++)
                        {
                            Dictionary<string, object> element = new Dictionary<string, object>();
                            element.Add("interval", this.Intervals[index]);
                            element.Add("value", Convert.ToDouble(list[0].ToString()));
                            elements.Add(element);
                        }
                        billboard.Add("scale", elements);
                    }
                }
                catch
                { }
            } 
            #endregion

            #region rotation
            try
            {
                List<object> list = currentIterationData.rotation.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                if (list.Count == 1)
                {
                    Dictionary<string, object> element = new Dictionary<string, object>();
                    element.Add("interval", this.FullInterval);
                    element.Add("number", Convert.ToDouble(list[0].ToString()));
                    billboard.Add("rotation", element);
                }
                else
                {
                    for (int index = 0; index < list.Count; index++)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.Intervals[index]);
                        element.Add("value", Convert.ToDouble(list[0].ToString()));
                        elements.Add(element);
                    }
                    billboard.Add("rotation", elements);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.rotation.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    if (list.Count == 1)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("interval", this.FullInterval);
                        element.Add("number", Convert.ToDouble(list[0].ToString()));
                        billboard.Add("rotation", element);
                    }
                    else
                    {
                        for (int index = 0; index < list.Count; index++)
                        {
                            Dictionary<string, object> element = new Dictionary<string, object>();
                            element.Add("interval", this.Intervals[index]);
                            element.Add("value", Convert.ToDouble(list[0].ToString()));
                            elements.Add(element);
                        }
                        billboard.Add("rotation", elements);
                    }
                }
                catch
                { }
            }
            #endregion

            #region width

            #endregion
            #region height

            #endregion

            #region scaleByDistance
            try
            {
                List<object> list = currentIterationData.scaleByDistance.ToList();
                List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                if (list.Count == 4)
                {
                    Dictionary<string, object> element = new Dictionary<string, object>();
                    element.Add("nearFarScalar", list);
                    billboard.Add("scaleByDistance", element);
                }
            }
            catch
            {
                try
                {
                    List<object> list = defaultData.scaleByDistance.ToList();
                    List<Dictionary<string, object>> elements = new List<Dictionary<string, object>>();
                    if (list.Count == 4)
                    {
                        Dictionary<string, object> element = new Dictionary<string, object>();
                        element.Add("nearFarScalar", list);
                        billboard.Add("scaleByDistance", element);
                    }
                }
                catch
                { }
            }
            
            #endregion
            
            #region sizeInMeters

            #endregion
            #region alignedAxis

            #endregion
            #region pixelOffsetScaleByDistance

            #endregion
            #region translucencyByDistance

            #endregion
            #region heightReference

            #endregion
            #region imageSubRegion

            #endregion

            this.BodyElement.Add("billboard", billboard);
            return billboard;
        }
        public Dictionary<string, object> MakePosition(Position currentIterationData)
        {
            Dictionary<string, object> position = new Dictionary<string, object>();

            List<double> cartographicDegrees = new List<double>();

            cartographicDegrees.Add((double)currentIterationData.longitude);
            cartographicDegrees.Add((double)currentIterationData.latitude);
            cartographicDegrees.Add((double)currentIterationData.altitude);

            position.Add("cartographicDegrees", cartographicDegrees);
            position.Add("interval", this.FullInterval);

            this.BodyElement.Add("position", position);

            return position;
        }
        public List<Dictionary<string, object>> MakeProperties(Properties propertiesElement)
        {
            List<Dictionary<string, object>> properties = new List<Dictionary<string, object>>();
            Dictionary<string, object> property = new Dictionary<string, object>();
            Dictionary<string, object> values = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(JsonConvert.SerializeObject(propertiesElement));
            Dictionary<string, object> othersPropertiesValues = System.Text.Json.JsonSerializer.Deserialize<Dictionary<string, object>>(JsonConvert.SerializeObject(propertiesElement.otherProperties));
            values.Remove("instanceId");
            values.Remove("otherProperties");

            foreach (var item in othersPropertiesValues)
            {
                values.Add(item.Key, item.Value);
            }

            property.Add("interval", FullInterval);
            property.Add("values", values);

            properties.Add(property);

            BodyElement.Add("properties", properties);

            return properties;
        }
        public string MakeId(string id)
        {
            this.BodyElement.Add("id", id);
            return id;
        }
        public string MakeName(string name)
        {
            this.BodyElement.Add("name", name);
            return name;
        }
        public string MakeDescription(string description)
        {
            this.BodyElement.Add("description", description);
            return description;
        }
        public Dictionary<string, object> toDictionary()
        {
            return this.BodyElement;
        }
        public static BodyElementCzml fromDictionary(Dictionary<string, object> value)
        {
            BodyElementCzml temp = new BodyElementCzml(value);
            return temp;
        }
        private bool TryGetBillboardParam(string keyParam, Dictionary<string, object> dictionary, out object values)
        {
            values = null;
            List<object> list = new List<object>();
            if (!dictionary.Any())
            {
                return false;
            }
            if (!dictionary.ContainsKey(keyParam))
            {
                return false;
            }
            object value;
            if (dictionary.TryGetValue(keyParam, out value))
            {
                try
                {
                    list = System.Text.Json.JsonSerializer.Deserialize<List<object>>(value.ToString());
                }
                catch (Exception ex)
                {
                    list.AddRange(value as object[]);
                }
                if (list.Count == 0)
                {
                    return false;
                }
                else if (list.Count > 1 && list.Count > this.Intervals.Count)
                {
                    return false;
                }
                else if (list.Count == 1)
                {
                    list.RemoveRange(1, list.Count - 1);
                }
            }
            values = list;
            return true;
        }
    }
    public class Czml 
    {
        public Czml()
        {
            CzmlHeader = new Dictionary<string, object>();
            CzmlBody = new List<BodyElementCzml>();
        }
        private Dictionary<string, object> CzmlHeader;
        private List<BodyElementCzml> CzmlBody;

        public bool TryGetHeader(out Dictionary<string, object> value)
        {
            value = null;
            if (CzmlHeader.Any())
            {
                value = CzmlHeader;
                return true;
            }
            return false;
        }
        public bool TrySetHeader(Dictionary<string, object> value)
        {
            if (value.Any())
            {
                if (CzmlHeader.Any())
                    CzmlHeader.Clear();
                CzmlHeader = value;
                return true;
            }
            return false;
        }
        public bool TryGetBodyElement(int index, out Dictionary<string, object> value)
        {
            value = null;
            if (index == 0)
                return false;
            if (CzmlBody.Any())
            {
                value = CzmlBody[index].toDictionary();
                return true;
            }
            return false;
        }
        public bool TrySetBodyElement(int index, Dictionary<string, object> dicInstance, Dictionary<string, object> dicDefaultBillboard, string entityId, string entityName, string entityDescription, List<int> intervalsOffsets)
        {
            if (dicInstance.Any())
            {
                BodyElementCzml bodyElementCzml = new BodyElementCzml();

                Instance instance = JsonConvert.DeserializeObject<Instance>(System.Text.Json.JsonSerializer.Serialize< Dictionary<string,object>>(dicInstance));

                if (!bodyElementCzml.TryMakeIntervals(instance.properties.adquireAt, intervalsOffsets))     //construye los intervalos para usar en una instancia del Czml
                {
                    throw new Exception($"No fue posible generar los intervalos para la instancia {index}");
                }

                bodyElementCzml.MakeId(entityId);
                bodyElementCzml.MakeName(entityName);
                if (entityDescription != null)
                {
                    bodyElementCzml.MakeDescription(entityDescription);
                }
                bodyElementCzml.MakeProperties(instance.properties);
                bodyElementCzml.MakePosition(instance.position);

                Billboard defaultBillboard = JsonConvert.DeserializeObject<Billboard>(System.Text.Json.JsonSerializer.Serialize<Dictionary<string, object>>(dicDefaultBillboard));
                bodyElementCzml.MakeBillboard(defaultBillboard, instance.currentGeometryProperties.billboard);

                CzmlBody.Add(bodyElementCzml);
                return true;
            }
            return false;
        }
        public List<Dictionary<string, object>> GetCzml()
        {
            List<Dictionary<string, object>> _CzmlData = new List<Dictionary<string, object>>();
            _CzmlData.Insert(0, CzmlHeader);
            foreach (BodyElementCzml element in CzmlBody)
            {
                _CzmlData.Add(element.toDictionary());
            }
            return _CzmlData;
        }
        override public string ToString()
        {
            List<Dictionary<string, object>> _CzmlData = GetCzml();
            if (_CzmlData.Any())
                return System.Text.Json.JsonSerializer.Serialize<List<Dictionary<string, object>>>(_CzmlData);
            else
                return "";
        }
    }

    public class Instance
    {
        public Properties properties { get; set; }
        public Position position { get; set; }
        public Currentgeometryproperties currentGeometryProperties { get; set; }
    }

    public class Properties
    {
        public string instanceId { get; set; }
        public string adquireAt { get; set; }
        public object otherProperties { get; set; }
    }

    public class Position
    {
        public double latitude { get; set; }
        public double longitude { get; set; }
        public double altitude { get; set; }
    }

    public class Currentgeometryproperties
    {
        public Billboard billboard { get; set; }
    }

    public class Billboard
    {
        public object[] show { get; set; }
        public object[] image { get; set; }
        public object[] color { get; set; }
        public object[] eyeOffset { get; set; }
        public object[] horizontalOrigin { get; set; }
        public object[] verticalOrigin { get; set; }
        public object[] pixelOffset { get; set; }
        public object[] scale { get; set; }
        public object[] rotation { get; set; }
        public object[] width { get; set; }
        public object[] height { get; set; }
        public object[] scaleByDistance { get; set; }
        public object[] sizeInMeters { get; set; }
        public object[] alignedAxis { get; set; }
        public object[] pixelOffsetScaleByDistance { get; set; }
        public object[] translucencyByDistance { get; set; }
        public object[] heightReference { get; set; }
        public object[] imageSubRegion { get; set; }
    }

}
