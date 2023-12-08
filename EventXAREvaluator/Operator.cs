using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventXAREvaluator
{
    public class Channel
    {
        public uint count;
        public uint countKeepAlive;
        public string id;
        public string description;
        public string conectionId;
        public string currentInactId;
        
        public Channel (string id , string desc)
        {
            this.count = 0;
            this.countKeepAlive = 0;
            this.id = id;
            this.description = desc;
            this.conectionId = Guid.NewGuid().ToString("N").ToUpper();
            this.currentInactId = "";
        }
        public bool StartInactivity()
        {
            if (currentInactId == "")
            {
                this.currentInactId = Guid.NewGuid().ToString("N").ToUpper();
                return true;
            }
            else
            {
                return false;
            }
        }
        public bool StopInactivity(out string InactivityId, out string ConnectionId)
        {
            if (currentInactId != "")
            {
                InactivityId = currentInactId;
                ConnectionId = this.conectionId;
                this.currentInactId = "";
                this.CounterReset();
                return true;
            }
            else
            {
                InactivityId = "";
                ConnectionId = this.conectionId;
                return false;
            }
        }
        public bool CounterOver(uint reference)
        {
            count++;
            if (reference <= count)
            {
                count = 0;
                return true;
            }
            return false;
        }
        public void CounterKeepAliveReset()
        {
            countKeepAlive = 0;
        }
        public bool CounterKeepAliveOver(uint reference)
        {
            countKeepAlive++;
            if (reference <= countKeepAlive)
            {
                countKeepAlive = 0;
                return true;
            }
            return false;
        }
        public void CounterReset()
        {
            count = 0;
        }

    }
    public class Operator
    {
        public List<Channel> channels;
        public string userName;
        public string sessionId;
        public uint count;
        public string currentConectionId;

        public Operator(string userName, string sessionId)
        {
            this.userName = userName;
            this.sessionId = sessionId;
            channels = new List<Channel>();
            count = 0;
            currentConectionId = "";
        }

        public bool ChannelConectionStarted()
        {
            if(currentConectionId == "")
            {
                currentConectionId = Guid.NewGuid().ToString("N").ToUpper();
                return true;
            }
            else { return false; }
        }

        public bool ChannelConectionFinished() 
        {
            if(currentConectionId != "")
            {
                currentConectionId = "";
                return true;    
            }
            else
            {
                return false;
            }
        }

        public bool RemoveChannel(string channelId, out Channel channelout)
        {
            foreach (Channel channel in channels)
            {
                if(channel.id.Equals(channelId))
                {
                    channelout = channel;
                    channels.Remove(channel);
                    return true;
                }
            }
            channelout = null;
            return false;
        }

        public void ActionDetected(string channelId)
        {
            foreach (Channel channel in channels)
            {
                if (channel.id.Equals(channelId))
                {
                    channel.CounterReset();
                    return;
                }
            }
        }

        public Channel GetChannel(string channelId)
        {
            foreach (Channel channel in channels)
            {
                if (channel.id.Equals(channelId))
                {
                    return channel;
                }
            }
            return null;
        }
        
        public void CounterReset()
        {
            count = 0;
        }

        public bool CounterOver(uint reference)
        {
            count++;
            if (reference <= count)
            {
                count = 0;
                return true;
            }
            return false;
        }
    }
}
