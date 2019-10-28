﻿using System.IO;
using System.Text;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;

namespace Hangfire.Azure
{
    public class CosmosJsonSerializer : CosmosSerializer
    {
        private static readonly Encoding DefaultEncoding = new UTF8Encoding(false, true);
        private readonly JsonSerializer Serializer;
        private readonly JsonSerializerSettings serializerSettings;

        public CosmosJsonSerializer()
            : this(new JsonSerializerSettings())
        {
        }

        public CosmosJsonSerializer(JsonSerializerSettings serializerSettings)
        {
            this.serializerSettings = serializerSettings;
            Serializer = JsonSerializer.Create(this.serializerSettings);
        }

        public override T FromStream<T>(Stream stream)
        {
            using (stream)
            {
                if (typeof(Stream).IsAssignableFrom(typeof(T)))
                {
                    return (T)(object)(stream);
                }

                using (StreamReader sr = new StreamReader(stream))
                {
                    using (JsonTextReader jsonTextReader = new JsonTextReader(sr))
                    {
                        return Serializer.Deserialize<T>(jsonTextReader);
                    }
                }
            }
        }

        public override Stream ToStream<T>(T input)
        {
            MemoryStream streamPayload = new MemoryStream();
            using (StreamWriter streamWriter = new StreamWriter(streamPayload, encoding: DefaultEncoding, bufferSize: 1024, leaveOpen: true))
            {
                using (JsonWriter writer = new JsonTextWriter(streamWriter))
                {
                    writer.Formatting = Formatting.None;
                    Serializer.Serialize(writer, input);
                    writer.Flush();
                    streamWriter.Flush();
                }
            }
            streamPayload.Position = 0;
            return streamPayload;
        }
    }
}
