using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Threading.Tasks;

namespace HQCommon
{
	public class Serializer
	{
		public static T Deserialize<T>(string p_filename)
		{
			if (!File.Exists(p_filename))
				return default(T);
			FileStream stream = new FileStream(p_filename, FileMode.Open);
			BinaryFormatter bformatter = new BinaryFormatter();
			T listDeserialised = (T)bformatter.Deserialize(stream);
			stream.Dispose();
			return listDeserialised;
		}

		public static void Serialize<T>(string p_filename, T p_list)
		{
			FileStream stream = new FileStream(p_filename, FileMode.Create);
			BinaryFormatter bformatter = new BinaryFormatter();
			bformatter.Serialize(stream, p_list);
			stream.Dispose();
		}
	}
}
