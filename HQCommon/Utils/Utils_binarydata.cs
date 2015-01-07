using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Runtime.InteropServices;

namespace HQCommon
{
    public static partial class Utils
    {
        // Consider using System.IO.BinaryReader and System.IO.BinaryWriter instead of 
        // the following tools. The difference: the following tools allow you to 
        // - read/write DateTime and TimeSpan
        // - read/write arrays
        #region Binary file
        /// <summary> Reads 1 value of type T from p_stream </summary>
        /// <typeparam name="T">One of the built-in primitive types, except for String. 
        /// DateTime and TimeSpan are supported</typeparam>
        /// <exception cref="EndOfStreamException"></exception>
        public static T ReadBinary<T>(Stream p_stream) where T : struct
        {
            // T is not String because String is not value type
            byte[] buff = new byte[SizeOfPrimitive(typeof(T))];
            if (p_stream.Read(buff, 0, buff.Length) < buff.Length)
                throw new EndOfStreamException();
            int startIdx = 0;
            return ParseBytes<T>(buff, ref startIdx);
        }

        /// <summary> Writes 1 value of type T to p_stream </summary>
        public static int WriteBinary<T>(Stream p_stream, T p_value) where T : struct
        {
            byte[] buff = GetBytes<T>(p_value);
            p_stream.Write(buff, 0, buff.Length);
            return buff.Length;
        }

        ///// <summary> Writes several values of type T to p_stream 
        ///// for non-primitive types (including reference types). </summary>
        //public static void WriteBinary<T>(Stream p_stream, IEnumerable<T> p_array, Action<T, Stream> p_writer)
        //{
        //    foreach (T t in p_array)
        //        p_writer(t, p_stream);
        //}

        /// <summary> Reads p_count bytes from p_stream and decodes it as an UTF8 string.
        /// Negative p_count causes reading until the first 0 byte.
        /// Postcondition: p_stream.Position is just after the last readed byte (e.g. 0 byte). </summary>
        /// <exception cref="ArgumentException">If p_count is negative and p_stream is not seekable</exception>
        public static string ReadUTF8String(Stream p_stream, int p_count)
        {
            if (p_count == 0)
                return "";
            byte[] buff;
            if (0 < p_count)
            {
                buff = new byte[p_count];
                p_stream.Read(buff, 0, buff.Length);
                return Encoding.UTF8.GetString(buff);
            }
            if (!p_stream.CanSeek)
                throw new ArgumentException();
            using (MemoryStream memStream = new MemoryStream())
            {
                buff = new byte[4096];
                int readed;
                do
                {
                    readed = p_stream.Read(buff, 0, buff.Length);
                    int zero = Array.IndexOf(buff, (byte)0, 0, readed);
                    if (0 <= zero)
                    {
                        memStream.Write(buff, 0, zero);
                        if (++zero < readed)
                            p_stream.Seek(zero - readed, SeekOrigin.Current);
                        break;
                    }
                    memStream.Write(buff, 0, readed);
                } while (readed > 0);
                memStream.Position = 0;
                return ReadUTF8String(memStream);
            }
        }

        /// <summary> Reads the remainder of p_stream and decodes it to string with UTF8 encoding.
        /// Zero bytes (if any) will be included in the returned string. </summary>
        public static string ReadUTF8String(Stream p_stream)
        {
            // Note: StreamReader.Dispose() is intentionally omitted, to avoid disposing p_stream.
            // Instead, the caller is responsible for disposing p_stream (more consistent API).
            return new StreamReader(p_stream, Encoding.UTF8).ReadToEnd();
        }

        public static int WriteUTF8String(Stream p_stream, string p_string, bool p_zeroTerminated)
        {
            byte[] buff = GetBytesUTF8(p_string, p_zeroTerminated);
            p_stream.Write(buff, 0, buff.Length);
            return buff.Length;
        }

        /// <summary> String to byte[] conversion </summary>
        public static byte[] GetBytesUTF8(string p_string, bool p_zeroTerminated)
        {
            byte[] result = Encoding.UTF8.GetBytes(p_string);
            if (p_zeroTerminated)
                Array.Resize(ref result, result.Length + 1);
            return result;
        }

        /// <summary> byte[] to T[] conversion.</summary>
        /// <exception cref="ArgumentException">if T is not a primitive type or DateTime/TimeSpan</exception>
        public static T[] ParseArray<T>(byte[] p_bytes, int p_startIdx) where T : struct
        {
            Type t = typeof(T);
            if (Type.GetTypeCode(t) == TypeCode.DateTime)
            {
                DateTime[] result = new DateTime[(p_bytes.Length - p_startIdx) / sizeof(Int64)];
                for (int i = 0; p_startIdx < p_bytes.Length; p_startIdx += sizeof(Int64))
                    result[i++] = DateTime.FromBinary(BitConverter.ToInt64(p_bytes, p_startIdx));
                return result as T[];
            }
            else if (typeof(TimeSpan).Equals(t))
            {
                TimeSpan[] result = new TimeSpan[(p_bytes.Length - p_startIdx) / sizeof(Int64)];
                for (int i = 0; p_startIdx < p_bytes.Length; p_startIdx += sizeof(Int64))
                    result[i++] = TimeSpan.FromTicks(BitConverter.ToInt64(p_bytes, p_startIdx));
                return result as T[];
            }
            else
            {
                int size = SizeOfPrimitive(t);
                T[] result = new T[(p_bytes.Length - p_startIdx) / size];
                Buffer.BlockCopy(p_bytes, p_startIdx, result, 0, result.Length * size);
                return result;
            }
        }

        /// <summary> byte[] to T[] conversion for non-primitive types (including reference types). </summary>
        public static IEnumerable<T> ParseArray<T>(byte[] p_bytes, int p_startIdx, Func<byte[], int[], T> p_parser)
        {
            int[] currentIdx = { p_startIdx };
            while (currentIdx[0] < p_bytes.Length)
                yield return p_parser(p_bytes, currentIdx);
        }

        /// <summary> byte[] to T conversion. </summary>
        public static T ParseBytes<T>(byte[] p_bytes)
        {
            int dummy = 0;
            return ParseBytes<T>(p_bytes, ref dummy);
        }

        /// <summary> byte[] to T conversion, increments p_startIdx </summary>
        public static T ParseBytes<T>(byte[] p_bytes, ref int p_startIdx)
        {
            switch (Type.GetTypeCode(typeof(T)))
            {
                case TypeCode.Boolean : return (T)(object)ParseBool(p_bytes, ref p_startIdx);
                case TypeCode.Char    : return (T)(object)ParseChar(p_bytes, ref p_startIdx);
                case TypeCode.SByte   : return (T)(object)(sbyte)p_bytes[p_startIdx++];
                case TypeCode.Byte    : return (T)(object)p_bytes[p_startIdx++];
                case TypeCode.Int16   : return (T)(object)ParseShort(p_bytes, ref p_startIdx);
                case TypeCode.UInt16  : return (T)(object)ParseUShort(p_bytes, ref p_startIdx);
                case TypeCode.Int32   : return (T)(object)ParseInt(p_bytes, ref p_startIdx);
                case TypeCode.UInt32  : return (T)(object)ParseUInt(p_bytes, ref p_startIdx);
                case TypeCode.Int64   : return (T)(object)ParseLong(p_bytes, ref p_startIdx);
                case TypeCode.UInt64  : return (T)(object)ParseULong(p_bytes, ref p_startIdx);
                case TypeCode.Single  : return (T)(object)ParseFloat(p_bytes, ref p_startIdx);
                case TypeCode.Double  : return (T)(object)ParseDouble(p_bytes, ref p_startIdx);
                case TypeCode.Decimal : return (T)(object)ParseDecimal(p_bytes, ref p_startIdx);
                case TypeCode.DateTime: return (T)(object)ParseDateTime(p_bytes, ref p_startIdx);
                case TypeCode.String  : return (T)(object)ParseStringUTF8(p_bytes, ref p_startIdx);
                default:
                    if (typeof(TimeSpan).Equals(typeof(T)))
                        return (T)(object)ParseTimeSpan(p_bytes, ref p_startIdx);
                    throw new ArgumentException();
            }
        }

        public static bool ParseBool(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(bool);
            return BitConverter.ToBoolean(p_bytes, idx); 
        }
        public static char ParseChar(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(char);
            return BitConverter.ToChar(p_bytes, idx); 
        }
        public static short ParseShort(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(short);
            return BitConverter.ToInt16(p_bytes, idx); 
        }
        public static ushort ParseUShort(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(ushort);
            return BitConverter.ToUInt16(p_bytes, idx); 
        }
        public static int ParseInt(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(int);
            return BitConverter.ToInt32(p_bytes, idx); 
        }
        public static uint ParseUInt(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(uint);
            return BitConverter.ToUInt32(p_bytes, idx); 
        }
        public static long ParseLong(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(long);
            return BitConverter.ToInt64(p_bytes, idx); 
        }
        public static ulong ParseULong(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(ulong);
            return BitConverter.ToUInt64(p_bytes, idx); 
        }
        public static float ParseFloat(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(float);
            return BitConverter.ToSingle(p_bytes, idx); 
        }
        public static double ParseDouble(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(double);
            return BitConverter.ToDouble(p_bytes, idx); 
        }
        public static decimal ParseDecimal(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(decimal);
            return new Decimal(new int[] {
                BitConverter.ToInt32(p_bytes, idx),
                BitConverter.ToInt32(p_bytes, idx+4),
                BitConverter.ToInt32(p_bytes, idx+8),
                BitConverter.ToInt32(p_bytes, idx+12)
            });
        }
        public static DateTime ParseDateTime(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(long);
            return DateTime.FromBinary(BitConverter.ToInt64(p_bytes, idx)); 
        }
        public static TimeSpan ParseTimeSpan(byte[] p_bytes, ref int p_startIdx)
        {
            int idx = p_startIdx;
            p_startIdx += sizeof(long);
            return TimeSpan.FromTicks(BitConverter.ToInt64(p_bytes, idx));
        }
        public static string ParseStringUTF8(byte[] p_bytes, ref int p_startIdx)
        {
            int i = Array.IndexOf(p_bytes, (byte)0, p_startIdx);
            int len = (i < 0) ? p_bytes.Length - p_startIdx : i - p_startIdx;
            string result = Encoding.UTF8.GetString(p_bytes, p_startIdx, len);
            p_startIdx += len - ~(i >> 31); // + ((i < 0) ? 0 : 1);
            return result;
        }


        /// <summary> T to byte[] conversion </summary>
        public static byte[] GetBytes<T>(T p_value)
        {
            object value = p_value;
            switch (Type.GetTypeCode(typeof(T)))
            {
                case TypeCode.Boolean : return BitConverter.GetBytes((bool)value);
                case TypeCode.Char    : return BitConverter.GetBytes((char)value);
                case TypeCode.SByte   : return new byte[] { (byte)(sbyte)value };
                case TypeCode.Byte    : return new byte[] { (byte)value };
                case TypeCode.Int16   : return BitConverter.GetBytes((Int16)value);
                case TypeCode.UInt16  : return BitConverter.GetBytes((UInt16)value);
                case TypeCode.Int32   : return BitConverter.GetBytes((Int32)value);
                case TypeCode.UInt32  : return BitConverter.GetBytes((UInt32)value);
                case TypeCode.Int64   : return BitConverter.GetBytes((Int64)value);
                case TypeCode.UInt64  : return BitConverter.GetBytes((UInt64)value);
                case TypeCode.Single  : return BitConverter.GetBytes((Single)value);
                case TypeCode.Double  : return BitConverter.GetBytes((Double)value);
                case TypeCode.Decimal : {
                    int[] ints = Decimal.GetBits((Decimal)value);
                    byte[] buff = new byte[ints.Length * sizeof(int)];
                    for (int i = ints.Length - 1; i >= 0; --i)
                        Array.Copy(BitConverter.GetBytes(ints[i]), 0, buff, i * sizeof(int), sizeof(int));
                    return buff;
                }
                case TypeCode.DateTime: return BitConverter.GetBytes(((DateTime)value).ToBinary());
                case TypeCode.String  : return GetBytesUTF8((String)value, true);
                default: 
                    if (typeof(TimeSpan).Equals(typeof(T)))
                        return BitConverter.GetBytes(((TimeSpan)value).Ticks);
                    throw new ArgumentException();
            }
        }

        /// <summary> T[] to byte[] conversion. </summary>
        /// <exception cref="ArgumentException">if T is not a primitive type or DateTime/TimeSpan</exception>
        public static byte[] GetBytes<T>(T[] p_array, int p_startIdx) where T : struct
        {
            byte[] result;
            DateTime[] dates;
            TimeSpan[] timeSpans;
            if (CanBe(p_array, out dates))
            {
                result = new byte[(dates.Length - p_startIdx) * sizeof(Int64)];
                for (int i = 0; p_startIdx < dates.Length; i += sizeof(Int64))
                    Buffer.BlockCopy(BitConverter.GetBytes(dates[p_startIdx++].ToBinary()), 0,
                        result, i, sizeof(Int64));
            }
            else if (CanBe(p_array, out timeSpans))
            {
                result = new byte[(timeSpans.Length - p_startIdx) * sizeof(Int64)];
                for (int i = 0; p_startIdx < timeSpans.Length; i += sizeof(Int64))
                    Buffer.BlockCopy(BitConverter.GetBytes(timeSpans[p_startIdx++].Ticks), 0,
                        result, i, sizeof(Int64));
            }
            else
            {
                int size = SizeOfPrimitive(typeof(T));
                result = new byte[(p_array.Length - p_startIdx) * size];
                Buffer.BlockCopy(p_array, p_startIdx * size, result, 0, result.Length);
            }
            return result;
        }

        /// <summary> T[] to byte[] conversion for non-primitive types (including reference types). </summary>
        public static byte[] GetBytes<T>(IEnumerable<T> p_array, Action<T, Stream> p_writer)
        {
            using (MemoryStream result = new MemoryStream())
            {
                foreach (T t in p_array)
                    p_writer(t, result);
                return result.ToArray();
            }
        }


        /// <summary> It's enough if it can determine the size of primitive types that have a TypeCode 
        /// constant (except for string) plus TimeSpan. </summary>
        public static int SizeOfPrimitive(Type p_type)
        {
            // Marshal.SizeOf() throws exception for DateTime (but not for TimeSpan, surprisingly..)
            if (Type.GetTypeCode(p_type) == TypeCode.DateTime)
                return sizeof(Int64);
            return Marshal.SizeOf(p_type);
            // Possible alternative:
            // return Buffer.ByteLength(Array.CreateInstance(p_type, 1));
        }
        #endregion

        /// <summary> Identical to System.IO.Compression.Crc32Helper.UpdateCrc32()
        /// which is undocumented and used in Gzip </summary>
        public static uint UpdateCrc32(uint p_crc32, byte[] p_bytes, int p_idx, int p_count)
        {
            if (g_crcTable == null)
            {
                uint[] crcTable = new uint[256];
                for (int i = crcTable.Length; 0 <= --i; )
                {
                    uint j = (uint)i; for (int k = 8; 0 <= --k; ) j = (j & 1) != 0 ? 3988292384u ^ (j >> 1) : (j >> 1);
                    crcTable[i] = j;
                }
                System.Threading.Volatile.Write(ref g_crcTable, crcTable);
            }
            for (p_crc32 ^= ~0u; 0 <= --p_count; )
                p_crc32 = g_crcTable[(p_crc32 ^ p_bytes[p_idx++]) & 255] ^ (p_crc32 >> 8);
            return ~p_crc32;
        }
        static uint[] g_crcTable;

        public static uint GetCrc32(byte[] p_bytes)
        {
            return (p_bytes == null) ? 0 : UpdateCrc32(0, p_bytes, 0, p_bytes.Length);
        }
        public static uint GetCrc32(string p_string)
        {
            return (p_string == null) ? 0 : GetCrc32(Encoding.UTF8.GetBytes(p_string));
        }

        /// <summary> Returns p_array[]; or a copy of p_array[0..p_length-1]
        /// when any of the followings are true: <para>
        /// - p_limit is positive: p_limit ≤ p_array.Length - p_length</para><para>
        /// - p_limit is negative: Max(-p_limit, ((long)p_limit-p_limit)*p_array.Length) ≺ p_array.Length - p_length </para>
        /// For example, p_limit==-1024.25 means that Max(1024.25, 0.25*p_array.Length) ≺ p_array.Length - p_length </summary>
        public static T[] TrimExcess<T>(T[] p_array, int p_length, double p_limit)
        {
            if (p_length < 0)
                throw new ArgumentOutOfRangeException("p_length");
            if (p_array == null)
                return null;
            long u = p_array.Length - p_length, limit = (long)p_limit;
            if (0 < u && (0 < limit ? limit <= u
                                    : (-u < limit || (limit - p_limit) * p_array.Length < u)))
            {
                T[] copy = new T[p_length];
                Array.Copy(p_array, copy, p_length);
                return copy;
            }
            return p_array;
        }
        /// <summary> Returns true if p_array[] has been replaced </summary>
        public static bool TrimExcess<T>(ref T[] p_array, int p_length, double p_limit)
        {
            T[] trimmed = TrimExcess<T>(p_array, p_length, p_limit);
            return (trimmed != p_array) && ((p_array = trimmed) == trimmed);
        }

        public static KeyValuePair<byte[], int> Compress(Stream p_input, Compression p_flags)
        {
            if (p_input == null)
                return default(KeyValuePair<byte[], int>);
            var mem = new MemoryStreamEx();
            Stream tmp;
            const System.IO.Compression.CompressionLevel L = System.IO.Compression.CompressionLevel.Optimal;
            switch (p_flags & Compression._Method)
            {
                case Compression.Store:   tmp = mem = (p_input as MemoryStreamEx) ?? mem; break;
                case Compression.Deflate: tmp = new System.IO.Compression.DeflateStream(mem, L); break;
                case Compression.Gzip:    tmp = new System.IO.Compression.GZipStream(mem, L); break;
                default : throw new ArgumentException(p_flags.ToString());
            }
            if (tmp != p_input)
                using (tmp)
                    p_input.CopyTo(tmp);
            if ((p_flags & Compression._Closing) == Compression.CloseSource)
                p_input.Close();
            return new KeyValuePair<byte[], int>(mem.GetBuffer(), (int)mem.LastLength);
        }
        [Flags]
        public enum Compression
        {
            Store = 0,
            Gzip = 1,
            Deflate = 2,
            _Method = 3,

            CloseSource = 4,
            LeaveOpen = 0,
            _Closing = 4,
        }
    }

    /// <summary> Note: endian-sensitive! </summary>
    [StructLayout(LayoutKind.Explicit)]
    public struct FloatAndUIntUnion
    {
        [FieldOffset(0)]
        public uint  UInt32Bits;
        [FieldOffset(0)]
        public float FloatValue;
    }

    #region Unrar support (read file with callback)
    /// <summary> Signature of an enumerator function that is used as callback
    /// during forward-only binary reading of a file, to process the contents
    /// of the file. The contents of the file is presented in the following
    /// way: some bytes are read into p_buffer[0..p_buffer.Count), then the
    /// MoveNext() method of the enumeration returned by this callback is
    /// called. The next item of the enumeration must be the number of bytes
    /// consumed by MoveNext() from p_buffer. (The enumeration must not modify
    /// any public fields of p_buffer!)
    /// Consuming 0 byte is interpreted as a request for more data (which will
    /// be appended to the unconsumed data). If the file has ended and zero
    /// byte is consumed twice, IOException is raised (premature end of file).
    /// The enumerator is advanced until it completes, even if end of file is
    /// reached and the buffer became empty.
    /// </summary>
    public delegate IEnumerable<int> ReadBinaryCallback<T>(ByteBuffer p_buffer,
                                            long p_fileSize, T p_arg);

    public static partial class Utils
    {
        public const char PathInsideSeparator = '!';

        /// <summary> If p_filename is in "path_to_archive!path_inside" format,
        /// detects the type of the "path_to_archive" part.
        /// Returns one of the following values:
        /// - ".rar", ".7z" : rar/7-Zip compressed file
        /// - ".csv.gz"     : GZIP-compressed .csv file (starts with "# ")
        /// - ".gz"         : GZIP-compressed file (does not start with "# ")
        /// - ".csv"        : .csv file (starts with "# ")
        /// - ""            : unknown type (default)
        /// </summary>
        public static string DetectFileType(string p_filename)
        {
            string inside;
            SplitPathInsideArchive(ref p_filename, out inside);
            string result = Path.GetExtension(p_filename).ToLower();
            if (!String.IsNullOrEmpty(result))
                return result;
            // Gets here only if p_filename has no extension
            using (FileStream fstream = File.OpenRead(p_filename))
                switch (new BinaryReader(fstream).ReadUInt16())
                {
                    case 0x7A37: result = ".7z";  break;
                    case 0x6152: result = ".rar"; break;
                    case 0x8B1F: result = ".gz";
                        fstream.Position = 0;
                        try
                        {
                            var s = new StreamReader(new System.IO.Compression.GZipStream(
                                fstream, System.IO.Compression.CompressionMode.Decompress));
                            if (s.Read() == '#' && s.Read() == ' ')
                                result = ".csv.gz";
                        } catch {
                            goto default;
                        }
                        break;
                    default :
                        result = String.Empty;
                        fstream.Position = 0;
                        try
                        {
                            var s = new StreamReader(fstream);
                            if (s.Read() == '#' && s.Read() == ' ')
                                result = ".csv";
                        } catch {}
                        break;
                }
            return result;
        }

        /// <summary> Returns true and modifies 'p_path' if it does not exists but 
        /// it is in "path_to_archive!path_inside" format and "path_to_archive" exists.
        /// In this case sets 'p_inside' to "path_inside". </summary>
        public static bool SplitPathInsideArchive(ref string p_path, out string p_inside)
        {
            p_inside = null;
            int i;
            if (String.IsNullOrEmpty(p_path) || File.Exists(p_path)
                || 0 > (i = p_path.LastIndexOf(PathInsideSeparator)))
                return false;
            string fn = p_path.Substring(0, i);
            if (!File.Exists(fn))
                return false;
            p_inside = p_path.Substring(i + 1);
            p_path = fn;
            return true;
        }

        /// <summary> Allows loading a file using a callback function
        /// (necessary for unrar, for example). Returns a non-null enumerable
        /// function if such implementation is available for the actual file
        /// format (in this case actual reading won't start until the caller
        /// enumerates the sequence). The enumerable function yields a dummy
        /// int value after every step of p_callback(), including the final one.
        /// Supported file formats: .dat,.csv (plain file); .gz,.csv.gz (note:
        /// fileSize=compressed size!);  .rar (first file only).
        /// Enumeration is not available for .rar. </summary>
        public static IEnumerable<int> ReadFileUsingCallback<T>(string p_filename, 
            ReadBinaryCallback<T> p_callback, T p_arg)
        {
            string fileType = DetectFileType(p_filename);
            switch (fileType)
            {
                case ".rar" :
                    RarReader.ReadFile(p_filename, p_callback, p_arg);
                    return null;
                case ".7z" :
                    // TODO: support for .7z
                    throw new NotImplementedException();
                default :
                    // Uncompressed file or GZip compression
                    return ReadGzOrDat(p_filename, fileType.Contains(".gz"), p_callback, p_arg);
            }
        }

        static IEnumerable<int> ReadGzOrDat<T>(string p_filename, bool p_isGzip,
            ReadBinaryCallback<T> p_callback, T p_arg)
        {
            const int DefaultBufferSize = 64 * 1024;    // must be power of 2
            using (FileStream fstream = new FileStream(p_filename, FileMode.Open,
                FileAccess.Read, FileShare.Read, DefaultBufferSize / 2, FileOptions.SequentialScan))
            {
                Stream stream = fstream;
                long fileSize = fstream.Length;
                if (p_isGzip)           // GZIP compression
                    stream = new System.IO.Compression.GZipStream(fstream,
                                System.IO.Compression.CompressionMode.Decompress);
                using (var buffer = new ByteBufferForReader(DefaultBufferSize))
                {
                    buffer.m_reader = p_callback(buffer, fileSize, p_arg).GetEnumerator();
                    buffer.m_filename = fstream.Name;
                    for (int end = 0, consumed = 0, nRead = 0; true; )
                    {
                        nRead = Math.Min(buffer.Array.Length - end, DefaultBufferSize / 2);
                        Utils.DebugAssert(nRead > 0 && end >= consumed);
                        nRead = stream.Read(buffer.Array, end, nRead);

                        end += nRead;
                        buffer.Count = end;
                        buffer.Start = 0;

                        for (int stage = 0; stage >= 0; )
                        {
                            stage = buffer.StepReader(ref consumed, nRead == 0, stage);
                            yield return consumed;
                        }
                        if (consumed < 0)
                            break;      // reader completed
                        // Now buffer.Length == notUsedCount, buffer.Ptr == buffer.Start + usedCount
                        consumed = 0;
                        if (buffer.Count <= 0)   // the reader consumed all
                            end = 0;
                        else if (buffer.Array.Length - end >= DefaultBufferSize / 2)
                            consumed = end - buffer.Count;
                        else    // Move remainder to beginning of buffer, grow if necessary
                            buffer.EnsureCapacity((end = buffer.Count) + DefaultBufferSize / 2,
                                DefaultBufferSize / 2);
                    }
                }
            }
        }

        private sealed class ByteBufferForReader : ByteBuffer, IDisposable
        {
            internal ByteBufferForReader()               : base() { }
            internal ByteBufferForReader(int p_capacity) : base(p_capacity) { }

            internal string m_filename;
            internal IEnumerator<int> m_reader;
            public void Dispose() { Utils.DisposeAndNull(ref m_reader); }

            /// <summary> After Start+=p_offset, Count-=p_offset, repeats {
            ///   m_reader.MoveNext(); Start+=nConsumed; Count-=nConsumed; }
            /// until the reader stops consuming data (asking for more).
            /// Returns usedCount := p_offset + number of bytes consumed by m_reader
            /// (or its bitwise complement if m_reader completed: !m_reader.MoveNext()).
            /// Precondition: p_offset is less or equal to this.Count.
            /// Postcondition: this.Start += usedCount, this.Count -= usedCount
            /// (i.e. this.Count == notUsedCount) </summary>
            internal int FeedReader(bool p_isEof, int p_offset)
            {
                for (int stage = 0; stage >= 0; )
                    stage = StepReader(ref p_offset, p_isEof, stage);
                return p_offset;
            }

            /// <summary> Calls m_reader.MoveNext() exactly once. Returns
            /// new value for p_stage (>= 0 if there are some data left in
            /// the buffer that the reader may consume, see below in Remarks).
            /// Negative return value with nonnegative p_offset indicates that
            /// the buffer is exhausted (reader wants more data).
            /// Negative return value with negative p_offset indicates that
            /// the reader has finished. </summary><remarks>
            /// p_stage contains information about the previous step of the reader:
            /// 0: since the previous step some data was added to the buffer
            /// 1: in the previous step the reader consumed nonzero but not all of the buffer
            /// 2: the reader consumed 0 bytes (requested more data). If it occurs
            ///    again, and end-of-file is reached, the method will raise IOException
            /// </remarks>
            internal int StepReader(ref int p_offset, bool p_isEof, int p_stage)
            {
                if (p_stage == 0)
                {
                    Start += p_offset;
                    Count -= p_offset;
                    p_stage = 1;
                }
                else if (p_stage < 0)
                    return -1;
                bool was0 = (p_stage == 2);
                if (!p_isEof && (was0 || Count <= 0))
                    return p_stage;
                if (!m_reader.MoveNext())   // process contents of the buffer
                {
                    p_offset ^= -1;         // p_offset<0: processing completed
                    Dispose();
                    return -1;
                }
                int consumed = Math.Max(0, Math.Min(m_reader.Current, Count));
                if (consumed == 0 && p_isEof && was0)
                    // the reader needs more data (consumed nothing 2x), but it's already EOF
                    throw new IOException("Premature end of file: " + m_filename);

                p_stage = (consumed == 0) ? 2 : 1;
                Start += consumed;
                Count -= consumed;
                p_offset += consumed;
                return (p_isEof || (0 < consumed && 0 < Count)) ? p_stage : -1;
            }
        }

        /// <summary> Associate class to be used via Utils.ReadBinaryFile() </summary>
        internal class RarReader : Schematrix.Unrar
        {
            const int BufferSizeIncrement = 128 * 1024;    // must be power of 2
            readonly ByteBufferForReader m_buffer = new ByteBufferForReader(BufferSizeIncrement);
            bool m_isReaderCompleted;

            private RarReader() { }
            protected override void Dispose(bool p_notFromDtor)
            {
                m_buffer.Dispose();
                base.Dispose(p_notFromDtor);
            }

            /// <summary> Reads the first file found in p_filename .rar archive,
            /// except if p_filename is in "path_to_archive!path_inside" format </summary>
            public static void ReadFile<T>(string p_filename,
                ReadBinaryCallback<T> p_callback, T p_arg)
            {
                RarReader _this = new RarReader();
                string fn = p_filename, inside;
                SplitPathInsideArchive(ref p_filename, out inside);
                try
                {
                    _this.Open(p_filename, OpenMode.Extract);
                    while (_this.ReadHeader() && inside != null && (_this.CurrentFile.IsDirectory
                            || !Utils.PathEquals(_this.CurrentFile.FileName, inside)))
                        _this.Skip();
                    if (_this.CurrentFile == null)
                        throw new IOException("Cannot find file inside archive");
                    _this.m_buffer.m_filename = fn = p_filename + '!' + _this.CurrentFile.FileName;
                    _this.m_buffer.m_reader = p_callback(_this.m_buffer,
                        _this.CurrentFile.UnpackedSize, p_arg).GetEnumerator();
                    _this.m_isReaderCompleted = (_this.m_buffer.m_reader == null);
                    if (!_this.m_isReaderCompleted)
                        _this.Test();
                    _this.Close();
                    ///Process remainder of the buffer after EOF
                    if (!_this.m_isReaderCompleted)
                        _this.m_buffer.FeedReader(true, 0);
                }
                catch (OperationCanceledException) { throw; }   // preserve stack trace
                catch (Exception e)
                {
                    throw new IOException("Error while reading \"" + fn
                        + "\": " + e.Message, e);
                }
                finally
                {
                    _this.Dispose();
                }
            }

            /// <summary> This method is called by unrar32/64.dll, during decompressing.
            /// p_unrarLen varies between 256 bytes and 4M. </summary>
            protected override int OnDataAvailable(IntPtr p_unrarPtr, int p_unrarLen)
            {
                for (int nCopied = 0; !m_isReaderCompleted && nCopied < p_unrarLen; )
                {
                    int room = m_buffer.Array.Length - m_buffer.Count;
                    if (room <= 0)
                        room = BufferSizeIncrement;
                    int nCopy = Math.Min(p_unrarLen - nCopied, room);
                    m_buffer.EnsureCapacity(m_buffer.Count + nCopy, BufferSizeIncrement);
                    Marshal.Copy(new IntPtr(p_unrarPtr.ToInt64() + nCopied), m_buffer.Array, 
                        m_buffer.Count, nCopy);
                    m_buffer.Count += nCopy;
                    nCopied += nCopy;
                    m_isReaderCompleted = (m_buffer.FeedReader(false, 0) < 0);
                }
                return m_isReaderCompleted ? -1 : base.OnDataAvailable(p_unrarPtr, p_unrarLen);
            }
        }

        public static IEnumerable<Schematrix.RARFileInfo> ListFilesInRar(string p_archiveFullPath)
        {
            using (var unrar = new Schematrix.Unrar(p_archiveFullPath))
            {
                unrar.Open();
                while (unrar.ReadHeader())
                {
                    yield return unrar.CurrentFile;
                    unrar.Skip();
                }
            }
        }

        /// <summary> Converts p_callback() to a ReadBinaryCallback&lt;T&gt; delegate
        /// (T = Func&lt;ByteBuffer, IEnumerable&lt;int&gt;&gt;).
        /// The integers generated by p_callback() specify the number of bytes required
        /// for the next step of its iteration (negated value indicates that EOF is
        /// allowed at that point). This method translates this sequence to number of
        /// bytes consumed from the input, consuming 0 when p_buffer.Count is less than
        /// requested. 
        /// See also documentation of ReadBinaryCallback&lt;T&gt;. Important: p_callback()
        /// enumeration must not modify any public fields of p_buffer!
        /// </summary>
        public static IEnumerable<int> ReadBinaryHelper(ByteBuffer p_buffer, long p_fileSize, 
            Func<ByteBuffer, IEnumerable<int>> p_callback)
        {
            using (var req = p_callback(p_buffer).GetEnumerator())
            {
                int consumed = 0, savedStart = p_buffer.Start, savedCount = p_buffer.Count;
                for (bool stay = false; stay || req.MoveNext(); )
                {
                    // req.Current < 0 means that EOF is allowed at this point
                    int sizeRequest = Math.Abs(req.Current);
                    stay = (consumed + sizeRequest > savedCount);
                    if (stay)
                    {
                        p_buffer.Start = savedStart;
                        p_buffer.Count = savedCount;
                        yield return consumed;
                        if (consumed == 0 && p_buffer.Count == 0 && req.Current < 0)
                            break;      // OK: it's EOF, accepted here
                        consumed = 0;
                        // If EOF is not allowed, keep asking more (consuming 0).
                        // This will trigger IOException if EOF is reached.
                        savedStart = p_buffer.Start;
                        savedCount = p_buffer.Count;
                    }
                    else
                    {
                        p_buffer.Start = savedStart + consumed;
                        p_buffer.Count = sizeRequest;
                        consumed      += sizeRequest;
                    }
                }
            }
        }


        #region Read text file as IEnumerable<string>, supporting Unrar

        /// <summary> Works for all file types known by DetectFileType() (including .rar).
        /// p_fileType should be one of the strings that DetectFileType() may return, or
        /// null (autodetect). </summary>
        public static IEnumerable<string> ReadTextFile(string p_filename, string p_fileType,
            Encoding p_enc, int p_nBufferedLines)
        {
            if (p_fileType == null)
                p_fileType = DetectFileType(p_filename);
            else
                p_fileType = p_fileType.ToLower();
            p_enc = p_enc ?? Encoding.UTF8;
            bool isGzip = p_fileType.Contains(".gz");
            if (isGzip || p_fileType == ".csv" || p_fileType == "")
                using (FileStream fstream = new FileStream(p_filename, FileMode.Open,
                    FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan))
                {
                    Stream stream = !isGzip ? (Stream)fstream
                        : new System.IO.Compression.GZipStream(fstream,
                            System.IO.Compression.CompressionMode.Decompress);
                    TextReader reader = new StreamReader(stream, p_enc);
                    for (string line = reader.ReadLine(); line != null; line = reader.ReadLine())
                        yield return line;
                }
            else
            {
                // This producer will run in a ThreadPool thread
                var producer = new Action<Queue<KeyValuePair<string[], int>>, Func<bool,bool>>(
                    (p_queue, p_checkUserBreak) => ReadFileUsingCallback(p_filename, ReadTextHelper.LineReader,
                            new ReadTextHelper {
                                m_queue          = p_queue,
                                m_encoding       = p_enc,
                                m_checkUserBreak = p_checkUserBreak,
                                m_nBufferedLines = (p_nBufferedLines <= 0) ? 1000 : p_nBufferedLines
                            }).ForEach()
                );
                foreach (var kv in Utils.ProducerConsumer(producer, p_checkUserBreak: null))
                {
                    for (int i = 0; i < kv.Value; ++i)
                        yield return kv.Key[i];
                }
            }
        }

        private struct ReadTextHelper
        {
            internal Queue<KeyValuePair<string[],int>> m_queue;
            internal Func<bool,bool> m_checkUserBreak;
            internal int m_nBufferedLines;
            internal Encoding m_encoding;

            // "Implements" the ReadBinaryCallback<ReadTextHelper> delegate
            internal static IEnumerable<int> LineReader(ByteBuffer p_buffer,
                long p_fileSize, ReadTextHelper p_this)
            {
                int poolIdx = 0, count = 0;
                string[][] pool = new string[4][];
                string[] current = pool[poolIdx] = new string[Math.Max(1, 
                    p_this.m_nBufferedLines / 4)];
                for (int carryForward = 0; carryForward < p_buffer.Count; )
                {
                    byte[] array = p_buffer.Array;
                    int lineStart = p_buffer.Start, end = lineStart + p_buffer.Count;
                    // invariant: p_buffer[0..carryForward-1] != '\n'
                    for (int i = lineStart + carryForward; i < end; ++i)
                        if (array[i] == '\n')
                        {
                            int len = i - lineStart;
                            if (len > 0 && array[i - 1] == '\r')
                                --len;
                            string line = p_this.m_encoding.GetString(array, lineStart, len);
                            lineStart = i + 1;
                            //if (!p_lineReader(line))
                            //    yield break;
                            current[count++] = line;
                            if (current.Length <= count)
                            {
                                if (!p_this.Send(current, count, pool.Length - 2))
                                    yield break;
                                poolIdx = ++poolIdx % pool.Length;
                                if (pool[poolIdx] == null)
                                    pool[poolIdx] = new string[current.Length];
                                current = pool[poolIdx];
                                count = 0;
                            }
                        }
                    carryForward = end - lineStart;
                    int consumed = lineStart - p_buffer.Start;
                    yield return consumed;      // == nr.of bytes consumed
                    if (consumed > 0 && carryForward >= p_buffer.Count)
                        yield return 0;
                }
                if (p_buffer.Count > 0)
                    //p_lineReader(lastLine);
                    current[count++] = p_this.m_encoding.GetString(p_buffer.Array, 
                        p_buffer.Start, p_buffer.Count);
                if (0 < count)
                    p_this.Send(current, count, int.MaxValue);
            }

            internal bool Send(string[] p_result, int p_resultLen, int p_maxQueueLength)
            {
                lock (m_queue)
                {
                    while (p_maxQueueLength <= m_queue.Count)
                    {
                        if (m_checkUserBreak(false))
                            return false;
                        System.Threading.Monitor.Wait(m_queue);
                        if (m_checkUserBreak(false))
                            return false;
                    }
                    m_queue.Enqueue(new KeyValuePair<string[], int>(p_result, p_resultLen));
                    System.Threading.Monitor.PulseAll(m_queue);
                    return true;
                }
            }
        }

        #endregion

        /// <summary> Reads until the first p_stopByte byte in the specified file.
        /// Supports .rar &amp; .gz files, too. </summary>
        public static string ReadUTF8String(string p_filename, byte p_stopByte, out int p_nBytes)
        {
            var utf8Bytes = new ByteBuffer(128);
            ReadFileUsingCallback(p_filename, ReadUTF8StringHelper,
                new KeyValuePair<ByteBuffer, byte>(utf8Bytes, p_stopByte)).ForEach();
            p_nBytes = utf8Bytes.Count;
            return Encoding.UTF8.GetString(utf8Bytes.Array, 0, utf8Bytes.Count);
        }
        private static IEnumerable<int> ReadUTF8StringHelper(ByteBuffer p_srcBuffer,
            long p_fileSize, KeyValuePair<ByteBuffer, byte> p_arg)
        {
            while (p_srcBuffer.Count > 0)
            {
                int i = p_srcBuffer.Start, end = i + p_srcBuffer.Count;
                for (byte[] array = p_srcBuffer.Array; i < end; ++i)
                    if (array[i] == p_arg.Value)
                        break;
                int nCopy = i - p_srcBuffer.Start;
                ByteBuffer dst = p_arg.Key;
                dst.EnsureCapacity(dst.Count + nCopy, 4096);
                Buffer.BlockCopy(p_srcBuffer.Array, p_srcBuffer.Start, dst.Array, dst.Count, nCopy);
                dst.Count += nCopy;
                if (i < p_srcBuffer.Count)
                    break;
                yield return p_srcBuffer.Count;
            }
        }

        /// <summary> Reads at most p_nBytes bytes starting at p_bytes[p_offset] into an ulong
        /// (little endian). Precondition: p_nBytes &gt; 0.
        /// Note: this method is always little endian, as opposed to BitConverter.ToIntXX()
        /// </summary>
        public static ulong ReadUlong(this byte[] p_bytes, int p_offset, int p_nBytes)
        {
            unchecked
            {
                //int n = Math.Min(p_nBytes, p_bytes.Length - p_offset); // without call&branch:
                int n = (p_bytes.Length - p_offset) - p_nBytes;          // (10% speed-up in release)
                n = p_nBytes + ((n >> 31) & n);
                if (BitConverter.IsLittleEndian && n > 7)                // 60% speed-up in debug, 10% in release
                    return (ulong)BitConverter.ToInt64(p_bytes, p_offset);
                ulong result = p_bytes[p_offset];
                switch (n)
                {
                    default:result |= (ulong)p_bytes[p_offset + 7] << 56; goto case 7;
                    case 7: result |= (ulong)p_bytes[p_offset + 6] << 48; goto case 6;
                    case 6: result |= (ulong)p_bytes[p_offset + 5] << 40; goto case 5;
                    case 5: result |= (ulong)p_bytes[p_offset + 4] << 32; goto case 4;
                    case 4: result |= (ulong)p_bytes[p_offset + 3] << 24; goto case 3;
                    case 3: result |= (ulong)p_bytes[p_offset + 2] << 16; goto case 2;
                    case 2: return result | ((ulong)p_bytes[p_offset + 1] << 8);
                    case 1: return result;
                }
            }
        }
        /// <summary> Writes the p_nBytes least significant bytes of p_value to
        /// p_bytes[p_offset] (in little-endian order). Precondition: p_nBytes &gt; 0 </summary>
        public static void WriteUlong(this byte[] p_bytes, int p_offset, int p_nBytes,
            ulong p_value)
        {
            unchecked
            {
                p_bytes[p_offset] = (byte)p_value;
                switch (Math.Min(p_nBytes, p_bytes.Length - p_offset))
                {
                    default:p_bytes[++p_offset] = (byte)(p_value >>= 8); goto case 7;
                    case 7: p_bytes[++p_offset] = (byte)(p_value >>= 8); goto case 6;
                    case 6: p_bytes[++p_offset] = (byte)(p_value >>= 8); goto case 5;
                    case 5: p_bytes[++p_offset] = (byte)(p_value >>= 8); goto case 4;
                    case 4: p_bytes[++p_offset] = (byte)(p_value >>= 8); goto case 3;
                    case 3: p_bytes[++p_offset] = (byte)(p_value >>= 8); goto case 2;
                    case 2: p_bytes[++p_offset] = (byte)(p_value >>= 8); break;
                }
            }
        }
        public static ushort ReadUshort(this byte[] p_bytes, int p_offset)
        {
            return (ushort)(p_bytes[p_offset] + (p_bytes[p_offset + 1] << 8));
        }
        //public static void WriteUshort(this byte[] p_bytes, int p_offset, ushort p_value)
        //{
        //    p_bytes[p_offset]     = unchecked((byte)p_value);
        //    p_bytes[p_offset + 1] = unchecked((byte)(p_value >> 8));
        //}
        public static int GetCodedCountSize(int p_count)
        {
            if (p_count < 64)        return 1;
            if (p_count < (1 << 14)) return 2;
            if (p_count < (1 << 22)) return 3;
            if ((1 << 30) < p_count) throw new ArgumentOutOfRangeException();
            return 4;
        }
        public static int WriteCodedCount(ref QuicklyClearableList<ulong> p_dst, int p_bytePos, int p_count)
        {
            int s = GetCodedCountSize(p_count);
            return WriteBits(ref p_dst, checked((int)((long)p_bytePos << 3)), ((ulong)p_count << 2) + (ulong)(s - 1), s << 3) >> 3;
        }
        /// <summary> Returns number of bytes read (1..4), or its bitwise complement
        /// if data is missing (it's beyond the end of p_dst[]).
        /// Throws exception if p_bytePos is beyond the end of p_dst[]. </summary>
        public static int ReadCodedCount(ulong[] p_dst, int p_bytePos, out int p_count)
        {
            uint ulongIdx = (uint)(p_bytePos >> 3), s;
            if (5 <= (p_bytePos & 7) && (ulongIdx < p_dst.Length - 1))
                s = (uint)BitVector.ReadBits(p_dst, checked((int)((long)p_bytePos << 3)), 32);
            else
                s = (uint)(p_dst[ulongIdx] >> unchecked(p_bytePos << 3));
            ulongIdx = (s & 3) + 1;     // nRead (1,2,3,4)
            p_count = (int)((s & (~0u >> -((int)ulongIdx << 3))) >> 2);
            return (int)ulongIdx ^ (((p_dst.Length << 3) - p_bytePos - (int)ulongIdx) >> 31);  // (Length*8 <= p_bytePos+nRead-1) ? ~nRead : nRead
        }
    }

    public class ByteBuffer
    {
        /// <summary> Invariant: never null </summary>
        public byte[] Array;
        public int Start, Count;
        public int End                      { get { return Start + Count; } }
        public ByteBuffer()                 { Array = (byte[])Enumerable.Empty<byte>(); }
        public ByteBuffer(int p_capacity)   { Allocate(p_capacity); }
        public ByteBuffer Allocate(int p_capacity)
        {
            Array = new byte[Math.Max(0, p_capacity)];
            Count = 0;
            return this;
        }
        /// <summary> Grows Array[] if p_newCapacity &gt; Array.Length, and moves
        /// the [Start..Start+Count) logical contents to Array[0..Count). </summary>
        public void EnsureCapacity(int p_newCapacity, int p_adjustmentPowerOf2)
        {
            Utils.DebugAssert((p_adjustmentPowerOf2 & (p_adjustmentPowerOf2 - 1)) == 0);
            Utils.DebugAssert(Count <= Array.Length);
            if (p_newCapacity > Array.Length)
            {
                if (--p_adjustmentPowerOf2 >= 0)
                    p_newCapacity = (Math.Max(Array.Length << 1, p_newCapacity)
                                        + p_adjustmentPowerOf2) & ~p_adjustmentPowerOf2;
                byte[] tmp = new byte[p_newCapacity];
                Buffer.BlockCopy(Array, Start, tmp, 0, Count);
                Array = tmp;
                Start = 0;
            }
            else if (Start != 0)
            {
                Buffer.BlockCopy(Array, Start, Array, 0, Count);
                Start = 0;
            }
        }
        ///// <summary> Sets Start:=0 and Count:=p_count, ensures that Array.Length
        ///// is at least max(p_count, p_newCapacity), and copies p_src[p_srcIndex,
        ///// p_srcIndex+p_count) to Array[Start..Start+Count).
        ///// p_adjustmentPowerOf2 is only used if Array[] needs to grow. </summary>
        //public void Assign(byte[] p_src, int p_srcIndex, int p_count, int p_newCapacity, 
        //    int p_adjustmentPowerOf2)
        //{
        //    Utils.DebugAssert((p_adjustmentPowerOf2 & (p_adjustmentPowerOf2 - 1)) == 0);
        //    if (p_count > p_newCapacity)
        //        p_newCapacity = p_count;
        //    if (p_newCapacity > Array.Length)
        //    {
        //        if (p_adjustmentPowerOf2-- > 0)
        //            p_newCapacity = (Math.Max(Array.Length << 1, p_newCapacity)
        //                                + p_adjustmentPowerOf2) & ~p_adjustmentPowerOf2;
        //        byte[] tmp = new byte[p_newCapacity];
        //        Buffer.BlockCopy(p_src, p_srcIndex, tmp, 0, p_count);
        //        Array = tmp;
        //    }
        //    else if (p_srcIndex != 0 || p_src != Array)
        //        Buffer.BlockCopy(p_src, p_srcIndex, Array, 0, p_count);
        //    Start = 0;
        //    Count = p_count;
        //}
        public byte this[int i]
        {
            get { return Array[Start + i]; }
            set { Array[Start + i] = value; }
        }
    }

    #endregion

    /// <summary> T must be a blittable type (or array of blittable items) 
    /// Blittable types: primitive value types except TimeSpan,DateTime,String </summary>
    public struct Pinned<T> : IDisposable where T : class
    {
        GCHandle m_handle;
        public T      Obj { get; private set; }
        public IntPtr Ptr { get; private set; }

        public Pinned(T p_object) : this()
        {
            Obj = p_object;
            m_handle = GCHandle.Alloc(Obj, GCHandleType.Pinned);
            Ptr = m_handle.AddrOfPinnedObject();
        }
        public void Dispose()
        {
            if (m_handle.IsAllocated)
            {
                m_handle.Free();
                m_handle = default(GCHandle);   // ensure that IsAllocated==false
                Ptr = IntPtr.Zero;
            }
        }
    }

    /// <summary> Adds a LastLength property, to allow querying the .Length after Dispose()
    /// (.Length throws ObjectDisposedException after Dispose()).
    /// This allows avoiding ToArray(), i.e. copying the GetBuffer() internal array after Dispose(). </summary>
    public class MemoryStreamEx : MemoryStream
    {
        long m_lengthAtDispose = -1;
        public long LastLength
        {
            get { return CanRead ? Length : Math.Max(0, m_lengthAtDispose); }
        }
        protected override void Dispose(bool disposing)
        {
            if (CanRead)
                m_lengthAtDispose = Length;
            base.Dispose(disposing);
        }
        /// <summary> Returns 'this' or a copy of 'this'. A copy is created when
        /// any of the followings are true about U := the number of unused bytes<para>
        /// -  0 ≺ p_limit ≤ U  </para><para>
        /// -  0 ≺ -p_limit ≺ U  </para><para>
        /// - p_limit ≺ 0 and ((long)p_limit-p_limit)*this.GetBuffer().Length ≺ U. </para>
        /// For example, p_limit==-1024.25 means that 1024.25 ≺ U or 0.25*this.GetBuffer().Length ≺ U.
        /// </summary>
        public MemoryStreamEx TrimExcess(double p_limit)
        {
            byte[] a = GetBuffer();
            if (!Utils.TrimExcess(ref a, (int)LastLength, p_limit))
                return this;
            byte[] copy = ToArray();
            return new MemoryStreamEx(copy, 0, copy.Length, true, true);
        }

        public MemoryStreamEx() : base() { }
        public MemoryStreamEx(byte[] buffer) : this(buffer == null ? new byte[0] : buffer, 0, buffer == null ? 0 : buffer.Length, true, true) { }
        public MemoryStreamEx(int capacity)  : base(capacity) { }
        public MemoryStreamEx(byte[] buffer, bool writable) : base(buffer, writable) { }     // IMPORTANT! buffer[] will be non-visible: GetBuffer() throws Exception! Use publiclyVisible=true to avoid
        public MemoryStreamEx(byte[] buffer, int index, int count) : base(buffer, index, count) { }                             // -||-
        public MemoryStreamEx(byte[] buffer, int index, int count, bool writable) : base(buffer, index, count, writable) { }    // -||-
        public MemoryStreamEx(byte[] buffer, int index, int count, bool writable, bool publiclyVisible) : base(buffer, index, count, writable, publiclyVisible) { }
    }

    /// <summary> A read-only stream that allows consuming bytes yielded
    /// by an IEnumerator&lt;byte&gt; as a stream. </summary>
    public class EnumeratorStream : Stream
    {
        long m_position = -1;
        IEnumerator<byte> m_it;
        /// <summary> Takes the responsibility of calling p_bytes.Dispose() </summary>
        public EnumeratorStream(IEnumerator<byte> p_bytes)
        {
            m_it = p_bytes;
        }
        protected override void Dispose(bool disposing)
        {
            Utils.DisposeAndNull(ref m_it);
            base.Dispose(disposing);
        }
        public override bool CanRead    { get { return true; } }
        public override bool CanSeek    { get { return false; } }
        public override bool CanWrite   { get { return false; } }
        public override long Length     { get { throw new NotSupportedException(); } }
        public override void Flush()    { }
        public override long Position
        {
            get { return m_position; }
            set { throw new NotSupportedException(); }
        }
        public override int Read(byte[] p_buffer, int p_offset, int p_count)
        {
            if (m_it == null)
                return 0;
            int i = p_offset;
            for (; --p_count >= 0; ++i, ++m_position)
            {
                if (!m_it.MoveNext())
                {
                    Utils.DisposeAndNull(ref m_it);
                    break;
                }
                p_buffer[i] = m_it.Current;
            }
            return i - p_offset;
        }
        public override long Seek(long offset, SeekOrigin origin)           { return Position; }
        public override void SetLength(long value)                          { throw new NotSupportedException(); }
        public override void Write(byte[] buffer, int offset, int count)    { throw new NotSupportedException(); }
    }

    /// <summary> A read-only stream that produces the UTF8 byte sequence
    /// made up by joining strings with a delimiter. If the stream is not
    /// empty, it begins with UTF8 identifier. </summary>
    public class Utf8StringReaderStream : EnumeratorStream
    {
        public Utf8StringReaderStream(string p_string)
            : this(new[] { p_string }, null) { }
        public Utf8StringReaderStream(IEnumerable<string> p_lines)
            : this(p_lines, Environment.NewLine) { }
        public Utf8StringReaderStream(IEnumerable<string> p_words, string p_delimiter)
            : base(GetUtf8Bytes(p_words, p_delimiter).GetEnumerator()) { }

        static IEnumerable<byte> GetUtf8Bytes(IEnumerable<string> p_words, string p_delimiter)
        {
            using (var it = p_words.EmptyIfNull().GetEnumerator())
            {
                var utf8 = new UTF8Encoding();
                char[] ch = new char[1];
                byte[] bytes = new byte[utf8.GetMaxByteCount(1)];
                for (string d = "\ufeff"; it.MoveNext(); d = p_delimiter)
                {
                    string s = d;
                    for (int a = 0; a < 2; ++a, s = it.Current)
                    {
                        if (String.IsNullOrEmpty(s))
                            continue;
                        for (int j = 0; j < s.Length; ++j)
                        {
                            ch[0] = s[j];
                            int n = utf8.GetBytes(ch, 0, 1, bytes, 0);
                            for (int i = 0; i < n; ++i)
                                yield return bytes[i];
                        }
                    }
                }
            }
        }
    }
}
